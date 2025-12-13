import os
import requests
from datetime import datetime
from dateutil.parser import parse
from zoneinfo import ZoneInfo

# =========================
# CONFIG
# =========================
# Shopify API
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# CTT API
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

# Zona horaria para comparar "hoy"
TZ_NAME = os.getenv("TZ_NAME", "Europe/Madrid")

# Archivo de log
LOG_FILE = "logs_actualizacion_envios.txt"


def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


def shopify_headers():
    return {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
    }


def get_fulfilled_orders(limit=300):
    """Obtiene hasta 'limit' pedidos con fulfillment completado."""
    all_orders = []
    url = f"{SHOP_URL}/admin/api/2023-10/orders.json"
    params = {
        "fulfillment_status": "fulfilled",
        "status": "any",
        "limit": 50,
        "order": "created_at desc",
    }

    while len(all_orders) < limit:
        r = requests.get(url, headers=shopify_headers(), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        orders = data.get("orders", [])
        if not orders:
            break

        all_orders.extend(orders)

        # Siguiente página si existe
        if "Link" in r.headers and 'rel="next"' in r.headers["Link"]:
            url = r.links["next"]["url"]
            params = None
        else:
            break

    return all_orders[:limit]


def get_ctt_status(tracking_number: str):
    """Consulta el estado actual desde la API de CTT y devuelve estado + fecha real."""
    r = requests.get(CTT_API_URL + tracking_number, timeout=30)
    if r.status_code != 200:
        return {"status": "CTT API error", "date": None}

    data = r.json()
    if data.get("error") is not None:
        return {"status": "Error en API CTT", "date": None}

    events = data.get("data", {}).get("shipping_history", {}).get("events", [])
    if not events:
        return {"status": "Sin eventos", "date": None}

    last_event = events[-1]
    return {
        "status": last_event.get("description", "Estado desconocido"),
        "date": last_event.get("event_date"),  # Fecha real del evento (string)
    }


def map_ctt_to_shopify(status: str):
    """Mapea el estado devuelto por CTT al formato de Shopify."""
    status_map = {
        "En reparto": "out_for_delivery",
        "Entrega hoy": "out_for_delivery",
        "Entregado": "delivered",
        "En tránsito": "in_transit",
        "Recogido": "in_transit",
        "Pendiente de recepción en CTT Express": "confirmed",
        "Reparto fallido": "failure",
    }
    return status_map.get(status, "in_transit")


def parse_ctt_datetime(event_date_str: str | None, tz: ZoneInfo):
    """Parsea la fecha de CTT y la normaliza a tz. Si viene sin tz, asumimos tz."""
    if not event_date_str:
        return None
    dt = parse(event_date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    else:
        dt = dt.astimezone(tz)
    return dt


def get_fulfillment_events(order_id: int, fulfillment_id: int):
    """Devuelve lista de eventos del fulfillment (Shopify)."""
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    r = requests.get(url, headers=shopify_headers(), timeout=30)
    if r.status_code != 200:
        log(f"❌ No se pudo obtener eventos para {order_id}/{fulfillment_id}: {r.status_code} - {r.text}")
        return []
    return r.json().get("events", []) or []


def fulfillment_has_status(events: list, status: str) -> bool:
    """True si hay algún evento con ese status en Shopify."""
    for ev in events:
        if ev.get("status") == status:
            return True
    return False


def get_last_event_info(events: list):
    """Devuelve (last_status, last_created_at_dt_utc_or_naive) del último evento."""
    if not events:
        return None, None
    last_event = events[-1]
    last_status = last_event.get("status")
    created_at = last_event.get("created_at")
    return last_status, parse(created_at) if created_at else (last_status, None)


def create_fulfillment_event(order_id: int, fulfillment_id: int, ctt_status: str, ctt_event_date_str: str | None):
    """
    Crea un nuevo evento en Shopify con dos reglas:
    - Si Shopify ya tiene delivered -> NO hace nada.
    - Solo crea eventos si la fecha de CTT es "hoy" (TZ_NAME).
      Y especialmente: si CTT dice Entregado pero no es hoy -> no crea delivered.
    """
    tz = ZoneInfo(TZ_NAME)
    today_local = datetime.now(tz).date()

    event_status = map_ctt_to_shopify(ctt_status)
    ctt_dt = parse_ctt_datetime(ctt_event_date_str, tz)

    # Si no hay fecha, no actualizamos (para cumplir tu regla "solo si coincide con hoy")
    if ctt_dt is None:
        log(f"⏭️ SKIP {order_id}: CTT sin fecha de evento (no actualizo nada)")
        return

    # Regla: solo si CTT es de HOY
    if ctt_dt.date() != today_local:
        # Caso extra: si fuera delivered antiguo, lo bloqueamos sí o sí
        if event_status == "delivered":
            log(f"⏭️ SKIP {order_id}: CTT='Entregado' pero fecha {ctt_dt.date()} != hoy {today_local}")
        else:
            log(f"⏭️ SKIP {order_id}: CTT fecha {ctt_dt.date()} != hoy {today_local} (no actualizo)")
        return

    # Traemos eventos 1 vez y aplicamos candados
    events = get_fulfillment_events(order_id, fulfillment_id)

    # Candado fuerte: si ya hay delivered en Shopify, no vuelvas a tocar nada
    if fulfillment_has_status(events, "delivered"):
        log(f"⏭️ SKIP {order_id}: ya tiene 'delivered' en Shopify (idempotente)")
        return

    last_status, last_date = get_last_event_info(events)

    # Si el último estado es el mismo, evitamos duplicar (comparando fecha)
    if last_status == event_status:
        if last_date:
            # last_date puede venir con tz o no; lo llevamos a tz para comparar por día
            if last_date.tzinfo is None:
                last_local = last_date.replace(tzinfo=tz)
            else:
                last_local = last_date.astimezone(tz)

            if last_local.date() >= ctt_dt.date():
                log(f"🔒 Pedido {order_id} ya tiene '{event_status}' actualizado para esa fecha, no se crea evento")
                return

        log(f"ℹ️ Estado sin cambios para pedido {order_id} ({event_status})")
        return

    # Crear evento
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    payload = {
        "event": {
            "status": event_status,
            "message": f"Estado CTT: {ctt_status}",
            # Guardamos created_at igual al evento real (normalizado a tz)
            "created_at": ctt_dt.isoformat(),
        }
    }

    r = requests.post(url, headers=shopify_headers(), json=payload, timeout=30)
    if r.status_code == 201:
        log(f"✅ Evento '{event_status}' añadido a pedido {order_id} (CTT: {ctt_status}, fecha: {ctt_dt.date()})")
    else:
        log(f"❌ Error al añadir evento en pedido {order_id}: {r.status_code} - {r.text}")


def main():
    if not ACCESS_TOKEN:
        raise RuntimeError("Falta SHOPIFY_ACCESS_TOKEN en el entorno")

    orders = get_fulfilled_orders()
    log(f"🔄 Procesando {len(orders)} pedidos... (TZ={TZ_NAME})")

    for order in orders:
        fulfillments = order.get("fulfillments", [])
        if not fulfillments:
            continue

        fulfillment = fulfillments[0]
        order_id = order["id"]
        fulfillment_id = fulfillment["id"]

        tracking_number = fulfillment.get("tracking_number")
        if not tracking_number:
            log(f"⚠️ Pedido {order_id} sin número de seguimiento")
            continue

        # Estado actual y fecha en CTT
        ctt_result = get_ctt_status(tracking_number)
        ctt_status = ctt_result["status"]
        ctt_date = ctt_result["date"]

        if "error" in (ctt_status or "").lower():
            log(f"⚠️ Error con CTT para {order_id}: {ctt_status}")
            continue

        # Crear/actualizar evento con reglas anti-duplicados y "solo hoy"
        create_fulfillment_event(order_id, fulfillment_id, ctt_status, ctt_event_date_str=ctt_date)


if __name__ == "__main__":
    main()
