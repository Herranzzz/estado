import os
import requests
import typing as t
import datetime as dt
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

# Zona horaria para la comparación de "hoy"
# Si prefieres MX: export LOCAL_TZ=America/Mexico_City en el job de GitHub Actions
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/Madrid")

# Archivo de log
LOG_FILE = "logs_actualizacion_envios.txt"


# =========================
# UTILIDAD
# =========================
def log(message: str):
    tz = ZoneInfo(LOCAL_TZ)
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


def to_local_date(date_str: str, tz_name: str) -> t.Optional[dt.date]:
    """
    Convierte una fecha/hora ISO de CTT a fecha (YYYY-MM-DD) en la zona horaria indicada.
    Devuelve None si no se puede parsear.
    """
    if not date_str:
        return None
    try:
        parsed = parse(date_str)
        # Si no trae tz, asumimos UTC (ajusta si conoces la tz exacta de CTT)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
        local_dt = parsed.astimezone(ZoneInfo(tz_name))
        return local_dt.date()
    except Exception as e:
        log(f"⚠️ No se pudo parsear fecha CTT '{date_str}': {e}")
        return None


def today_local(tz_name: str) -> dt.date:
    return datetime.now(ZoneInfo(tz_name)).date()


# =========================
# SHOPIFY
# =========================
def get_fulfilled_orders(limit=300):
    """Obtiene hasta 'limit' pedidos con fulfillment completado."""
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
    }

    all_orders = []
    url = f"{SHOP_URL}/admin/api/2023-10/orders.json"
    params = {
        "fulfillment_status": "fulfilled",
        "status": "any",
        "limit": 50,
        "order": "created_at desc",
    }

    while len(all_orders) < limit:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        orders = data.get("orders", [])
        if not orders:
            break
        all_orders.extend(orders)

        # Avanza a la siguiente página si existe
        if "Link" in r.headers and 'rel="next"' in r.headers["Link"]:
            url = r.links["next"]["url"]
            params = None
        else:
            break

    return all_orders[:limit]


def get_last_fulfillment_event(order_id, fulfillment_id):
    """Obtiene el último evento registrado en Shopify con fecha y estado."""
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        log(f"❌ No se pudo obtener eventos para {order_id}: {r.status_code}")
        return None, None

    events = r.json().get("events", [])
    if not events:
        return None, None

    last_event = events[-1]
    last_status = last_event.get("status")
    last_date_raw = last_event.get("created_at")
    last_date = parse(last_date_raw) if last_date_raw else None
    return last_status, last_date


def create_fulfillment_event(order_id, fulfillment_id, status, event_date=None):
    """
    Crea un nuevo evento en Shopify SOLO si:
      - La fecha del último evento de CTT es HOY (en LOCAL_TZ).
      - Y no existe ya el mismo estado en Shopify con fecha >= a la de CTT.
    """
    event_status = map_ctt_to_shopify(status)

    # 1) Validar fecha de CTT = hoy
    ctt_event_date = to_local_date(event_date, LOCAL_TZ)
    if not ctt_event_date:
        log(f"⏭️ Pedido {order_id}: Evento '{status}' ignorado (sin fecha CTT)")
        return

    if ctt_event_date != today_local(LOCAL_TZ):
        log(f"⏭️ Pedido {order_id}: Evento '{status}' ignorado, fecha CTT {ctt_event_date} ≠ hoy {today_local(LOCAL_TZ)}")
        return

    # 2) Evitar duplicados o retrocesos
    last_status, last_date = get_last_fulfillment_event(order_id, fulfillment_id)
    if last_status == event_status:
        if last_date:
            last_local_date = to_local_date(last_date.isoformat(), LOCAL_TZ)
            if last_local_date and last_local_date >= ctt_event_date:
                log(f"🔒 Pedido {order_id}: Ya tiene estado '{event_status}' con fecha >= {ctt_event_date}, no se crea evento")
                return
        else:
            log(f"🔒 Pedido {order_id}: Ya tiene estado '{event_status}', no se crea evento")
            return

    # 3) Crear evento nuevo en Shopify
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {
        "event": {
            "status": event_status,
            "message": f"Estado CTT: {status}",
        }
    }
    if event_date:
        payload["event"]["created_at"] = parse(event_date).isoformat()

    r = requests.post(url, headers=headers, json=payload)
    if r.status_code == 201:
        log(f"✅ Evento '{event_status}' añadido a pedido {order_id} (CTT: {status} - {event_date})")
    else:
        log(f"❌ Error al añadir evento en pedido {order_id}: {r.status_code} - {r.text}")


# =========================
# CTT
# =========================
def get_ctt_status(tracking_number: str):
    """Consulta el estado actual desde la API de CTT y devuelve estado + fecha real."""
    try:
        r = requests.get(CTT_API_URL + tracking_number, timeout=25)
    except requests.RequestException as e:
        return {"status": f"CTT API error: {e}", "date": None}

    if r.status_code != 200:
        return {"status": f"CTT API error HTTP {r.status_code}", "date": None}

    try:
        data = r.json()
    except ValueError:
        return {"status": "Error parseando JSON CTT", "date": None}

    if data.get("error") is not None:
        return {"status": "Error en API CTT", "date": None}

    events = data.get("data", {}).get("shipping_history", {}).get("events", [])
    if not events:
        return {"status": "Sin eventos", "date": None}

    last_event = events[-1]
    return {
        "status": last_event.get("description", "Estado desconocido"),
        "date": last_event.get("event_date"),  # Fecha real del evento (ISO)
    }


def map_ctt_to_shopify(status: str) -> str:
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


# =========================
# MAIN
# =========================
def main():
    if not ACCESS_TOKEN:
        log("❌ Falta SHOPIFY_ACCESS_TOKEN en el entorno.")
        return

    orders = get_fulfilled_orders()
    log(f"🔄 Procesando {len(orders)} pedidos...")

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
        ctt_status = (ctt_result["status"] or "").strip()
        ctt_date = ctt_result["date"]

        if "error" in ctt_status.lower():
            log(f"⚠️ Error con CTT para {order_id}: {ctt_status}")
            continue

        if ctt_status in ("Sin eventos", "Estado desconocido"):
            log(f"ℹ️ Pedido {order_id}: {ctt_status}")
            continue

        # Crear/actualizar evento SOLO si la fecha CTT es hoy (control dentro de la función)
        create_fulfillment_event(order_id, fulfillment_id, ctt_status, event_date=ctt_date)


if __name__ == "__main__":
    main()
