import os
import requests
from datetime import datetime
import time

# Shopify API
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# CTT API
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

# Archivo de log
LOG_FILE = "logs_actualizacion_envios.txt"

# Límite de peticiones por segundo para Shopify (seguro < 2 rps)
REQUEST_DELAY = 0.6


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


def shopify_headers():
    return {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }


def safe_get(url, params=None):
    """GET con manejo simple de 429."""
    while True:
        r = requests.get(url, headers=shopify_headers(), params=params)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1"))
            time.sleep(max(wait, 1))
            continue
        r.raise_for_status()
        time.sleep(REQUEST_DELAY)
        return r


def safe_post(url, json):
    """POST con manejo simple de 429."""
    while True:
        r = requests.post(url, headers=shopify_headers(), json=json)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1"))
            time.sleep(max(wait, 1))
            continue
        time.sleep(REQUEST_DELAY)
        return r


def get_fulfilled_orders(limit=300):
    """Obtiene hasta 'limit' pedidos con fulfillment completado."""
    all_orders = []
    url = f"{SHOP_URL}/admin/api/2023-10/orders.json"
    params = {
        "fulfillment_status": "fulfilled",  # pedidos con fulfillment creado
        "status": "any",
        "limit": 50,
        "order": "created_at desc"
    }

    while len(all_orders) < limit:
        r = safe_get(url, params=params)
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


def get_ctt_status(tracking_number):
    """Consulta el estado actual desde la API de CTT y devuelve estado + fecha real."""
    try:
        r = requests.get(CTT_API_URL + tracking_number, timeout=12)
        if r.status_code != 200:
            return {"status": "CTT API error", "date": None}

        data = r.json()
        if data.get("error"):
            return {"status": "Error en API CTT", "date": None}

        events = data.get("data", {}).get("shipping_history", {}).get("events", [])
        if not events:
            return {"status": "Sin eventos", "date": None}

        last_event = events[-1]
        return {
            "status": last_event.get("description", "Estado desconocido"),
            "date": last_event.get("event_date")  # Fecha real del evento
        }
    except Exception as e:
        return {"status": f"Error CTT: {e}", "date": None}


def map_ctt_to_shopify(status):
    """Mapea el estado devuelto por CTT al formato de Shopify."""
    status_map = {
        "En reparto": "out_for_delivery",
        "Entrega hoy": "out_for_delivery",
        "Entregado": "delivered",
        "En tránsito": "in_transit",
        "Recogido": "in_transit",
        "Grabado": "confirmed",
        "Reparto fallido": "failure"
    }
    return status_map.get(status, "in_transit")


def get_fulfillment_events(order_id, fulfillment_id):
    """Devuelve (set_de_statuses, ultimo_status) de los eventos de un fulfillment."""
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    try:
        r = safe_get(url)
        events = r.json().get("events", [])
        statuses = {e.get("status") for e in events if e.get("status")}
        last_status = events[-1].get("status") if events else None
        return statuses, last_status
    except Exception as e:
        log(f"❌ No se pudo obtener eventos para {order_id}: {e}")
        return set(), None


def create_fulfillment_event(order_id, fulfillment_id, ctt_status, event_date=None):
    """Crea un nuevo evento en Shopify con el estado de CTT y fecha real."""
    event_status = map_ctt_to_shopify(ctt_status)
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    payload = {
        "event": {
            "status": event_status,
            "message": f"Estado CTT: {ctt_status}"
        }
    }
    # Podemos mantener la fecha real sin provocar duplicados gracias al chequeo por existencia
    if event_date:
        payload["event"]["created_at"] = event_date

    r = safe_post(url, json=payload)
    if r.status_code == 201:
        log(f"✅ Evento '{event_status}' añadido a pedido {order_id} (CTT: {ctt_status})")
    else:
        log(f"❌ Error al añadir evento en pedido {order_id}: {r.status_code} - {r.text}")


def main():
    orders = get_fulfilled_orders()
    log(f"🔄 Procesando {len(orders)} pedidos...")

    for order in orders:
        fulfillments = order.get("fulfillments", [])
        if not fulfillments:
            continue

        # Si hay varios fulfillments, procesa todos (evita perder el correcto)
        for fulfillment in fulfillments:
            order_id = order["id"]
            fulfillment_id = fulfillment["id"]

            # Si el fulfillment está marcado como success (entregado por Shopify), no tocar
            if fulfillment.get("status") == "success":
                log(f"🔒 Pedido {order_id} fulfillment {fulfillment_id} ya 'success' → no se actualiza")
                continue

            tracking_number = fulfillment.get("tracking_number")
            if not tracking_number:
                log(f"⚠️ Pedido {order_id} fulfillment {fulfillment_id} sin número de seguimiento")
                continue

            ctt_result = get_ctt_status(tracking_number)
            ctt_status = ctt_result["status"]
            ctt_date = ctt_result["date"]

            if not isinstance(ctt_status, str) or "error" in ctt_status.lower():
                log(f"⚠️ Error con CTT para {order_id}: {ctt_status}")
                continue

            mapped_ctt_status = map_ctt_to_shopify(ctt_status)

            statuses, last_status = get_fulfillment_events(order_id, fulfillment_id)

            # Si ya existe un evento delivered, no volver a crear delivered
            if "delivered" in statuses and mapped_ctt_status == "delivered":
                log(f"🔒 Pedido {order_id} ya tiene 'delivered' (aunque no sea el último) → no se actualiza")
                continue

            # Si ya existe el mismo estado (in_transit, out_for_delivery, failure, confirmed...), no duplicar
            if mapped_ctt_status in statuses:
                log(f"ℹ️ Pedido {order_id} ya tiene evento '{mapped_ctt_status}' → no se actualiza")
                continue

            # Si no hay duplicado, creamos el evento con la fecha real de CTT (opcional)
            create_fulfillment_event(order_id, fulfillment_id, ctt_status, event_date=ctt_date)


if __name__ == "__main__":
    main()
