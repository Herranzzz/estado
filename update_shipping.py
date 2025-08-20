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

# Límite de peticiones por segundo para Shopify
REQUEST_DELAY = 0.6  # aprox 1.6 requests/segundo


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


def get_fulfilled_orders(limit=300):
    """Obtiene pedidos con fulfillment completado."""
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}
    all_orders = []
    url = f"{SHOP_URL}/admin/api/2023-10/orders.json"
    params = {"fulfillment_status": "fulfilled", "status": "any", "limit": 50, "order": "created_at desc"}

    while len(all_orders) < limit:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        orders = data.get("orders", [])
        if not orders:
            break
        all_orders.extend(orders)

        if "Link" in r.headers and 'rel="next"' in r.headers["Link"]:
            url = r.links["next"]["url"]
            params = None
        else:
            break

    return all_orders[:limit]


def get_ctt_status(tracking_number):
    """Consulta estado actual de CTT y devuelve status + fecha."""
    try:
        r = requests.get(CTT_API_URL + tracking_number, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            return {"status": "Error CTT", "date": None}
        events = data.get("data", {}).get("shipping_history", {}).get("events", [])
        if not events:
            return {"status": "Sin eventos", "date": None}
        last_event = events[-1]
        return {"status": last_event.get("description", "Desconocido"), "date": last_event.get("event_date")}
    except Exception as e:
        return {"status": f"Error CTT: {e}", "date": None}


def map_ctt_to_shopify(status):
    """Mapea estados CTT a Shopify."""
    mapping = {
        "En reparto": "out_for_delivery",
        "Entrega hoy": "out_for_delivery",
        "Entregado": "delivered",
        "En tránsito": "in_transit",
        "Recogido": "in_transit",
        "Grabado": "confirmed",
        "Reparto fallido": "failure"
    }
    return mapping.get(status, "in_transit")


def get_last_fulfillment_event_status(order_id, fulfillment_id):
    """Obtiene último estado registrado en Shopify para un fulfillment."""
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        events = r.json().get("events", [])
        if not events:
            return None
        # Shopify ya devuelve 'in_transit', 'delivered', 'out_for_delivery', etc.
        return events[-1].get("status")
    except Exception as e:
        log(f"❌ Error al obtener eventos de Shopify {order_id}: {e}")
        return None


def create_fulfillment_event(order_id, fulfillment_id, status, event_date=None):
    """Crea un evento en Shopify con el estado de CTT."""
    event_status = map_ctt_to_shopify(status)
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}
    payload = {"event": {"status": event_status, "message": f"Estado CTT: {status}"}}
    if event_date:
        payload["event"]["created_at"] = event_date
    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code == 201:
            log(f"✅ Evento '{event_status}' añadido a pedido {order_id} (CTT: {status})")
        else:
            log(f"❌ Error al añadir evento {order_id}: {r.status_code} - {r.text}")
    except Exception as e:
        log(f"❌ Error al añadir evento {order_id}: {e}")


def main():
    orders = get_fulfilled_orders()
    log(f"🔄 Procesando {len(orders)} pedidos...")

    for order in orders:
        fulfillments = order.get("fulfillments", [])
        if not fulfillments:
            continue

        fulfillment = fulfillments[0]
        order_id = order["id"]
        fulfillment_id = fulfillment["id"]

        # 🔒 No actualizar si fulfillment ya entregado
        if fulfillment.get("status") == "success":
            log(f"🔒 Pedido {order_id} ya entregado (fulfillment 'success')")
            continue

        tracking_number = fulfillment.get("tracking_number")
        if not tracking_number:
            log(f"⚠️ Pedido {order_id} sin número de seguimiento")
            continue

        ctt_result = get_ctt_status(tracking_number)
        ctt_status = ctt_result["status"]
        ctt_date = ctt_result["date"]

        if "error" in ctt_status.lower():
            log(f"⚠️ Error CTT pedido {order_id}: {ctt_status}")
            continue

        mapped_ctt_status = map_ctt_to_shopify(ctt_status)
        last_status = get_last_fulfillment_event_status(order_id, fulfillment_id)

        if last_status == "delivered":
            log(f"🔒 Pedido {order_id} ya marcado como entregado, no se actualiza")
            continue

        if last_status == mapped_ctt_status:
            log(f"ℹ️ Estado sin cambios para pedido {order_id} ({mapped_ctt_status})")
            continue

        create_fulfillment_event(order_id, fulfillment_id, ctt_status, event_date=ctt_date)
        time.sleep(REQUEST_DELAY)  # para no exceder límite Shopify


if __name__ == "__main__":
    main()
