import os
import requests
from datetime import datetime

# Shopify API
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN") or "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"

# CTT API
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

# Archivo de log
LOG_FILE = "logs_actualizacion_envios.txt"


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


def get_fulfilled_orders(limit=300):
    """Obtiene hasta 'limit' pedidos con fulfillment completado."""
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    all_orders = []
    url = f"{SHOP_URL}/admin/api/2023-10/orders.json"
    params = {
        "fulfillment_status": "fulfilled",
        "status": "any",
        "limit": 50,
        "order": "created_at desc"
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


def get_ctt_status(tracking_number):
    """Consulta el estado actual desde la API de CTT."""
    r = requests.get(CTT_API_URL + tracking_number)
    if r.status_code != 200:
        return "CTT API error"
    
    data = r.json()
    if data.get("error") is not None:
        return "Error en API CTT"

    events = data.get("data", {}).get("shipping_history", {}).get("events", [])
    if not events:
        return "Sin eventos"

    last_event = events[-1]
    return last_event.get("description", "Estado desconocido")


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


def get_last_fulfillment_event_status(order_id, fulfillment_id):
    """Obtiene el último estado registrado en Shopify para un fulfillment."""
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        log(f"❌ No se pudo obtener eventos para {order_id}: {r.status_code}")
        return None

    events = r.json().get("events", [])
    if not events:
        return None

    return events[-1].get("status")


def create_fulfillment_event(order_id, fulfillment_id, status):
    """Crea un nuevo evento en Shopify con el estado de CTT."""
    event_status = map_ctt_to_shopify(status)
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "event": {
            "status": event_status,
            "message": f"Estado CTT: {status}"
        }
    }
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code == 201:
        log(f"✅ Evento '{event_status}' añadido a pedido {order_id} (CTT: {status})")
    else:
        log(f"❌ Error al añadir evento en pedido {order_id}: {r.status_code} - {r.text}")


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

        tracking_number = fulfillment.get("tracking_number")
        if not tracking_number:
            log(f"⚠️ Pedido {order_id} sin número de seguimiento")
            continue

        # Estado actual en CTT
        ctt_status = get_ctt_status(tracking_number)
        if "error" in ctt_status.lower():
            log(f"⚠️ Error con CTT para {order_id}: {ctt_status}")
            continue

        mapped_ctt_status = map_ctt_to_shopify(ctt_status)

        # Estado actual en Shopify
        last_status = get_last_fulfillment_event_status(order_id, fulfillment_id)

        # 🔒 BLOQUEO: si Shopify ya tiene delivered, no actualizar
        if last_status == "delivered":
            log(f"🔒 Pedido {order_id} ya está entregado en Shopify, no se actualiza")
            continue

        # 🔒 BLOQUEO: si CTT indica entregado y Shopify ya lo tiene así
        if mapped_ctt_status == "delivered" and last_status == "delivered":
            log(f"🔒 Pedido {order_id} ya marcado como entregado, no se actualiza")
            continue

        # Evitar actualizar si no hay cambios
        if last_status == mapped_ctt_status:
            log(f"ℹ️ Estado sin cambios para pedido {order_id} ({mapped_ctt_status})")
            continue

        # Actualizar en Shopify
        create_fulfillment_event(order_id, fulfillment_id, ctt_status)


if __name__ == "__main__":
    main()
