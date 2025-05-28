import os

ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# El resto del código igual

import requests

# Datos Shopify
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = "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"  # Cambia aquí

# API CTT Express
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

def get_fulfilled_orders():
    url = f"{SHOP_URL}/admin/api/2023-10/orders.json?fulfillment_status=fulfilled&status=any&limit=50"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["orders"]

def get_ctt_status(tracking_number):
    url = CTT_API_URL + tracking_number
    r = requests.get(url)
    if r.status_code != 200:
        return "CTT API error"
    
    data = r.json()
    
    if data.get("error") is not None:
        return "Error en API CTT"
    
    shipping_history = data.get("data", {}).get("shipping_history", {})
    events = shipping_history.get("events", [])
    
    if not events:
        return "Sin eventos"
    
    last_event = events[-1]
    status = last_event.get("description", "Estado desconocido")
    return status

def create_fulfillment_event(order_id, fulfillment_id, status):
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    
    status_map = {
        "En reparto": "out_for_delivery",
        "Entregado": "delivered",
        "En tránsito": "in_transit",
        "Recogido": "in_transit",
        "Grabado": "confirmed",
        "Reparto fallido": "exception",
        "Entrega hoy": "out_for_delivery"
    }
    
    event_status = status_map.get(status, "in_transit")
    
    data = {
        "event": {
            "status": event_status,
            "message": f"Estado CTT: {status}"
        }
    }
    r = requests.post(url, headers=headers, json=data)
    if r.status_code == 201:
        print(f"✅ Evento '{event_status}' añadido a pedido {order_id}")
    else:
        print(f"❌ Error al añadir evento: {r.status_code} {r.text}")

# Main
orders = get_fulfilled_orders()
for order in orders:
    fulfillments = order.get("fulfillments", [])
    if not fulfillments:
        continue
    
    fulfillment = fulfillments[0]
    tracking_number = fulfillment.get("tracking_number")
    if not tracking_number:
        continue

    status = get_ctt_status(tracking_number)
    create_fulfillment_event(order["id"], fulfillment["id"], status)
