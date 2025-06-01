import os
import time
import requests

# Datos Shopify
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# API CTT Express
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

def get_fulfilled_orders(limit=300):
    all_orders = []
    last_id = None
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    while len(all_orders) < limit:
        params = {
            "fulfillment_status": "fulfilled",
            "status": "any",
            "limit": 50
        }
        if last_id:
            params["since_id"] = last_id

        url = f"{SHOP_URL}/admin/api/2023-10/orders.json"
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        orders = r.json().get("orders", [])
        if not orders:
            break

        all_orders.extend(orders)
        last_id = orders[-1]["id"]

        time.sleep(1)  # Evita exceder el límite de Shopify

    return all_orders[:limit]

def get_ctt_status(tracking_number):
    url = CTT_API_URL + tracking_number
    r = requests.get(url)
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

# MAIN
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

    # Evita duplicar eventos si ya está entregado
    if status == "Entregado":
        events_url = f"{SHOP_URL}/admin/api/2023-10/orders/{order['id']}/fulfillments/{fulfillment['id']}/events.json"
        r = requests.get(events_url, headers={"X-Shopify-Access-Token": ACCESS_TOKEN})
        r.raise_for_status()
        existing_events = r.json().get("fulfillment_events", [])
        if any(event.get("status") == "delivered" for event in existing_events):
            print(f"📦 Pedido {order['id']} ya tiene estado 'delivered'")
            continue

    create_fulfillment_event(order["id"], fulfillment["id"], status)
