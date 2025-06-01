import os
import requests
import time

# Shopify
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# CTT
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

def get_fulfilled_orders(limit=300):
    all_orders = []
    page_info = None
    base_url = f"{SHOP_URL}/admin/api/2023-10/orders.json?fulfillment_status=fulfilled&status=any&limit=50"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    while len(all_orders) < limit:
        url = base_url
        if page_info:
            url = f"{SHOP_URL}/admin/api/2023-10/orders.json?limit=50&page_info={page_info}"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        orders = r.json()["orders"]
        if not orders:
            break
        all_orders.extend(orders)
        link = r.headers.get("Link", "")
        if 'rel="next"' in link:
            try:
                page_info = link.split('page_info=')[1].split('>')[0].split('&')[0]
            except IndexError:
                break
        else:
            break
    return all_orders[:limit]

def get_ctt_status(tracking_number):
    r = requests.get(CTT_API_URL + tracking_number)
    if r.status_code != 200:
        return "CTT API error"
    data = r.json()
    if data.get("error") is not None:
        return "Error en API CTT"
    events = data.get("data", {}).get("shipping_history", {}).get("events", [])
    if not events:
        return "Sin eventos"
    return events[-1].get("description", "Estado desconocido")

def is_already_delivered(order_id, fulfillment_id):
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return False
    events = r.json().get("fulfillment_events", [])
    for event in events:
        if event.get("status") == "delivered":
            return True
    return False

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
            "message": f"Estado CTT: {status}",
            "notify_customer": event_status == "delivered"
        }
    }

    r = requests.post(url, headers=headers, json=data)
    if r.status_code == 201:
        print(f"✅ Pedido {order_id}: '{event_status}' añadido.")
    else:
        print(f"❌ Pedido {order_id}: Error {r.status_code} - {r.text}")

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

    if is_already_delivered(order["id"], fulfillment["id"]):
        continue

    status = get_ctt_status(tracking_number)
    create_fulfillment_event(order["id"], fulfillment["id"], status)
    time.sleep(1)  # Evita saturar APIs
