import os
import requests

# Shopify API
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN") 

# CTT API
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

# Obtener hasta 300 pedidos con fulfillment
def get_fulfilled_orders(limit=300):
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
        if "Link" in r.headers and 'rel="next"' in r.headers["Link"]:
            url = r.links["next"]["url"]
            params = None
        else:
            break

    return all_orders[:limit]

# Consultar estado actual desde CTT
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

    last_event = events[-1]
    return last_event.get("description", "Estado desconocido")

# Crear evento en Shopify
def create_fulfillment_event(order_id, fulfillment_id, status):
    status_map = {
        "En reparto": "out_for_delivery",
        "Entrega hoy": "out_for_delivery",
        "Entregado": "delivered",
        "En tránsito": "in_transit",
        "Recogido": "in_transit",
        "Grabado": "confirmed",
        "Reparto fallido": "exception",
        "Recogido": "in_transit"
    }

    event_status = status_map.get(status, "in_transit")
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
        print(f"✅ Evento '{event_status}' añadido a pedido {order_id}")
    else:
        print(f"❌ Error al añadir evento: {r.status_code} - {r.text}")

# Verificar si ya fue entregado para no duplicar
def ya_entregado(order):
    fulfillments = order.get("fulfillments", [])
    for f in fulfillments:
        events = f.get("tracking_events", [])
        for e in events:
            if e.get("status") == "delivered":
                return True
    return False

# Main
def main():
    orders = get_fulfilled_orders()
    for order in orders:
        if ya_entregado(order):
            continue

        fulfillments = order.get("fulfillments", [])
        if not fulfillments:
            continue

        fulfillment = fulfillments[0]
        tracking_number = fulfillment.get("tracking_number")
        if not tracking_number:
            continue

        status = get_ctt_status(tracking_number)
        create_fulfillment_event(order["id"], fulfillment["id"], status)

if __name__ == "__main__":
    main()
