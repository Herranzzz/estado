import requests
import os
import time

SHOP = "48d471-2"
ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
}


def get_fulfilled_orders():
    url = f"https://{SHOP}.myshopify.com/admin/api/2023-10/orders.json"
    params = {
        "fulfillment_status": "fulfilled",
        "status": "any",
        "limit": 50
    }

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()["orders"]


def already_has_delivered_event(order_id, fulfillment_id):
    """Comprueba si ya hay un evento 'delivered' en el fulfillment"""
    url = f"https://{SHOP}.myshopify.com/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"⚠ Error al verificar eventos para fulfillment {fulfillment_id}")
        return False

    events = response.json().get("events", [])
    return any(event.get("status") == "delivered" for event in events)


def create_delivery_event(order_id, fulfillment_id):
    url = f"https://{SHOP}.myshopify.com/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    data = {
        "event": {
            "status": "delivered",
            "notify_customer": True
        }
    }

    response = requests.post(url, headers=HEADERS, json=data)
    if response.status_code == 201:
        print(f"✅ Evento 'delivered' creado para fulfillment {fulfillment_id}")
    else:
        print(f"❌ Error creando evento: {response.status_code} - {response.text}")


def main():
    print("🔄 Buscando pedidos entregados...")
    orders = get_fulfilled_orders()

    for order in orders:
        order_id = order["id"]
        fulfillments = order.get("fulfillments", [])

        for fulfillment in fulfillments:
            fulfillment_id = fulfillment["id"]
            if not already_has_delivered_event(order_id, fulfillment_id):
                create_delivery_event(order_id, fulfillment_id)
                time.sleep(1)
            else:
                print(f"⏭ Ya tiene evento 'delivered': {fulfillment_id}")


if __name__ == "__main__":
    main()
