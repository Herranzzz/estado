import requests
import os
import time

# Configura tu dominio y token de Shopify
SHOP = "48d471-2"  # cambia si usas otro dominio
ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
}


def get_fulfilled_orders():
    """Obtiene pedidos que ya están completamente entregados"""
    url = f"https://{SHOP}.myshopify.com/admin/api/2023-10/orders.json"
    params = {
        "fulfillment_status": "fulfilled",
        "status": "any",
        "limit": 50
    }

    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()["orders"]


def create_delivery_event(order_id, fulfillment_id):
    """Crea el evento de entrega en Shopify y notifica al cliente"""
    url = f"https://{SHOP}.myshopify.com/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    data = {
        "event": {
            "status": "delivered",
            "notify_customer": True
        }
    }

    response = requests.post(url, headers=HEADERS, json=data)
    if response.status_code == 201:
        print(f"✔ Entrega registrada y correo enviado para fulfillment {fulfillment_id}")
    else:
        print(f"❌ Error al registrar entrega: {response.status_code} - {response.text}")


def main():
    print("🔄 Buscando pedidos entregados...")
    orders = get_fulfilled_orders()

    for order in orders:
        order_id = order["id"]
        fulfillments = order.get("fulfillments", [])

        for fulfillment in fulfillments:
            fulfillment_id = fulfillment["id"]
            # Evita duplicados: no crees eventos si ya existe el de "delivered"
            events_url = f"https://{SHOP}.myshopify.com/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
            events_resp = requests.get(events_url, headers=HEADERS)
            events = events_resp.json().get("events", [])

            already_delivered = any(e["status"] == "delivered" for e in events)
            if not already_delivered:
                create_delivery_event(order_id, fulfillment_id)
                time.sleep(1)  # para evitar el rate limit
            else:
                print(f"⚠ Ya existe evento 'delivered' para fulfillment {fulfillment_id}")


if __name__ == "__main__":
    main()
