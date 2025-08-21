import os
import requests
from datetime import datetime

# Shopify API
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# CTT API
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

# Archivo de log
LOG_FILE = "logs_actualizacion_envios.txt"


def log(message):
    """Escribe logs en archivo y en consola"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {message}"
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def get_open_orders():
    """Obtiene pedidos abiertos con fulfillments"""
    url = f"{SHOP_URL}/admin/api/2023-10/orders.json?status=any&fulfillment_status=unfulfilled,partial"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN}
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.json().get("orders", [])
    except Exception as e:
        log(f"❌ Error al obtener pedidos: {e}")
        return []


def get_tracking_info(tracking_number):
    """Consulta estado en CTT"""
    try:
        r = requests.get(f"{CTT_API_URL}{tracking_number}")
        r.raise_for_status()
        data = r.json()
        if not data or not isinstance(data, list):
            return None
        return data[-1]  # último evento
    except Exception as e:
        log(f"❌ Error al consultar CTT {tracking_number}: {e}")
        return None


def map_ctt_to_shopify(ctt_status):
    """Mapea estado CTT → Shopify"""
    if not ctt_status:
        return None
    s = ctt_status.lower()
    if "entregado" in s:
        return "delivered"
    elif "reparto" in s:
        return "out_for_delivery"
    elif "fallido" in s or "incidencia" in s:
        return "failure"
    return "in_transit"


def get_last_fulfillment_event_status(order_id, fulfillment_id):
    """Obtiene último estado registrado en Shopify para un fulfillment"""
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN}
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        events = r.json().get("events", [])
        if not events:
            return None
        # Shopify ya devuelve directamente: in_transit, delivered, failure, etc.
        return events[-1].get("status")
    except Exception as e:
        log(f"❌ Error al obtener eventos de Shopify {order_id}: {e}")
        return None


def add_fulfillment_event(order_id, fulfillment_id, status, message=None):
    """Crea un nuevo fulfillment_event en Shopify"""
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}
    payload = {"event": {"status": status}}
    if message:
        payload["event"]["message"] = message
    try:
        r = requests.post(url, headers=headers, json=payload)
        r.raise_for_status()
        log(f"✅ Evento '{status}' añadido a pedido {order_id}")
    except Exception as e:
        log(f"❌ Error al añadir evento en pedido {order_id}: {e}")


def main():
    log("🚀 Iniciando actualización de estados...")
    orders = get_open_orders()

    for order in orders:
        order_id = order["id"]
        fulfillments = order.get("fulfillments", [])

        for fulfillment in fulfillments:
            tracking_number = fulfillment.get("tracking_number")
            if not tracking_number:
                continue

            # Estado actual en CTT
            ctt_info = get_tracking_info(tracking_number)
            if not ctt_info:
                continue

            mapped_ctt_status = map_ctt_to_shopify(ctt_info.get("s"))

            # Último estado en Shopify
            last_status = get_last_fulfillment_event_status(order_id, fulfillment["id"])

            # Evitar duplicados
            if last_status == mapped_ctt_status:
                log(f"ℹ️ Estado sin cambios para pedido {order_id} ({mapped_ctt_status})")
                continue

            # Añadir nuevo evento
            add_fulfillment_event(order_id, fulfillment["id"], mapped_ctt_status, ctt_info.get("s"))

    log("🏁 Proceso finalizado.")


if __name__ == "__main__":
    main()
