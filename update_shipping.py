import os
import requests
import time
from datetime import datetime

# Shopify API
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN") 

# CTT API
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="

# Archivos de log
LOG_FILE = "logs_actualizacion_envios.txt"
EMAILS_SENT_FILE = "emails_enviados.txt"


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)


def load_sent_emails():
    if not os.path.exists(EMAILS_SENT_FILE):
        return set()
    with open(EMAILS_SENT_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f.readlines())


def mark_email_sent(order_id):
    with open(EMAILS_SENT_FILE, "a", encoding="utf-8") as f:
        f.write(f"{order_id}\n")


def get_fulfilled_orders(limit=300):
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
    r = requests.get(CTT_API_URL + tracking_number)
    if r.status_code != 200:
        return {"status": "CTT API error", "date": None}
    data = r.json()
    events = data.get("data", {}).get("shipping_history", {}).get("events", [])
    if not events:
        return {"status": "Sin eventos", "date": None}
    last_event = events[-1]
    return {"status": last_event.get("description", "Estado desconocido"), "date": last_event.get("event_date")}


def map_ctt_to_shopify(status):
    status_map = {
        "En reparto": "out_for_delivery",
        "Entrega hoy": "out_for_delivery",
        "Entregado": "delivered",
        "Recogido": "picked_up",
        "Reparto fallido": "failure",
        "En tránsito": "in_transit"
    }
    return status_map.get(status, "in_transit")


def get_fulfillment_events(order_id, fulfillment_id):
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        log(f"❌ No se pudo obtener eventos para {order_id}: {r.status_code}")
        return []
    return r.json().get("events", [])


def create_fulfillment_event(order_id, fulfillment_id, status, event_date=None):
    event_status = map_ctt_to_shopify(status)
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}
    payload = {"event": {"status": event_status, "message": f"Estado CTT: {status}"}}
    if event_date:
        payload["event"]["created_at"] = event_date
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code == 201:
        log(f"✅ Evento '{event_status}' añadido a pedido {order_id} (CTT: {status})")
        return True
    else:
        log(f"❌ Error al añadir evento en pedido {order_id}: {r.status_code} - {r.text}")
        return False


def send_shopify_email(order_id, sent_emails):
    """Envía la notificación de Shopify al cliente (solo para recogido)"""
    if order_id in sent_emails:
        log(f"🔒 Pedido {order_id} ya tiene correo de recogido enviado")
        return

    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/send.json"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}
    payload = {"fulfillment": {"notify_customer": True}}
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code == 200:
        log(f"📧 Notificación enviada a cliente por pedido {order_id}")
        mark_email_sent(order_id)
        sent_emails.add(order_id)
    else:
        log(f"❌ Error enviando notificación para pedido {order_id}: {r.status_code} - {r.text}")


def main():
    sent_emails = load_sent_emails()
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

        ctt_result = get_ctt_status(tracking_number)
        ctt_status = ctt_result["status"]
        ctt_date = ctt_result["date"]

        mapped_ctt_status = map_ctt_to_shopify(ctt_status)
        events = get_fulfillment_events(order_id, fulfillment_id)

        # Evitar duplicados
        if any(e["status"] == mapped_ctt_status for e in events):
            log(f"🔒 Pedido {order_id} ya tiene evento '{mapped_ctt_status}' registrado")
            continue

        # Crear evento
        if create_fulfillment_event(order_id, fulfillment_id, ctt_status, event_date=ctt_date):
            # Enviar correo solo si es "picked_up"
            if mapped_ctt_status == "picked_up":
                send_shopify_email(order_id, sent_emails)

        # Pausa para no superar 2 llamadas/segundo
        time.sleep(0.5)


if __name__ == "__main__":
    main()
