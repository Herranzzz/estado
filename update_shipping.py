import os
import requests
from datetime import datetime
from dateutil.parser import parse

# Shopify API
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN") 

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
    """Consulta el estado actual desde la API de CTT y devuelve estado + fecha real."""
    r = requests.get(CTT_API_URL + tracking_number)
    if r.status_code != 200:
        return {"status": "CTT API error", "date": None}

    data = r.json()
    if data.get("error") is not None:
        return {"status": "Error en API CTT", "date": None}

    events = data.get("data", {}).get("shipping_history", {}).get("events", [])
    if not events:
        return {"status": "Sin eventos", "date": None}

    last_event = events[-1]
    return {
        "status": last_event.get("description", "Estado desconocido"),
        "date": last_event.get("event_date")  # Fecha real del evento
    }

def map_ctt_to_shopify(status):
    """Mapea el estado devuelto por CTT al formato de Shopify."""
    status_map = {
        "En reparto": "out_for_delivery",
        "Entrega hoy": "out_for_delivery",
        "Entregado": "delivered",
        "En tránsito": "in_transit",
        "Recogido": "in_transit",
        "Pendiente de recepción en CTT Express": "confirmed",
        "Reparto fallido": "failure"
    }
    return status_map.get(status, "in_transit")

def get_last_fulfillment_event(order_id, fulfillment_id):
    """Obtiene el último evento registrado en Shopify con fecha y estado."""
    url = f"{SHOP_URL}/admin/api/2023-10/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        log(f"❌ No se pudo obtener eventos para {order_id}: {r.status_code}")
        return None, None

    events = r.json().get("events", [])
    if not events:
        return None, None

    last_event = events[-1]
    last_status = last_event.get("status")
    last_date = parse(last_event.get("created_at"))
    return last_status, last_date

def create_fulfillment_event(order_id, fulfillment_id, status, event_date=None):
    """Crea un nuevo evento en Shopify con la fecha real si procede."""
    event_status = map_ctt_to_shopify(status)
    
    last_status, last_date = get_last_fulfillment_event(order_id, fulfillment_id)
    
    if last_status == event_status:
        if event_date:
            ctt_event_date = parse(event_date)
            if last_date.date() >= ctt_event_date.date():
                log(f"🔒 Pedido {order_id} ya tiene estado '{event_status}' actualizado para esa fecha, no se crea evento")
                return
        else:
            log(f"ℹ️ Estado sin cambios para pedido {order_id} ({event_status})")
            return

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
    if event_date:
        payload["event"]["created_at"] = parse(event_date).isoformat()

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

        # Estado actual y fecha en CTT
        ctt_result = get_ctt_status(tracking_number)
        ctt_status = ctt_result["status"]
        ctt_date = ctt_result["date"]

        if "error" in ctt_status.lower():
            log(f"⚠️ Error con CTT para {order_id}: {ctt_status}")
            continue

        # Crear o actualizar evento solo si hay cambio
        create_fulfillment_event(order_id, fulfillment_id, ctt_status, event_date=ctt_date)

if __name__ == "__main__":
    main()
