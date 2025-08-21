import os
import requests
from datetime import datetime
import time

# ===== Config =====
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="
LOG_FILE = "logs_actualizacion_envios.txt"
REQUEST_DELAY = 0.6  # ~1.6 req/s para ir seguros con Shopify
API_VERSION = "2023-10"  # usa la que ya tenías operativa

# ===== Utils =====
def log(message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{ts}] {message}"
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def _headers():
    return {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}

def safe_get(url, params=None):
    while True:
        r = requests.get(url, headers=_headers(), params=params)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1") or "1")
            time.sleep(max(wait, 1))
            continue
        r.raise_for_status()
        time.sleep(REQUEST_DELAY)
        return r

def safe_post(url, json):
    while True:
        r = requests.post(url, headers=_headers(), json=json)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1") or "1")
            time.sleep(max(wait, 1))
            continue
        time.sleep(REQUEST_DELAY)
        return r

# ===== Shopify =====
def get_fulfilled_orders(limit=300):
    """
    Trae pedidos cuyo order.fulfillment_status es 'fulfilled' (como tu script original),
    paginando hasta 'limit'.
    """
    all_orders = []
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders.json"
    params = {
        "status": "any",
        "fulfillment_status": "fulfilled",
        "limit": 50,
        "order": "created_at desc",
    }

    while len(all_orders) < limit:
        r = safe_get(url, params=params)
        data = r.json()
        orders = data.get("orders", [])
        if not orders:
            break
        all_orders.extend(orders)
        # paginación
        if "Link" in r.headers and 'rel="next"' in r.headers["Link"]:
            url = r.links["next"]["url"]
            params = None
        else:
            break

    return all_orders[:limit]

def get_fulfillment_events(order_id, fulfillment_id):
    """
    Devuelve (set_de_statuses_existentes, ultimo_status) para un fulfillment.
    """
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    try:
        r = safe_get(url)
        events = r.json().get("events", [])
        statuses = {e.get("status") for e in events if e.get("status")}
        last_status = events[-1].get("status") if events else None
        return statuses, last_status
    except Exception as e:
        log(f"❌ No se pudo obtener eventos para order {order_id} fulfillment {fulfillment_id}: {e}")
        return set(), None

def create_fulfillment_event(order_id, fulfillment_id, ctt_status, event_date=None):
    """
    Crea event en Shopify mapeando desde CTT y, si viene, con fecha real del evento.
    """
    event_status = map_ctt_to_shopify(ctt_status)
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    payload = {"event": {"status": event_status, "message": f"Estado CTT: {ctt_status}"}}
    if event_date:
        payload["event"]["created_at"] = event_date
    r = safe_post(url, json=payload)
    if r.status_code == 201:
        log(f"✅ Evento '{event_status}' añadido a pedido {order_id} (CTT: {ctt_status})")
    else:
        log(f"❌ Error al añadir evento en pedido {order_id}: {r.status_code} - {r.text}")

# ===== CTT =====
def get_ctt_status(tracking_number):
    """
    Consulta el estado actual desde la API de CTT y devuelve {status, date}.
    Estructura esperada (como tu script que funcionaba):
      data -> shipping_history -> events -> [ { description, event_date, ... }, ... ]
    """
    try:
        r = requests.get(CTT_API_URL + tracking_number, timeout=12)
        if r.status_code != 200:
            return {"status": "CTT API error", "date": None}
        data = r.json()
        if data.get("error"):
            return {"status": "Error en API CTT", "date": None}
        events = data.get("data", {}).get("shipping_history", {}).get("events", [])
        if not events:
            return {"status": "Sin eventos", "date": None}
        last = events[-1]
        return {"status": last.get("description", "Estado desconocido"),
                "date": last.get("event_date")}
    except Exception as e:
        return {"status": f"Error CTT: {e}", "date": None}

def map_ctt_to_shopify(status):
    """
    Mapea el estado devuelto por CTT al formato de Shopify.
    """
    status_map = {
        "En reparto": "out_for_delivery",
        "Entrega hoy": "out_for_delivery",
        "Entregado": "delivered",
        "En tránsito": "in_transit",
        "Recogido": "in_transit",
        "Grabado": "confirmed",
        "Reparto fallido": "failure",
    }
    # fallback por si CTT cambia textos
    if not isinstance(status, str):
        return "in_transit"
    return status_map.get(status, _fallback_map(status))

def _fallback_map(text):
    s = text.lower()
    if "entregado" in s:
        return "delivered"
    if "reparto" in s:
        return "out_for_delivery"
    if "fallid" in s or "incidenc" in s or "anulación" in s or "anulacion" in s:
        return "failure"
    return "in_transit"

# ===== Main =====
def main():
    log("🚀 Iniciando actualización de estados...")
    orders = get_fulfilled_orders()
    log(f"📦 Pedidos (fulfilled) recuperados: {len(orders)}")

    count_checked = 0
    count_created = 0
    count_skipped_success = 0
    count_no_tracking = 0
    count_duplicated = 0
    count_errors = 0

    for order in orders:
        fulfillments = order.get("fulfillments", [])
        if not fulfillments:
            continue

        for f in fulfillments:
            order_id = order["id"]
            fulfillment_id = f["id"]

            # Si el fulfillment está marcado success (entregado), no tiene sentido seguir añadiendo eventos
            if f.get("status") == "success":
                count_skipped_success += 1
                log(f"🔒 Pedido {order_id} fulfillment {fulfillment_id} ya 'success' → no se actualiza")
                continue

            tracking_number = f.get("tracking_number")
            if not tracking_number:
                count_no_tracking += 1
                log(f"⚠️ Pedido {order_id} fulfillment {fulfillment_id} sin número de seguimiento")
                continue

            ctt = get_ctt_status(tracking_number)
            ctt_status, ctt_date = ctt["status"], ctt["date"]
            if not isinstance(ctt_status, str) or "error" in ctt_status.lower():
                count_errors += 1
                log(f"⚠️ Error con CTT para pedido {order_id}: {ctt_status}")
                continue

            mapped = map_ctt_to_shopify(ctt_status)
            statuses, last_status = get_fulfillment_events(order_id, fulfillment_id)

            log(f"🔎 order {order_id} | f{id}: {fulfillment_id} | CTT='{ctt_status}'→'{mapped}' | "
                f"Shopify_last='{last_status}' | existentes={sorted(list(statuses))}")

            # Si YA existe un evento con ese mismo status, no duplicar
            if mapped in statuses:
                count_duplicated += 1
                log(f"ℹ️ Pedido {order_id} ya tiene evento '{mapped}' → no se actualiza")
                continue

            # Crear evento nuevo
            try:
                create_fulfillment_event(order_id, fulfillment_id, ctt_status, event_date=ctt_date)
                count_created += 1
            except Exception as e:
                count_errors += 1
                log(f"❌ Error al crear evento para {order_id}: {e}")

            count_checked += 1

    log(f"🏁 Fin. Resumen → creados:{count_created} | ya_success:{count_skipped_success} | "
        f"sin_tracking:{count_no_tracking} | duplicados:{count_duplicated} | errores:{count_errors}")

if __name__ == "__main__":
    main()
