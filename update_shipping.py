import os
import requests
from datetime import datetime
import time
import re

# ===== Config =====
SHOP_URL = "https://48d471-2.myshopify.com"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
CTT_API_URL = "https://wct.cttexpress.com/p_track_redis.php?sc="
LOG_FILE = "logs_actualizacion_envios.txt"
REQUEST_DELAY = 0.6  # ~1.6 req/s
API_VERSION = "2023-10"

# ===== Utils =====
def log(message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {message}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def _headers():
    return {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}

def safe_get(url, params=None, timeout=12):
    while True:
        r = requests.get(url, headers=_headers(), params=params, timeout=timeout)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1") or "1")
            log(f"⚠️ 429 recibido. Esperando {wait}s...")
            time.sleep(max(wait, 1))
            continue
        r.raise_for_status()
        time.sleep(REQUEST_DELAY)
        return r

def safe_post(url, json=None, timeout=12):
    while True:
        r = requests.post(url, headers=_headers(), json=json, timeout=timeout)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1") or "1")
            log(f"⚠️ 429 recibido (post). Esperando {wait}s...")
            time.sleep(max(wait, 1))
            continue
        time.sleep(REQUEST_DELAY)
        return r

# ===== Shopify helpers =====
def get_fulfilled_orders(limit=300):
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
        if "Link" in r.headers and 'rel="next"' in r.headers["Link"]:
            url = r.links["next"]["url"]
            params = None
        else:
            break
    return all_orders[:limit]

def get_fulfillment_events(order_id, fulfillment_id):
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    try:
        r = safe_get(url)
        events = r.json().get("events", [])
        statuses = {e.get("status") for e in events if e.get("status")}
        last_status = events[-1].get("status") if events else None
        return statuses, last_status, events
    except Exception as e:
        log(f"❌ Error al obtener eventos para order {order_id} fulfillment {fulfillment_id}: {e}")
        return set(), None, []

def create_fulfillment_event(order_id, fulfillment_id, event_status, message=None, event_date=None):
    url = f"{SHOP_URL}/admin/api/{API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    payload = {"event": {"status": event_status}}
    if message:
        payload["event"]["message"] = message
    if event_date:
        payload["event"]["created_at"] = event_date
    r = safe_post(url, json=payload)
    if r.status_code == 201:
        log(f"✅ Evento '{event_status}' añadido a pedido {order_id} ({fulfillment_id})")
    else:
        log(f"❌ Fallo al crear evento {order_id} ({fulfillment_id}): {r.status_code} - {r.text}")

# ===== CTT parsing & mapping =====
def _extract_from_event(ev):
    """Intenta extraer código y texto relevantes de un evento CTT (varias claves posibles)."""
    if not isinstance(ev, dict):
        return None, None
    # Prioridad: keys que suelen contener código o texto
    code_keys = ["status", "code", "event_code", "status_code"]
    text_keys = ["description", "message", "s", "texto", "status_description", "statusText", "statusTextEN"]
    code = None
    text = None
    for k in code_keys:
        if k in ev and ev[k] is not None:
            code = str(ev[k])
            break
    for k in text_keys:
        if k in ev and ev[k]:
            text = str(ev[k])
            break
    # si no hay texto, buscar en valores por si están en otras claves
    if not text:
        for v in ev.values():
            if isinstance(v, str) and len(v) > 0:
                # posible best-effort: primera cadena razonable
                text = v
                break
    return code, text

def get_ctt_status(tracking_number):
    """
    Llama a la API CTT y devuelve dict:
      {
        "code": <codigo si hay, str o None>,
        "text": <texto del evento, str o None>,
        "event_date": <fecha si hay, str or None>,
        "raw_event": <dict raw> or None,
        "no_events": True/False
      }
    """
    try:
        r = requests.get(f"{CTT_API_URL}{tracking_number}", timeout=12)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log(f"⚠️ Error al llamar CTT {tracking_number}: {e}")
        return {"code": None, "text": None, "event_date": None, "raw_event": None, "no_events": True}

    # Buscar events en varias estructuras posibles
    events = None
    if isinstance(data, dict):
        # estándar: data -> shipping_history -> events
        events = data.get("data", {}).get("shipping_history", {}).get("events")
        # a veces la respuesta puede ser lista arriba
        if events is None:
            # si el propio objeto contiene 'events'
            events = data.get("events")
    elif isinstance(data, list) and len(data) > 0:
        # algunos endpoints devuelven lista de eventos
        events = data

    if not events:
        return {"code": None, "text": None, "event_date": None, "raw_event": None, "no_events": True}

    # Tomamos el último evento relevante (el último con alguna cadena)
    last_event = None
    for ev in reversed(events):
        if isinstance(ev, dict) and any(isinstance(v, (str, int)) and str(v).strip() for v in ev.values()):
            last_event = ev
            break
    if not last_event:
        last_event = events[-1]

    code, text = _extract_from_event(last_event)
    # fecha
    event_date = None
    for k in ("event_date", "date", "timestamp", "time"):
        if isinstance(last_event, dict) and k in last_event and last_event[k]:
            event_date = last_event[k]
            break

    return {"code": code, "text": text, "event_date": event_date, "raw_event": last_event, "no_events": False}

def map_ctt_to_shopify(code, text):
    """
    Mapea preferentemente por código; si no, heurísticas sobre texto.
    Devuelve uno de: in_transit, out_for_delivery, delivered, failure, confirmed
    """
    # mapping de códigos (ejemplo común)
    if code:
        code_s = str(code).strip()
        code_map = {
            "10": "in_transit",   # admitido / recibido
            "20": "in_transit",   # en tránsito
            "30": "out_for_delivery", # en reparto
            "40": "delivered",    # entregado
            "50": "failure",      # incidencia / devuelto
        }
        if code_s in code_map:
            return code_map[code_s]

    # heurísticas sobre texto
    if text:
        s = text.lower()
        if re.search(r"entreg|recolectado", s):
            return "delivered"
        if re.search(r"\b(entrega hoy|en reparto|en reparto\b|reparto|repartiendo)\b", s):
            return "out_for_delivery"
        if re.search(r"transit|tr[nó]nsit|en tr[ií]nsito|en tránsito|en camino|en ruta", s):
            return "in_transit"
        if re.search(r"fall|incid|devuel|devolu|devuelto|anul|cancel|no entreg", s):
            return "failure"
        if re.search(r"grabad|grabaci|impr", s):  # grabado / impreso
            return "confirmed"
    # fallback conservador
    return "in_transit"

# ===== Main =====
def main():
    log("🚀 Iniciando actualización de estados (versión robusta)...")
    orders = get_fulfilled_orders()
    log(f"📦 Pedidos (fulfilled) recuperados: {len(orders)}")

    counters = {"checked": 0, "created": 0, "skipped_dup": 0, "no_tracking": 0, "errors": 0}

    for order in orders:
        fulfillments = order.get("fulfillments", [])
        if not fulfillments:
            continue

        for f in fulfillments:
            order_id = order["id"]
            fulfillment_id = f["id"]
            tracking_number = f.get("tracking_number")

            statuses, last_status, raw_events = get_fulfillment_events(order_id, fulfillment_id)

            # Logging básico del fulfillment
            log(f"--- Pedido {order_id} | fulfillment {fulfillment_id} | tracking={tracking_number} | shopify_events={sorted(list(statuses))}")

            if not tracking_number:
                counters["no_tracking"] += 1
                log(f"⚠️ Pedido {order_id} fulfillment {fulfillment_id} sin número de seguimiento")
                continue

            # Obtener estado CTT (código y texto)
            ctt = get_ctt_status(tracking_number)
            code = ctt.get("code")
            text = ctt.get("text")
            event_date = ctt.get("event_date")
            raw_event = ctt.get("raw_event")
            no_events = ctt.get("no_events", False)

            # Log raw CTT for debugging
            log(f"🔎 CTT raw for {tracking_number}: code={code} | text={text} | event_date={event_date}")

            mapped = map_ctt_to_shopify(code, text)
            log(f"   -> Mapeado a Shopify: '{mapped}'")

            # Si ya existe ese estado, no duplicar
            if mapped in statuses:
                counters["skipped_dup"] += 1
                log(f"ℹ️ Pedido {order_id} ya tiene evento '{mapped}' → salto")
                continue

            # Si el fulfillment ya figura como 'success' en Shopify:
            # - Si CTT mapea a 'delivered' -> crear delivered.
            # - Si CTT mapea a otra cosa -> crear esa cosa (no forzar delivered).
            # - Si CTT no tiene eventos (no_events) -> crear un evento conservador 'confirmed' si no hay eventos en Shopify,
            #   en vez de marcarlo 'delivered' directamente.
            try:
                if f.get("status") == "success":
                    if "delivered" in statuses:
                        log(f"🔒 Pedido {order_id} fulfillment {fulfillment_id} ya tiene 'delivered' → no crear")
                        continue
                    if mapped == "delivered":
                        create_fulfillment_event(order_id, fulfillment_id, "delivered", message=text, event_date=event_date)
                        counters["created"] += 1
                        continue
                    if not no_events:
                        # CTT tiene info y dice otra cosa (in_transit/out_for_delivery/failure/confirmed) -> crear ese estado
                        create_fulfillment_event(order_id, fulfillment_id, mapped, message=text, event_date=event_date)
                        counters["created"] += 1
                        continue
                    # no_events == True && no delivered event -> crear 'confirmed' conservador
                    if len(statuses) == 0:
                        create_fulfillment_event(order_id, fulfillment_id, "confirmed", message="Sin eventos CTT, marcado como confirmed", event_date=None)
                        counters["created"] += 1
                        continue
                    # si ya hay eventos pero ninguno 'delivered' y no hay CTT: no forzamos
                    log(f"⚠️ Pedido {order_id} fulfillment {fulfillment_id} está 'success' pero no hay info CTT y hay eventos previos -> no forzamos 'delivered'")
                    continue
                else:
                    # Normal flow: crear evento mapeado si no existe
                    create_fulfillment_event(order_id, fulfillment_id, mapped, message=text, event_date=event_date)
                    counters["created"] += 1
            except Exception as e:
                counters["errors"] += 1
                log(f"❌ Error procesando order {order_id} fulfillment {fulfillment_id}: {e}")

            counters["checked"] += 1

    log(f"🏁 Fin. resumen: {counters}")

def get_fulfilled_orders(limit=300):
    # colocada abajo para que main() la llame sin redefinir arriba
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
        if "Link" in r.headers and 'rel="next"' in r.headers["Link"]:
            url = r.links["next"]["url"]
            params = None
        else:
            break
    return all_orders[:limit]

if __name__ == "__main__":
    main()
