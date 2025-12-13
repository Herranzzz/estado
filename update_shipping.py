#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza el estado de los envíos de CTT con Shopify y crea fulfillment events.

- Consulta CTT para obtener el último estado del tracking (endpoint p_track_redis por defecto).
- Mapea el texto de CTT a estados de fulfillment event de Shopify.
- Solo hace SKIP si el fulfillment ya tiene un event 'delivered' en Shopify.
- Evita crear eventos duplicados (mismo status) para el mismo fulfillment.
- Incluye reintentos/backoff para 429 Too Many Requests de Shopify.
"""

import os
import time
import json
import typing as t
import unicodedata
from datetime import datetime, timezone

import requests

# =========================
# CONFIG
# =========================

SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "dondefue.myshopify.com").strip()
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-04").strip()

ORDERS_LIMIT = int(os.getenv("ORDERS_LIMIT", "250"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))

# --- CTT ---
# Si no pasas env var, usamos el endpoint que ya estabas usando en Apps Script.
DEFAULT_CTT_ENDPOINT = "https://wct.cttexpress.com/p_track_redis.php?sc={tracking}"
CTT_TRACKING_ENDPOINT = os.getenv("CTT_TRACKING_ENDPOINT", "").strip() or DEFAULT_CTT_ENDPOINT

# =========================
# HELPERS
# =========================

def log(msg: str) -> None:
    print(msg, flush=True)

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = " ".join(s.split())
    return s

def safe_json(resp: requests.Response) -> t.Any:
    # Shopify/CTT a veces devuelven HTML o textos raros en errores
    try:
        return resp.json()
    except Exception:
        try:
            return json.loads(resp.text)
        except Exception:
            return None

def require_env() -> bool:
    missing = []
    if not SHOPIFY_ACCESS_TOKEN:
        missing.append("SHOPIFY_ACCESS_TOKEN")
    if not SHOPIFY_STORE_DOMAIN or "myshopify.com" not in SHOPIFY_STORE_DOMAIN:
        missing.append("SHOPIFY_STORE_DOMAIN (ej: dondefue.myshopify.com)")

    if missing:
        log("❌ Faltan variables de entorno:")
        for m in missing:
            log(f"   - {m}")
        return False
    return True

# =========================
# SHOPIFY API (con reintentos)
# =========================

def shopify_headers() -> dict:
    return {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def shopify_url(path: str) -> str:
    path = path.lstrip("/")
    return f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/{path}"

def request_with_retry(method: str, url: str, *, headers=None, params=None, json_payload=None, timeout=20.0,
                       max_attempts: int = 8) -> requests.Response:
    """
    Reintenta en 429 y 5xx con backoff exponencial.
    Respeta Retry-After si viene.
    """
    backoff = 1.0
    last_exc = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_payload,
                timeout=timeout,
            )

            # Rate limit / server errors
            if resp.status_code == 429 or (500 <= resp.status_code <= 599):
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_s = float(retry_after)
                    except Exception:
                        wait_s = backoff
                else:
                    wait_s = backoff

                log(f"⚠️ Error Shopify request: {resp.status_code} {resp.reason} para {url}. "
                    f"Reintentando en {wait_s:.1f}s (intento {attempt}/{max_attempts})")
                time.sleep(wait_s)
                backoff = min(backoff * 2, 30.0)
                continue

            # Otros errores: no reintentar salvo que quieras
            resp.raise_for_status()
            return resp

        except requests.RequestException as e:
            last_exc = e
            log(f"⚠️ Excepción Shopify request: {e}. Reintentando en {backoff:.1f}s (intento {attempt}/{max_attempts})")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    # Si llegamos aquí, falló
    raise last_exc if last_exc else RuntimeError("Shopify request failed")

def shopify_get(path: str, params: t.Optional[dict] = None) -> dict:
    url = shopify_url(path)
    resp = request_with_retry(
        "GET", url,
        headers=shopify_headers(),
        params=params or {},
        timeout=REQUEST_TIMEOUT,
    )
    data = safe_json(resp)
    return data if isinstance(data, dict) else {}

def shopify_post(path: str, payload: dict) -> dict:
    url = shopify_url(path)
    resp = request_with_retry(
        "POST", url,
        headers=shopify_headers(),
        json_payload=payload,
        timeout=REQUEST_TIMEOUT,
    )
    data = safe_json(resp)
    return data if isinstance(data, dict) else {}

def get_fulfilled_orders(limit: int = 50) -> t.List[dict]:
    data = shopify_get(
        "orders.json",
        params={
            "status": "any",
            "fulfillment_status": "shipped",
            "limit": limit,
            "order": "created_at desc",
        },
    )
    return data.get("orders", []) or []

# Cache de eventos para no reventar Shopify a GETs
EventsCache = dict[tuple[int, int], list[dict]]

def get_fulfillment_events(order_id: int, fulfillment_id: int, cache: EventsCache) -> t.List[dict]:
    key = (order_id, fulfillment_id)
    if key in cache:
        return cache[key]

    # OJO: sin punto al final. Si ves "events.json." => es bug de string
    path = f"orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    data = shopify_get(path)
    events = data.get("fulfillment_events", []) or []
    cache[key] = events
    return events

def has_delivered_event(order_id: int, fulfillment_id: int, cache: EventsCache) -> bool:
    events = get_fulfillment_events(order_id, fulfillment_id, cache)
    for e in events:
        if (e.get("status") or "").strip() == "delivered":
            return True
    return False

def has_event_status(order_id: int, fulfillment_id: int, status: str, cache: EventsCache) -> bool:
    events = get_fulfillment_events(order_id, fulfillment_id, cache)
    for e in events:
        if (e.get("status") or "").strip() == status:
            return True
    return False

def append_event_to_cache(order_id: int, fulfillment_id: int, status: str, cache: EventsCache) -> None:
    # Para evitar un GET extra tras el POST
    key = (order_id, fulfillment_id)
    cache.setdefault(key, [])
    cache[key].append({"status": status})

def create_fulfillment_event(
    order_id: int,
    fulfillment_id: int,
    ctt_status_text: str,
    cache: EventsCache,
    event_date: t.Optional[str] = None,
) -> bool:
    mapped = map_ctt_status_to_shopify_event(ctt_status_text)
    if not mapped:
        log(f"ℹ️ {order_id}/{fulfillment_id}: estado CTT no mapeable -> NO se crea evento ({ctt_status_text})")
        return False

    # Evitar duplicados del mismo status (esto NO es "skip del pedido", solo evita spam)
    if has_event_status(order_id, fulfillment_id, mapped, cache):
        log(f"⏭️ SKIP {order_id}/{fulfillment_id}: ya existe evento '{mapped}'")
        return False

    happened_at = None
    if event_date:
        try:
            dt = datetime.fromisoformat(str(event_date).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            happened_at = dt.astimezone(timezone.utc).isoformat()
        except Exception:
            happened_at = None

    payload = {
        "fulfillment_event": {
            "status": mapped,
            "message": f"CTT: {ctt_status_text}",
        }
    }
    if happened_at:
        payload["fulfillment_event"]["happened_at"] = happened_at

    path = f"orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    try:
        shopify_post(path, payload)
        append_event_to_cache(order_id, fulfillment_id, mapped, cache)
        return True
    except requests.HTTPError as e:
        log(f"❌ Error creando fulfillment event {order_id}/{fulfillment_id} ({mapped}): {e}")
        return False

# =========================
# CTT API (p_track_redis)
# =========================

def get_ctt_status(tracking_number: str) -> dict:
    """
    Devuelve dict con:
      - status: str (texto del último evento)
      - date: str ISO opcional
    Usando el endpoint clásico p_track_redis.php.
    """
    endpoint = CTT_TRACKING_ENDPOINT.format(tracking=tracking_number)

    r = requests.get(endpoint, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

    data = safe_json(r)
    if not isinstance(data, dict):
        return {"status": "Estado desconocido", "date": None}

    # Estructura típica:
    # data.data.shipping_history.events[-1] => {description, event_date}
    events = (
        data.get("data", {})
            .get("shipping_history", {})
            .get("events", None)
    )

    if isinstance(events, list) and events:
        last = events[-1]
        desc = (last.get("description") or "").strip()
        dt = last.get("event_date") or last.get("date") or None
        return {"status": desc or "Estado desconocido", "date": dt}

    return {"status": "Sin eventos", "date": None}

# =========================
# MAPPING CTT -> SHOPIFY
# =========================

def map_ctt_status_to_shopify_event(ctt_status_text: str) -> t.Optional[str]:
    """
    Estados Shopify permitidos:
      - in_transit
      - confirmed
      - out_for_delivery
      - delivered
      - failure
      - ready_for_pickup
      - attempted_delivery
    """
    s = normalize_text(ctt_status_text)
    if not s:
        return None

    def has_any(*needles: str) -> bool:
        return any(n in s for n in needles)

    # 1) DELIVERED
    if has_any(
        "entregado", "entregue", "entrega efectuada", "delivered",
        "entregado ao destinatario", "entregado al destinatario",
        "entregado en buzon", "entregado en buzón", "buzon"
    ):
        return "delivered"

    # 2) FAILURE
    if has_any(
        "devolucion", "devolucao", "retorno", "retornado",
        "en devolucion", "en devolución", "devuelto", "devolvido",
        "direccion incorrecta", "dirección incorrecta",
        "destinatario desconocido", "desconocido",
        "rechazado", "recusado",
        "perdido", "extraviado", "danado", "dañado", "roubado", "robado",
        "incidencia grave", "no entregable"
    ):
        return "failure"

    # 3) ATTEMPTED DELIVERY
    if has_any(
        "intento", "tentativa",
        "ausente", "nao foi possivel entregar", "não foi possível entregar",
        "no se pudo entregar", "no ha sido posible entregar",
        "cliente ausente", "destinatario ausente", "destinatario no disponible",
        "no atendido", "no localizado",
        "reparto fallido", "fallo en entrega", "entrega fallida"
    ):
        return "attempted_delivery"

    # 4) READY FOR PICKUP
    if has_any(
        "listo para recoger", "listo p/ recoger", "pronto para levantamento",
        "disponible para recogida", "disponivel para recolha",
        "punto de recogida", "ponto de recolha",
        "en tienda", "en oficina", "en delegacion", "en delegación",
        "locker", "parcel shop", "pick up", "pickup"
    ):
        return "ready_for_pickup"

    # 5) OUT FOR DELIVERY
    if has_any(
        "en reparto", "en distribucion", "en distribución",
        "saiu para entrega", "saiu p/ entrega", "em distribuicao", "em distribuição",
        "out for delivery", "repartidor", "en ruta de entrega"
    ):
        return "out_for_delivery"

    # 6) CONFIRMED  ✅ (incluye Pendiente de recepción)
    if has_any(
        "admitido", "admitida",
        "recogido", "recolhido", "recolhida",
        "aceptado", "aceite", "aceite pela ctt",
        "registrado", "registado",
        "recebido", "recebida",
        "entrada en red", "entrada em rede",
        "pendiente de recepcion",  # <- aquí cae “Pendiente de recepción en CTT Express”
    ):
        return "confirmed"

    # 7) IN TRANSIT  (OJO: ya NO incluye pendiente de recepción)
    if has_any(
        "en transito", "en tránsito",
        "em transito", "em trânsito",
        "en curso", "en proceso",
        "clasificado", "classificado",
        "en plataforma", "hub", "en centro", "en almac", "almacen", "armazem",
        "salida de", "salio de", "saida de", "departed",
        "llegada a", "chegada a", "arrived"
    ):
        return "in_transit"

    return None

# =========================
# MAIN
# =========================

def main() -> None:
    if not require_env():
        return

    cache: EventsCache = {}

    orders = get_fulfilled_orders(limit=ORDERS_LIMIT)
    log(f"📦 Procesando {len(orders)} pedidos...")

    for order in orders:
        fulfillments = order.get("fulfillments", []) or []
        if not fulfillments:
            continue

        order_id = int(order["id"])

        for fulfillment in fulfillments:
            fulfillment_id = int(fulfillment["id"])

            # ✅ SOLO SKIP si ya hay delivered
            try:
                if has_delivered_event(order_id, fulfillment_id, cache):
                    log(f"⏭️ SKIP {order_id}/{fulfillment_id}: ya tiene 'delivered' en Shopify.")
                    continue
            except requests.HTTPError as e:
                # Si Shopify rate-limitea incluso aquí, no queremos matar todo el job
                log(f"⚠️ No pude leer events para {order_id}/{fulfillment_id} (Shopify): {e}")
                # seguimos igualmente (sin skip) para no “perder” actualizaciones
                pass

            tracking_numbers: t.List[str] = []
            if fulfillment.get("tracking_numbers"):
                tracking_numbers = [tn for tn in fulfillment["tracking_numbers"] if tn]
            elif fulfillment.get("tracking_number"):
                tracking_numbers = [fulfillment["tracking_number"]]

            if not tracking_numbers:
                log(f"⚠️ Pedido {order_id}/{fulfillment_id} sin número de seguimiento")
                continue

            for tn in tracking_numbers:
                try:
                    ctt_result = get_ctt_status(tn)
                except requests.HTTPError as e:
                    log(f"⚠️ Error HTTP CTT para {order_id}/{fulfillment_id} ({tn}): {e}")
                    continue
                except Exception as e:
                    log(f"⚠️ Error CTT para {order_id}/{fulfillment_id} ({tn}): {e}")
                    continue

                ctt_status = (ctt_result.get("status") or "").strip()
                ctt_date = ctt_result.get("date")

                if not ctt_status or ctt_status in ("Sin eventos", "Estado desconocido"):
                    log(f"ℹ️ {order_id}/{fulfillment_id} ({tn}): {ctt_status or 'Sin estado'}")
                    continue

                # Crear evento
                created = create_fulfillment_event(
                    order_id,
                    fulfillment_id,
                    ctt_status,
                    cache=cache,
                    event_date=ctt_date,
                )

                if created:
                    mapped = map_ctt_status_to_shopify_event(ctt_status)
                    log(f"🚚 Actualizado {order_id}/{fulfillment_id} ({tn}): {ctt_status} -> {mapped}")

if __name__ == "__main__":
    main()
