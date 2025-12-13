#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza el estado de los envíos de CTT con Shopify y crea fulfillment events.

- Consulta CTT para obtener el último estado del tracking (endpoint p_track_redis.php).
- Mapea el texto de CTT a estados de fulfillment event de Shopify.
- Idempotencia EXACTA (como has pedido):
  - SOLO hace SKIP si el fulfillment ya tiene un event 'delivered' en Shopify.
  - NO hace SKIP por otros estados (confirmed/in_transit/out_for_delivery/etc.).
- Protección contra rate limit (429) de Shopify con retries + backoff y throttling.
"""

import os
import time
import math
import typing as t
import requests
from datetime import datetime, timezone
import unicodedata

# =========================
# CONFIG
# =========================

SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "dondefue.myshopify.com")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-04")

ORDERS_LIMIT = int(os.getenv("ORDERS_LIMIT", "50"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))

# --- CTT ---
# Endpoint real (como en tu Apps Script), con placeholder {tracking}
CTT_TRACKING_ENDPOINT = os.getenv(
    "CTT_TRACKING_ENDPOINT",
    "https://wct.cttexpress.com/p_track_redis.php?sc={tracking}"
).strip()

# Reintentos Shopify
SHOPIFY_MAX_RETRIES = int(os.getenv("SHOPIFY_MAX_RETRIES", "8"))
SHOPIFY_BASE_BACKOFF = float(os.getenv("SHOPIFY_BASE_BACKOFF", "1.0"))  # segundos
SHOPIFY_BACKOFF_MAX = float(os.getenv("SHOPIFY_BACKOFF_MAX", "30.0"))   # segundos

# Throttle por call limit: si quedan pocas llamadas, duerme un pelín
SHOPIFY_THROTTLE_MARGIN = int(os.getenv("SHOPIFY_THROTTLE_MARGIN", "5"))
SHOPIFY_THROTTLE_SLEEP = float(os.getenv("SHOPIFY_THROTTLE_SLEEP", "1.0"))

# =========================
# HELPERS
# =========================

def log(msg: str) -> None:
    print(msg, flush=True)

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

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = " ".join(s.split())
    return s

def _maybe_throttle_from_headers(headers: t.Mapping[str, str]) -> None:
    """
    Shopify REST suele devolver: X-Shopify-Shop-Api-Call-Limit: "used/limit"
    Si vamos muy justos, dormimos un poco para evitar 429.
    """
    try:
        v = headers.get("X-Shopify-Shop-Api-Call-Limit") or headers.get("x-shopify-shop-api-call-limit")
        if not v or "/" not in v:
            return
        used_s, limit_s = v.split("/", 1)
        used = int(used_s.strip())
        limit = int(limit_s.strip())
        if (limit - used) <= SHOPIFY_THROTTLE_MARGIN:
            time.sleep(SHOPIFY_THROTTLE_SLEEP)
    except Exception:
        return

# =========================
# SHOPIFY API (con retry 429)
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

def _shopify_request(method: str, path: str, *, params: dict | None = None, json: dict | None = None) -> dict:
    """
    Request con:
      - retry 429/5xx
      - respeto a Retry-After
      - throttle por Call-Limit
    """
    url = shopify_url(path)
    last_err: Exception | None = None

    for attempt in range(1, SHOPIFY_MAX_RETRIES + 1):
        try:
            r = requests.request(
                method=method,
                url=url,
                headers=shopify_headers(),
                params=params or {},
                json=json,
                timeout=REQUEST_TIMEOUT,
            )

            # throttle (aunque sea success)
            _maybe_throttle_from_headers(r.headers)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    wait_s = float(retry_after)
                else:
                    wait_s = min(SHOPIFY_BACKOFF_MAX, SHOPIFY_BASE_BACKOFF * (2 ** (attempt - 1)))
                log(f"⚠️ Shopify 429 (rate limit). Esperando {wait_s:.1f}s (intento {attempt}/{SHOPIFY_MAX_RETRIES})")
                time.sleep(wait_s)
                continue

            if 500 <= r.status_code < 600:
                wait_s = min(SHOPIFY_BACKOFF_MAX, SHOPIFY_BASE_BACKOFF * (2 ** (attempt - 1)))
                log(f"⚠️ Shopify {r.status_code}. Reintentando en {wait_s:.1f}s (intento {attempt}/{SHOPIFY_MAX_RETRIES})")
                time.sleep(wait_s)
                continue

            r.raise_for_status()
            return r.json()

        except requests.RequestException as e:
            last_err = e
            wait_s = min(SHOPIFY_BACKOFF_MAX, SHOPIFY_BASE_BACKOFF * (2 ** (attempt - 1)))
            log(f"⚠️ Error Shopify request: {e}. Reintentando en {wait_s:.1f}s (intento {attempt}/{SHOPIFY_MAX_RETRIES})")
            time.sleep(wait_s)

    raise last_err if last_err else RuntimeError("Fallo Shopify desconocido")

def shopify_get(path: str, params: t.Optional[dict] = None) -> dict:
    return _shopify_request("GET", path, params=params)

def shopify_post(path: str, payload: dict) -> dict:
    return _shopify_request("POST", path, json=payload)

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

def get_fulfillment_events(order_id: int, fulfillment_id: int) -> t.List[dict]:
    data = shopify_get(f"orders/{order_id}/fulfillments/{fulfillment_id}/events.json")
    return data.get("fulfillment_events", []) or []

def has_delivered_event(order_id: int, fulfillment_id: int) -> bool:
    """
    SOLO esto define el SKIP: si ya hay delivered, no se toca nunca más.
    """
    events = get_fulfillment_events(order_id, fulfillment_id)
    for e in events:
        if (e.get("status") or "").strip() == "delivered":
            return True
    return False

def create_fulfillment_event(
    order_id: int,
    fulfillment_id: int,
    ctt_status_text: str,
    event_date: t.Optional[str] = None,
) -> bool:
    """
    Crea un fulfillment event en Shopify según mapping del texto CTT.
    IMPORTANTE: NO hace SKIP por estados repetidos (solo se hace SKIP por delivered en main()).
    """
    mapped = map_ctt_status_to_shopify_event(ctt_status_text)
    if not mapped:
        log(f"ℹ️ {order_id}/{fulfillment_id}: estado CTT ambiguo/no mapeable -> NO se crea evento ({ctt_status_text})")
        return False

    happened_at = None
    if event_date:
        try:
            dt = datetime.fromisoformat(event_date.replace("Z", "+00:00"))
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

    try:
        shopify_post(f"orders/{order_id}/fulfillments/{fulfillment_id}/events.json", payload)
        return True
    except Exception as e:
        log(f"❌ Error creando fulfillment event {order_id}/{fulfillment_id} ({mapped}): {e}")
        return False

# =========================
# CTT API (p_track_redis.php)
# =========================

def get_ctt_status(tracking_number: str) -> dict:
    """
    Llama al endpoint tipo:
      https://wct.cttexpress.com/p_track_redis.php?sc=TRACKING

    Y extrae:
      data.data.shipping_history.events[-1].description
      data.data.shipping_history.events[-1].event_date
    """
    endpoint = CTT_TRACKING_ENDPOINT.format(tracking=tracking_number)

    r = requests.get(endpoint, headers={"Accept": "application/json"}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    events = None
    try:
        events = data.get("data", {}).get("shipping_history", {}).get("events")
    except Exception:
        events = None

    if isinstance(events, list) and events:
        last = events[-1]
        desc = (last.get("description") or "").strip()
        date = last.get("event_date") or last.get("date") or None
        return {"status": desc or "Estado desconocido", "date": date}

    return {"status": "Sin eventos", "date": None}

# =========================
# MAPPING CTT -> SHOPIFY
# =========================

def map_ctt_status_to_shopify_event(ctt_status_text: str) -> t.Optional[str]:
    """
    Mapea el texto de estado de CTT a los estados de fulfillment event de Shopify:
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
        "reparto fallido", "fallo en entrega", "entrega fallida",
        "incidencia en el reparto", "incidencia reparto"
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
        "out for delivery", "repartidor", "en ruta de entrega", "en ruta"
    ):
        return "out_for_delivery"

    # 6) CONFIRMED (aquí metemos Pendiente de recepción)
    if has_any(
        "admitido", "admitida",
        "recogido", "recolhido", "recolhida",
        "aceptado", "aceite", "aceite pela ctt", "aceite pela rede",
        "registrado", "registado",
        "recebido", "recebida",
        "entrada en red", "entrada em rede",
        "pendiente de recepcion",                  # ✅ clave
        "pendiente de recepcion en ctt express"    # ✅ literal
    ):
        return "confirmed"

    # 7) IN TRANSIT
    if has_any(
        "en transito", "en tránsito",
        "em transito", "em trânsito",
        "clasificado", "classificado",
        "en plataforma", "hub", "en centro", "en almac", "almacen", "armazem",
        "salida de", "salio de", "saida de", "departed",
        "llegada a", "chegada a", "arrived"
    ):
        return "in_transit"

    # 8) Ambiguos (si quieres, los mapeamos luego)
    if has_any(
        "informacion recibida", "info recibida",
        "etiqueta creada", "label created",
        "preaviso", "pre-aviso"
    ):
        return None

    return None

# =========================
# MAIN
# =========================

def main() -> None:
    if not require_env():
        return

    orders = get_fulfilled_orders(limit=ORDERS_LIMIT)
    log(f"📦 Procesando {len(orders)} pedidos...")

    for order in orders:
        fulfillments = order.get("fulfillments", []) or []
        if not fulfillments:
            continue

        order_id = int(order["id"])

        for fulfillment in fulfillments:
            fulfillment_id = int(fulfillment["id"])

            # ✅ ÚNICO SKIP QUE QUIERES:
            try:
                if has_delivered_event(order_id, fulfillment_id):
                    log(f"⏭️ SKIP {order_id}/{fulfillment_id}: ya tiene 'delivered' en Shopify.")
                    continue
            except Exception as e:
                # Si Shopify rate-limitea incluso tras retries, no crasheamos todo:
                log(f"⚠️ No pude comprobar delivered para {order_id}/{fulfillment_id}: {e}")
                # seguimos procesando el siguiente fulfillment para no matar el run
                continue

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

                # Si el texto trae "error" literal, lo tratamos como fallo de consulta
                if "error" in normalize_text(ctt_status):
                    log(f"⚠️ Error con CTT para {order_id}/{fulfillment_id} ({tn}): {ctt_status}")
                    continue

                success = create_fulfillment_event(
                    order_id,
                    fulfillment_id,
                    ctt_status,
                    event_date=ctt_date,
                )

                if success:
                    mapped_status = map_ctt_status_to_shopify_event(ctt_status)
                    log(f"🚚 Actualizado {order_id}/{fulfillment_id} ({tn}): {ctt_status} -> {mapped_status}")

if __name__ == "__main__":
    main()
