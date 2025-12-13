#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza el estado de los envíos de CTT con Shopify y crea fulfillment events.

- Consulta CTT para obtener el último estado del tracking.
- Mapea el texto de CTT a estados de fulfillment event de Shopify.
- Idempotente:
  - Si un fulfillment ya tiene un event 'delivered', no se toca más.
  - No crea eventos duplicados (mismo status) para el mismo fulfillment.

Mejoras incluidas:
- Retries con backoff exponencial para Shopify (incluye 429 con Retry-After).
- No reintenta 400 (loggea body/errores para diagnosticar).
- Cache de events por fulfillment para reducir llamadas y evitar 429.
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

SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "TU-TIENDA.myshopify.com")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-04")

ORDERS_LIMIT = int(os.getenv("ORDERS_LIMIT", "50"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))

# Shopify retry / rate limit
SHOPIFY_MAX_RETRIES = int(os.getenv("SHOPIFY_MAX_RETRIES", "8"))
SHOPIFY_BACKOFF_BASE = float(os.getenv("SHOPIFY_BACKOFF_BASE", "1.0"))  # 1s, 2s, 4s...
SHOPIFY_BACKOFF_MAX = float(os.getenv("SHOPIFY_BACKOFF_MAX", "30.0"))   # cap
SHOPIFY_MIN_SLEEP_BETWEEN_CALLS = float(os.getenv("SHOPIFY_MIN_SLEEP_BETWEEN_CALLS", "0.0"))

# --- CTT ---
# Define un endpoint que reciba el tracking en {tracking}
# Ejemplo (ficticio): https://wct.cttexpress.com/api/track/{tracking}
CTT_TRACKING_ENDPOINT = os.getenv("CTT_TRACKING_ENDPOINT", "").strip()
CTT_API_KEY = os.getenv("CTT_API_KEY", "").strip()  # si aplica

# Si tu CTT requiere headers/cookies específicos, puedes ampliarlo aquí
CTT_HEADERS_EXTRA = os.getenv("CTT_HEADERS_EXTRA", "").strip()
# Formato esperado: "Header1:Value1|Header2:Value2"

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
    if not CTT_TRACKING_ENDPOINT:
        missing.append("CTT_TRACKING_ENDPOINT")

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

def parse_headers_extra(raw: str) -> dict:
    headers: dict = {}
    if not raw:
        return headers
    parts = raw.split("|")
    for p in parts:
        if ":" not in p:
            continue
        k, v = p.split(":", 1)
        headers[k.strip()] = v.strip()
    return headers

def _safe_json(resp: requests.Response) -> t.Any:
    try:
        return resp.json()
    except Exception:
        return None

def _sleep(seconds: float) -> None:
    if seconds <= 0:
        return
    time.sleep(seconds)

# =========================
# SHOPIFY API
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

def _shopify_request(method: str, path: str, *, params: t.Optional[dict] = None, json_payload: t.Optional[dict] = None) -> dict:
    """
    Request Shopify con retry/backoff:
    - Reintenta: 429, 5xx, timeouts, errores de red
    - NO reintenta: 400/401/403/404 (log del body y levanta)
    """
    url = shopify_url(path)
    last_exc: Exception | None = None

    for attempt in range(1, SHOPIFY_MAX_RETRIES + 1):
        if SHOPIFY_MIN_SLEEP_BETWEEN_CALLS > 0:
            _sleep(SHOPIFY_MIN_SLEEP_BETWEEN_CALLS)

        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=shopify_headers(),
                params=params or None,
                json=json_payload if json_payload is not None else None,
                timeout=REQUEST_TIMEOUT,
            )

            # OK
            if 200 <= resp.status_code < 300:
                return resp.json() if resp.content else {}

            # 429 rate limit
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = None
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except Exception:
                        wait = None

                if wait is None:
                    wait = min(SHOPIFY_BACKOFF_BASE * (2 ** (attempt - 1)), SHOPIFY_BACKOFF_MAX)

                log(f"⚠️ Shopify 429 Too Many Requests. Reintentando en {wait:.1f}s (intento {attempt}/{SHOPIFY_MAX_RETRIES})")
                _sleep(wait)
                continue

            # 5xx transient
            if 500 <= resp.status_code <= 599:
                wait = min(SHOPIFY_BACKOFF_BASE * (2 ** (attempt - 1)), SHOPIFY_BACKOFF_MAX)
                log(f"⚠️ Shopify {resp.status_code} Server Error. Reintentando en {wait:.1f}s (intento {attempt}/{SHOPIFY_MAX_RETRIES})")
                _sleep(wait)
                continue

            # 4xx no-retry (pero mostramos info útil)
            body = _safe_json(resp)
            snippet = body if body is not None else (resp.text[:500] if resp.text else "")
            log(f"❌ Shopify {resp.status_code} {resp.reason} en {url}")
            if snippet:
                try:
                    pretty = json.dumps(snippet, ensure_ascii=False) if isinstance(snippet, (dict, list)) else str(snippet)
                except Exception:
                    pretty = str(snippet)
                log(f"   ↳ Body: {pretty}")
            resp.raise_for_status()

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            wait = min(SHOPIFY_BACKOFF_BASE * (2 ** (attempt - 1)), SHOPIFY_BACKOFF_MAX)
            log(f"⚠️ Excepción Shopify (red/timeout): {e}. Reintentando en {wait:.1f}s (intento {attempt}/{SHOPIFY_MAX_RETRIES})")
            _sleep(wait)
            continue
        except requests.HTTPError as e:
            # Ya loggeado arriba para 4xx no-retry
            raise
        except Exception as e:
            last_exc = e
            wait = min(SHOPIFY_BACKOFF_BASE * (2 ** (attempt - 1)), SHOPIFY_BACKOFF_MAX)
            log(f"⚠️ Excepción Shopify: {e}. Reintentando en {wait:.1f}s (intento {attempt}/{SHOPIFY_MAX_RETRIES})")
            _sleep(wait)
            continue

    if last_exc:
        raise last_exc
    raise RuntimeError("Fallo desconocido en Shopify request")

def shopify_get(path: str, params: t.Optional[dict] = None) -> dict:
    return _shopify_request("GET", path, params=params)

def shopify_post(path: str, payload: dict) -> dict:
    return _shopify_request("POST", path, json_payload=payload)

def get_fulfilled_orders(limit: int = 50) -> t.List[dict]:
    """
    Pedidos con fulfillment_status 'shipped'/'fulfilled' dependen del flujo.
    Aquí usamos 'shipped' (los que tienen fulfillments con tracking normalmente).
    """
    data = shopify_get(
        "orders.json",
        params={
            "status": "any",
            "fulfillment_status": "shipped",
            "limit": limit,
            "order": "created_at desc",
        },
    )
    return data.get("orders", [])

# Cache para reducir llamadas y evitar 429
_events_cache: dict[tuple[int, int], t.List[dict]] = {}

def get_fulfillment_events(order_id: int, fulfillment_id: int, *, force_refresh: bool = False) -> t.List[dict]:
    key = (order_id, fulfillment_id)
    if not force_refresh and key in _events_cache:
        return _events_cache[key]

    data = shopify_get(f"orders/{order_id}/fulfillments/{fulfillment_id}/events.json")
    events = data.get("fulfillment_events", []) or []
    _events_cache[key] = events
    return events

def has_delivered_event(order_id: int, fulfillment_id: int) -> bool:
    events = get_fulfillment_events(order_id, fulfillment_id)
    for e in events:
        if (e.get("status") or "").strip() == "delivered":
            return True
    return False

def has_event_status(order_id: int, fulfillment_id: int, status: str) -> bool:
    events = get_fulfillment_events(order_id, fulfillment_id)
    for e in events:
        if (e.get("status") or "").strip() == status:
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
    Devuelve True si se creó, False si se omitió o falló.
    """
    mapped = map_ctt_status_to_shopify_event(ctt_status_text)
    if not mapped:
        log(f"ℹ️ {order_id}/{fulfillment_id}: estado CTT no mapeable -> NO se crea evento ({ctt_status_text})")
        return False

    # Idempotencia: no duplicar (usa cache)
    if has_event_status(order_id, fulfillment_id, mapped):
        log(f"⏭️ SKIP {order_id}/{fulfillment_id}: ya existe evento '{mapped}'")
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
        # refrescamos cache para que el siguiente chequeo vea el nuevo evento
        get_fulfillment_events(order_id, fulfillment_id, force_refresh=True)
        return True
    except requests.HTTPError as e:
        log(f"❌ Error creando fulfillment event {order_id}/{fulfillment_id} ({mapped}): {e}")
        return False
    except Exception as e:
        log(f"❌ Excepción creando fulfillment event {order_id}/{fulfillment_id} ({mapped}): {e}")
        return False

# =========================
# CTT API
# =========================

def get_ctt_status(tracking_number: str) -> dict:
    """
    Llama a CTT y devuelve dict con:
      - status: str (texto del último evento)
      - date: str ISO opcional
    """
    endpoint = CTT_TRACKING_ENDPOINT.format(tracking=tracking_number)

    headers = {"Accept": "application/json"}
    if CTT_API_KEY:
        # Si tu CTT usa otra cabecera (Authorization/Bearer/etc.), cámbialo aquí.
        headers["X-API-KEY"] = CTT_API_KEY

    headers.update(parse_headers_extra(CTT_HEADERS_EXTRA))

    r = requests.get(endpoint, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

    data = r.json()

    # ---- ADAPTA ESTA PARTE A TU RESPUESTA REAL DE CTT ----

    # Caso 1: { "status": "...", "date": "..." }
    if isinstance(data, dict) and "status" in data:
        return {
            "status": str(data.get("status") or "").strip(),
            "date": (data.get("date") or None),
        }

    # Caso 2: { "events": [ {"description": "...", "date": "..."}, ... ] }
    if isinstance(data, dict) and isinstance(data.get("events"), list) and data["events"]:
        last = data["events"][-1]
        return {
            "status": str(last.get("description") or last.get("status") or "").strip(),
            "date": (last.get("date") or last.get("datetime") or None),
        }

    # Caso 3: lista de eventos directamente
    if isinstance(data, list) and data:
        last = data[-1]
        if isinstance(last, dict):
            return {
                "status": str(last.get("description") or last.get("status") or "").strip(),
                "date": (last.get("date") or last.get("datetime") or None),
            }

    return {"status": "Estado desconocido", "date": None}

# =========================
# MAPPING CTT -> SHOPIFY
# =========================

SHOPIFY_EVENT_STATUSES = {
    "in_transit",
    "confirmed",
    "out_for_delivery",
    "delivered",
    "failure",
    "ready_for_pickup",
    "attempted_delivery",
}

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
        "reparto fallido", "fallo en entrega", "entrega fallida"
    ):
        return "attempted_delivery"

    # 4) READY FOR PICKUP
    if has_any(
        "listo para recoger", "listo p/ recoger", "pronto para levantamento",
        "disponible para recogida", "disponivel para recolha",
        "en punto", "punto de recogida", "ponto de recolha",
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

    # 6) CONFIRMED
    if has_any(
        "admitido", "admitida",
        "aceptado", "aceite", "aceite pela ctt", "aceite pela rede",
        "registrado", "registado", "registration", "recebido", "recebida",
        "entrada en red", "entrada em rede",
        "pendiente de recepcion"  # “Pendiente de recepción en CTT Express”
    ):
        return "confirmed"

    # 7) IN TRANSIT
    if has_any(
        "en transito", "en tránsito",
        "recogido", "recolhido", "recolhida",
        "em transito", "em trânsito",
        "en curso", "en proceso",
        "clasificado", "classificado",
        "en plataforma", "hub", "en centro", "en almac", "almacen", "armazem",
        "salida de", "salio de", "saida de", "departed",
        "llegada a", "chegada a", "arrived"
    ):
        return "in_transit"

    # 8) Ambiguos
    if has_any(
        "aguardando", "a aguardar", "preaviso", "pre-aviso", "informacion recibida",
        "info recibida", "etiqueta creada", "label created"
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

            # 0) Idempotencia: si ya hay delivered, no tocar este fulfillment
            try:
                if has_delivered_event(order_id, fulfillment_id):
                    log(f"⏭️ SKIP {order_id}/{fulfillment_id}: ya tiene 'delivered' en Shopify.")
                    continue
            except requests.HTTPError as e:
                # Si Shopify devuelve 4xx/5xx aquí, log y continúa para no tumbar el job entero
                log(f"⚠️ Error consultando eventos Shopify {order_id}/{fulfillment_id}: {e}")
                continue
            except Exception as e:
                log(f"⚠️ Excepción consultando eventos Shopify {order_id}/{fulfillment_id}: {e}")
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
