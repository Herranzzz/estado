#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza el estado de los envíos de CTT con Shopify y crea fulfillment events.

- Consulta CTT (endpoint p_track_redis.php) para obtener el último estado del tracking.
- Mapea el texto de CTT a estados de fulfillment event de Shopify.
- Idempotente:
  - Si un fulfillment ya tiene un event 'delivered', no se toca más.
  - No crea eventos duplicados (mismo status) para el mismo fulfillment.
"""

import os
import typing as t
import requests
from datetime import datetime, timezone
import unicodedata

# =========================
# CONFIG
# =========================

SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "TU-TIENDA.myshopify.com")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-04")

ORDERS_LIMIT = int(os.getenv("ORDERS_LIMIT", "50"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))

# --- CTT ---
# Endpoint real (igual que tu Apps Script):
# https://wct.cttexpress.com/p_track_redis.php?sc={tracking}
DEFAULT_CTT_TRACKING_ENDPOINT = "https://wct.cttexpress.com/p_track_redis.php?sc={tracking}"
CTT_TRACKING_ENDPOINT = os.getenv("CTT_TRACKING_ENDPOINT", DEFAULT_CTT_TRACKING_ENDPOINT).strip()

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

def shopify_get(path: str, params: t.Optional[dict] = None) -> dict:
    r = requests.get(
        shopify_url(path),
        headers=shopify_headers(),
        params=params or {},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()

def shopify_post(path: str, payload: dict) -> dict:
    r = requests.post(
        shopify_url(path),
        headers=shopify_headers(),
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()

def get_fulfilled_orders(limit: int = 50) -> t.List[dict]:
    """
    Pedidos con fulfillment_status 'shipped' (los que suelen tener fulfillments con tracking).
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

def get_fulfillment_events(order_id: int, fulfillment_id: int) -> t.List[dict]:
    data = shopify_get(f"orders/{order_id}/fulfillments/{fulfillment_id}/events.json")
    return data.get("fulfillment_events", []) or []

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
        log(f"ℹ️ {order_id}/{fulfillment_id}: estado CTT ambiguo/no mapeable -> NO se crea evento ({ctt_status_text})")
        return False

    # Idempotencia: no duplicar mismo status
    if has_event_status(order_id, fulfillment_id, mapped):
        log(f"⏭️ SKIP {order_id}/{fulfillment_id}: ya existe evento '{mapped}'")
        return False

    happened_at = None
    if event_date:
        try:
            # event_date suele venir en ISO; si viene con Z, normalizamos
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

    try:
        shopify_post(f"orders/{order_id}/fulfillments/{fulfillment_id}/events.json", payload)
        return True
    except requests.HTTPError as e:
        log(f"❌ Error creando fulfillment event {order_id}/{fulfillment_id} ({mapped}): {e}")
        return False

# =========================
# CTT API
# =========================

def get_ctt_status(tracking_number: str) -> dict:
    """
    Endpoint real (igual que tu Apps Script):
      https://wct.cttexpress.com/p_track_redis.php?sc={tracking}

    Devuelve:
      - status: str (description del último evento)
      - date: str (event_date del último evento) o None
    """
    endpoint = CTT_TRACKING_ENDPOINT.format(tracking=tracking_number)

    headers = {"Accept": "application/json"}
    headers.update(parse_headers_extra(CTT_HEADERS_EXTRA))

    r = requests.get(endpoint, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

    data = r.json()

    # Estructura esperada:
    # data["data"]["shipping_history"]["events"] -> lista de eventos
    events = (
        data.get("data", {})
            .get("shipping_history", {})
            .get("events", [])
    )

    if isinstance(events, list) and events:
        last = events[-1] or {}
        status = str(last.get("description") or "").strip()
        date = last.get("event_date") or None
        return {"status": status or "Estado desconocido", "date": date}

    return {"status": "Sin eventos", "date": None}

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

    # 2) FAILURE (devoluciones/incidencias graves)
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

    # 3) ATTEMPTED DELIVERY (intento fallido)
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
        "out for delivery", "repartidor", "en ruta de entrega", "en ruta",
        "entrega hoy"
    ):
        return "out_for_delivery"

    # 6) CONFIRMED
    # IMPORTANTE: "Pendiente de recepción en CTT Express" -> normaliza a "pendiente de recepcion ..."
    if has_any(
        "pendiente de recepcion",
        "admitido", "admitida",
        "aceptado", "aceite", "aceite pela ctt", "aceite pela rede",
        "registrado", "registado", "recebido", "recebida",
        "entrada en red", "entrada em rede",
        "grabado"
    ):
        return "confirmed"

    # 7) IN TRANSIT
    if has_any(
        "en transito", "en tránsito",
        "em transito", "em trânsito",
        "en curso", "en proceso",
        "clasificado", "classificado",
        "en plataforma", "hub", "en centro", "en almac", "almacen", "armazem",
        "salida de", "salio de", "saida de", "departed",
        "llegada a", "chegada a", "arrived",
        "enviado",
        "cambio direccion y fecha de entrega", "cambio dirección y fecha de entrega"
    ):
        return "in_transit"

    # 8) Ambiguos (NO crear evento)
    if has_any(
        "aguardando", "a aguardar", "preaviso", "pre-aviso",
        "informacion recibida", "info recibi
