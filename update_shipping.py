#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza el estado de los envíos de CTT con Shopify y crea fulfillment events.

Mejoras clave (vs versiones anteriores):
- SHOPIFY_STORE_DOMAIN ya NO tiene valor por defecto (evita usar "TU-TIENDA..." sin querer).
- Evita duplicar eventos: si ya existe un fulfillment_event con el mismo status, no crea otro.
- Sigue siendo idempotente para delivered: si ya existe 'delivered', no crea nada más.
- Llamada a CTT robusta: reintentos, timeout, User-Agent, y manejo de respuestas NO-JSON (HTML/empty).
- Manejo básico de rate limit de Shopify (429) con backoff.
"""

import os
import time
import typing as t
import unicodedata
from datetime import datetime, timezone

import requests


# =========================
# CONFIG / ENV
# =========================

SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")  # ej: dondefue.myshopify.com (SIN https://)
ORDERS_LIMIT = int(os.getenv("ORDERS_LIMIT", "250"))

LOG_FILE = "logs_actualizacion_envios.txt"
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-01")

CTT_TIMEOUT_SEC = int(os.getenv("CTT_TIMEOUT_SEC", "20"))
CTT_RETRIES = int(os.getenv("CTT_RETRIES", "3"))
CTT_RETRY_SLEEP_SEC = float(os.getenv("CTT_RETRY_SLEEP_SEC", "1.5"))

SHOPIFY_TIMEOUT_SEC = int(os.getenv("SHOPIFY_TIMEOUT_SEC", "30"))
SHOPIFY_RETRIES = int(os.getenv("SHOPIFY_RETRIES", "3"))
SHOPIFY_RETRY_SLEEP_SEC = float(os.getenv("SHOPIFY_RETRY_SLEEP_SEC", "1.0"))


# =========================
# HELPERS
# =========================

def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {message}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def normalize_text(s: t.Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def require_env() -> bool:
    if not SHOPIFY_ACCESS_TOKEN:
        log("❌ Falta SHOPIFY_ACCESS_TOKEN en el entorno.")
        return False
    if not SHOPIFY_STORE_DOMAIN:
        log("❌ Falta SHOPIFY_STORE_DOMAIN en el entorno (ej: dondefue.myshopify.com).")
        return False
    return True


# =========================
# SHOPIFY HTTP
# =========================

def shopify_headers() -> dict:
    return {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def shopify_request(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    json: dict | None = None,
) -> requests.Response:
    """
    Request con manejo básico de 429/5xx.
    """
    last_exc: Exception | None = None

    for attempt in range(1, SHOPIFY_RETRIES + 1):
        try:
            resp = requests.request(
                method,
                url,
                headers=shopify_headers(),
                params=params,
                json=json,
                timeout=SHOPIFY_TIMEOUT_SEC,
            )

            # Rate limit
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else (SHOPIFY_RETRY_SLEEP_SEC * attempt)
                log(f"⏳ Shopify 429 rate limit. Reintentando en {sleep_s:.1f}s (intento {attempt}/{SHOPIFY_RETRIES})")
                time.sleep(sleep_s)
                continue

            # Errores transitorios
            if 500 <= resp.status_code <= 599:
                sleep_s = SHOPIFY_RETRY_SLEEP_SEC * attempt
                log(f"⚠️ Shopify {resp.status_code}. Reintentando en {sleep_s:.1f}s (intento {attempt}/{SHOPIFY_RETRIES})")
                time.sleep(sleep_s)
                continue

            return resp

        except Exception as e:
            last_exc = e
            sleep_s = SHOPIFY_RETRY_SLEEP_SEC * attempt
            log(f"⚠️ Excepción Shopify request: {e}. Reintentando en {sleep_s:.1f}s (intento {attempt}/{SHOPIFY_RETRIES})")
            time.sleep(sleep_s)

    raise RuntimeError(f"Shopify request falló tras {SHOPIFY_RETRIES} intentos. Último error: {last_exc}")


def get_fulfilled_orders(limit: int = ORDERS_LIMIT) -> t.List[dict]:
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/orders.json"
    params = {
        "status": "any",
        "fulfillment_status": "fulfilled",
        "limit": limit,
        "fields": "id,name,fulfillments",
    }

    resp = shopify_request("GET", url, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("orders", [])


def get_fulfillment_events(order_id: int, fulfillment_id: int) -> t.List[dict]:
    url = (
        f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/"
        f"{SHOPIFY_API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    )
    resp = shopify_request("GET", url)
    if resp.status_code >= 400:
        log(f"⚠️ No se pudieron obtener events para {order_id}/{fulfillment_id}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    return data.get("fulfillment_events", [])


def existing_event_statuses(order_id: int, fulfillment_id: int) -> t.Set[str]:
    statuses: t.Set[str] = set()
    for ev in get_fulfillment_events(order_id, fulfillment_id):
        st = normalize_text(ev.get("status"))
        if st:
            statuses.add(st)
    return statuses


def has_delivered_event(order_id: int, fulfillment_id: int) -> bool:
    return "delivered" in existing_event_statuses(order_id, fulfillment_id)


def create_fulfillment_event(
    order_id: int,
    fulfillment_id: int,
    ctt_status: str,
    event_date: t.Optional[str] = None,
) -> bool:
    event_status = map_ctt_status_to_shopify_status(ctt_status)
    if not event_status:
        log(
            f"🔁 Saltando evento {order_id}/{fulfillment_id}: "
            f"no se pudo mapear estado CTT '{ctt_status}'"
        )
        return False

    # Evitar duplicados: si ya hay ese status, no crear otro
    statuses = existing_event_statuses(order_id, fulfillment_id)
    if normalize_text(event_status) in statuses:
        log(f"⏭️ SKIP {order_id}/{fulfillment_id}: ya existe fulfillment_event '{event_status}'")
        return False

    url = (
        f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/"
        f"{SHOPIFY_API_VERSION}/orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
    )

    happened_at = event_date if event_date else datetime.now(timezone.utc).isoformat()

    payload = {
        "event": {
            "status": event_status,
            "happened_at": happened_at,
            "message": f"Actualizado desde CTT: {ctt_status}",
        }
    }

    resp = shopify_request("POST", url, json=payload)
    if resp.status_code >= 400:
        log(f"❌ Error creando event {order_id}/{fulfillment_id}: {resp.status_code} {resp.text}")
        return False

    log(f"📨 Creado fulfillment event '{event_status}' para {order_id}/{fulfillment_id} ({ctt_status})")
    return True


# =========================
# CTT
# =========================

def get_ctt_status(tracking_number: str) -> dict:
    """
    Devuelve:
      {"status": "<texto>", "date": "<event_date o None>"}

    Fuente:
      https://wct.cttexpress.com/p_track_redis.php?sc=<tracking>
    """
    if not tracking_number:
        return {"status": "", "date": None}

    api_url = f"https://wct.cttexpress.com/p_track_redis.php?sc={tracking_number}"

    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": "Mozilla/5.0 (compatible; estado-bot/1.0; +github-actions)",
        "Referer": "https://wct.cttexpress.com/",
    }

    last_err: str | None = None

    for attempt in range(1, CTT_RETRIES + 1):
        try:
            resp = requests.get(api_url, headers=headers, timeout=CTT_TIMEOUT_SEC)

            if resp.status_code >= 400:
                last_err = f"ERROR CTT HTTP {resp.status_code}"
                time.sleep(CTT_RETRY_SLEEP_SEC * attempt)
                continue

            # A veces devuelve HTML o vacío (y json() revienta). Validamos primero.
            content_type = (resp.headers.get("Content-Type") or "").lower()
            text = (resp.text or "").strip()

            if not text:
                last_err = "ERROR al llamar a CTT: respuesta vacía"
                time.sleep(CTT_RETRY_SLEEP_SEC * attempt)
                continue

            if "application/json" not in content_type:
                # Intentar igual por si no setean bien el content-type
                # pero si parece HTML, lo tratamos como error
                if text.startswith("<") and "html" in text.lower():
                    last_err = "ERROR al llamar a CTT: respuesta HTML (posible bloqueo/caída)"
                    time.sleep(CTT_RETRY_SLEEP_SEC * attempt)
                    continue

            try:
                data = resp.json()
            except Exception as e:
                # Guardamos un snippet para debug sin llenar logs
                snippet = text[:120].replace("\n", " ")
                last_err = f"ERROR al parsear JSON CTT: {e} | body[:120]={snippet}"
                time.sleep(CTT_RETRY_SLEEP_SEC * attempt)
                continue

            eventos = (
                data.get("data", {})
                .get("shipping_history", {})
                .get("events")
            )

            if not eventos:
                return {"status": "Sin eventos", "date": None}

            ultimo = eventos[-1]
            descripcion = (ultimo.get("description") or "").strip()
            event_date = ultimo.get("event_date")

            return {"status": descripcion, "date": event_date}

        except Exception as e:
            last_err = f"ERROR al llamar a CTT: {e}"
            time.sleep(CTT_RETRY_SLEEP_SEC * attempt)

    return {"status": last_err or "ERROR desconocido llamando a CTT", "date": None}


def map_ctt_status_to_shopify_status(ctt_status: str) -> str:
    """
    Shopify fulfillment event statuses:
    - in_transit
    - out_for_delivery
    - delivered
    - failure
    - ready_for_pickup
    - attempted_delivery
    """
    s = normalize_text(ctt_status)
    if not s:
        return ""

    # Entregado
    if "entreg" in s or "reparto finalizado" in s:
        return "delivered"

    # En reparto (más específico que in_transit)
    if "en reparto" in s or "salida a reparto" in s:
        return "out_for_delivery"

    # Intentos de entrega
    if "ausente" in s or "ausencia" in s or "intento" in s:
        return "attempted_delivery"

    # Incidencias / devoluciones
    if "devuelto" in s or "devolucion" in s or "incidencia" in s:
        return "failure"

    # Listo para recoger (si aplica en tus textos)
    if "listo para recoger" in s or "punto de recogida" in s:
        return "ready_for_pickup"

    # Tránsito (clasificación / ruta / admitido / pendiente recepción)
    if (
        "clasificacion" in s
        or "ruta" in s
        or "reparto" in s
        or "pendiente de recepcion" in s
        or "admitido" in s
        or "aceptado" in s
    ):
        return "in_transit"

    return "in_transit"


# =========================
# MAIN
# =========================

def main() -> None:
    if not require_env():
        return

    orders = get_fulfilled_orders(limit=ORDERS_LIMIT)
    log(f"📦 Procesando {len(orders)} pedidos...")

    for order in orders:
        fulfillments = order.get("fulfillments", [])
        if not fulfillments:
            continue

        order_id = order["id"]

        for fulfillment in fulfillments:
            fulfillment_id = fulfillment["id"]

            # 0) Idempotencia: si ya hay delivered, no tocar este fulfillment
            if has_delivered_event(order_id, fulfillment_id):
                log(f"⏭️ SKIP {order_id}/{fulfillment_id}: ya tiene 'delivered' en Shopify.")
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
                ctt_result = get_ctt_status(tn)
                ctt_status = (ctt_result.get("status") or "").strip()
                ctt_date = ctt_result.get("date")

                if "error" in normalize_text(ctt_status):
                    log(f"⚠️ Error con CTT para {order_id}/{fulfillment_id} ({tn}): {ctt_status}")
                    continue

                if ctt_status in ("Sin eventos", "Estado desconocido"):
                    log(f"ℹ️ {order_id}/{fulfillment_id} ({tn}): {ctt_status}")
                    continue

                success = create_fulfillment_event(
                    order_id,
                    fulfillment_id,
                    ctt_status,
                    event_date=ctt_date,
                )

                if success:
                    mapped_status = map_ctt_status_to_shopify_status(ctt_status)
                    log(f"🚚 Actualizado {order_id}/{fulfillment_id} ({tn}): {ctt_status} -> {mapped_status}")


if __name__ == "__main__":
    main()
