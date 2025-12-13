#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza el estado de los envíos de CTT con Shopify y crea fulfillment events.

✔ Solo hace SKIP si el fulfillment YA tiene un evento 'delivered'
✔ El endpoint de CTT va FIJO en el código
✔ No requiere CTT_TRACKING_ENDPOINT como variable de entorno
✔ Evita duplicados de eventos
"""

import os
import typing as t
import time
import requests
from datetime import datetime, timezone
import unicodedata

# =========================
# CONFIG
# =========================

SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "dondefue.myshopify.com")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-04")

ORDERS_LIMIT = int(os.getenv("ORDERS_LIMIT", "250"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))

# --- CTT ---
# 🔒 ENDPOINT FIJO (NO variable de entorno)
CTT_TRACKING_ENDPOINT = "https://wct.cttexpress.com/p_track_redis.php?sc={tracking}"

CTT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0"
}

# =========================
# HELPERS
# =========================

def log(msg: str) -> None:
    print(msg, flush=True)

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.split())

def require_env() -> bool:
    missing = []
    if not SHOPIFY_ACCESS_TOKEN:
        missing.append("SHOPIFY_ACCESS_TOKEN")
    if "myshopify.com" not in SHOPIFY_STORE_DOMAIN:
        missing.append("SHOPIFY_STORE_DOMAIN")

    if missing:
        log("❌ Faltan variables de entorno:")
        for m in missing:
            log(f"   - {m}")
        return False
    return True

# =========================
# SHOPIFY API (con control de errores)
# =========================

def shopify_headers() -> dict:
    return {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def shopify_url(path: str) -> str:
    return f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/{path.lstrip('/')}"

def shopify_get(path: str, params: dict | None = None) -> dict:
    r = requests.get(
        shopify_url(path),
        headers=shopify_headers(),
        params=params or {},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()

def shopify_post(path: str, payload: dict) -> None:
    r = requests.post(
        shopify_url(path),
        headers=shopify_headers(),
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()

# =========================
# SHOPIFY HELPERS
# =========================

def get_fulfilled_orders(limit: int) -> list[dict]:
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

def get_fulfillment_events(order_id: int, fulfillment_id: int) -> list[dict]:
    try:
        data = shopify_get(
            f"orders/{order_id}/fulfillments/{fulfillment_id}/events.json"
        )
        return data.get("fulfillment_events", [])
    except requests.HTTPError as e:
        log(f"⚠️ Error leyendo eventos {order_id}/{fulfillment_id}: {e}")
        return []

def has_delivered_event(order_id: int, fulfillment_id: int) -> bool:
    for e in get_fulfillment_events(order_id, fulfillment_id):
        if (e.get("status") or "").strip() == "delivered":
            return True
    return False

def has_event_status(order_id: int, fulfillment_id: int, status: str) -> bool:
    for e in get_fulfillment_events(order_id, fulfillment_id):
        if (e.get("status") or "").strip() == status:
            return True
    return False

# =========================
# CTT
# =========================

def get_ctt_status(tracking: str) -> dict:
    url = CTT_TRACKING_ENDPOINT.format(tracking=tracking)
    r = requests.get(url, headers=CTT_HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

    data = r.json()

    if isinstance(data, dict) and "estado" in data:
        return {
            "status": data.get("estado", ""),
            "date": data.get("fecha"),
        }

    if isinstance(data, dict) and isinstance(data.get("events"), list) and data["events"]:
        last = data["events"][-1]
        return {
            "status": last.get("description") or last.get("status"),
            "date": last.get("date"),
        }

    return {"status": "Estado desconocido", "date": None}

# =========================
# MAPPING CTT → SHOPIFY
# =========================

def map_ctt_status_to_shopify_event(ctt_status: str) -> str | None:
    s = normalize_text(ctt_status)

    if any(x in s for x in ["entregado", "entrega efectuada", "buzon"]):
        return "delivered"

    if any(x in s for x in ["intento", "ausente", "no se pudo entregar"]):
        return "attempted_delivery"

    if any(x in s for x in ["en reparto", "en ruta"]):
        return "out_for_delivery"

    if any(x in s for x in ["admitido", "registrado", "pendiente de recepcion"]):
        return "confirmed"

    if any(x in s for x in ["en transito", "clasificado", "en centro"]):
        return "in_transit"

    return None

# =========================
# MAIN
# =========================

def main() -> None:
    if not require_env():
        return

    orders = get_fulfilled_orders(ORDERS_LIMIT)
    log(f"📦 Procesando {len(orders)} pedidos...")

    for order in orders:
        order_id = int(order["id"])
        for f in order.get("fulfillments", []):
            fulfillment_id = int(f["id"])

            # 🔴 SOLO skip si YA está delivered
            if has_delivered_event(order_id, fulfillment_id):
                log(f"⏭️ SKIP {order_id}/{fulfillment_id}: ya delivered")
                continue

            tracking_numbers = f.get("tracking_numbers") or []
            if not tracking_numbers and f.get("tracking_number"):
                tracking_numbers = [f["tracking_number"]]

            for tn in tracking_numbers:
                try:
                    ctt = get_ctt_status(tn)
                except Exception as e:
                    log(f"⚠️ Error CTT {order_id}/{fulfillment_id} ({tn}): {e}")
                    continue

                status_text = ctt.get("status", "")
                mapped = map_ctt_status_to_shopify_event(status_text)

                if not mapped:
                    log(f"ℹ️ {order_id}/{fulfillment_id}: no mapeable ({status_text})")
                    continue

                if has_event_status(order_id, fulfillment_id, mapped):
                    continue

                payload = {
                    "fulfillment_event": {
                        "status": mapped,
                        "message": f"CTT: {status_text}",
                    }
                }

                try:
                    shopify_post(
                        f"orders/{order_id}/fulfillments/{fulfillment_id}/events.json",
                        payload,
                    )
                    log(f"🚚 {order_id}/{fulfillment_id}: {status_text} → {mapped}")
                    time.sleep(0.4)
                except Exception as e:
                    log(f"❌ Error creando evento {order_id}/{fulfillment_id}: {e}")

if __name__ == "__main__":
    main()
