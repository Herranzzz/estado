#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza el estado de los envíos de CTT con Shopify y crea fulfillment events.

Requisitos importantes:

- Usa la API de CTT (wct.cttexpress.com) para obtener el último evento.
- Mapea el texto de CTT a estados de fulfillment event de Shopify.
- NO vuelve a marcar como entregado un fulfillment que ya tiene un event 'delivered'
  en Shopify (idempotente a nivel de fulfillment).
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
# Debes pasar esto en el workflow:
# SHOPIFY_STORE_DOMAIN: dondefue.myshopify.com
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "TU-TIENDA.myshopify.com")

# Límite de pedidos a procesar por ejecución
ORDERS_LIMIT = int(os.getenv("ORDERS_LIMIT", "250"))

LOG_FILE = "logs_actualizacion_envios.txt"

SHOPIFY_API_VERSION = "2024-01"


# =========================
# HELPERS BÁSICOS
# =========================

def log(message: str) -> None:
    """Escribe en log y por pantalla."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {message}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        # Si falla el log, no queremos romper la ejecución
        pass


def normalize_text(s: t.Optional[str]) -> str:
    """Normaliza texto: minúsculas, sin tildes, sin espacios extra."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    return s


# =========================
# SHOPIFY HELPERS
# =========================

def shopify_headers() -> dict:
    return {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def get_fulfilled_orders(limit: int = ORDERS_LIMIT) -> t.List[dict]:
    """
    Recupera pedidos con fulfillments para actualizar eventos.
    Ajusta filtros según lo que quieras considerar.
    """
    url = (
        f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/"
        f"{SHOPIFY_API_VERSION}/orders.json"
    )
    params = {
        "status": "any",
        "fulfillment_status": "fulfilled",
        "limit": limit,
        "fields": "id,name,fulfillments",
    }

    resp = requests.get(url, headers=shopify_headers(), params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("orders", [])


def has_delivered_event(order_id: int, fulfillment_id: int) -> bool:
    """
    Devuelve True si el fulfillment ya tiene un fulfillment_event con status 'delivered'
    en Shopify. Esto hace que el script sea idempotente para entregados.
    """
    url = (
        f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/"
        f"{SHOPIFY_API_VERSION}/orders/{order_id}/"
        f"fulfillments/{fulfillment_id}/events.json"
    )

    try:
        resp = requests.get(url, headers=shopify_headers(), timeout=30)
        if resp.status_code >= 400:
            log(
                f"⚠️ No se pudieron obtener fulfillment events para "
                f"{order_id}/{fulfillment_id}: {resp.status_code} {resp.text}"
            )
            return False

        data = resp.json()
        events = data.get("fulfillment_events", [])

        for ev in events:
            if normalize_text(ev.get("status")) == "delivered":
                return True

        return False

    except Exception as e:
        log(
            f"⚠️ Excepción leyendo fulfillment events de "
            f"{order_id}/{fulfillment_id}: {e}"
        )
        # Ante error al leer eventos, preferimos NO bloquear la actualización
        return False


def create_fulfillment_event(
    order_id: int,
    fulfillment_id: int,
    ctt_status: str,
    event_date: t.Optional[str] = None,
) -> bool:
    """
    Crea un fulfillment event en Shopify a partir del estado de CTT.
    Devuelve True si ha ido bien.
    """
    event_status = map_ctt_status_to_shopify_status(ctt_status)

    # Si no hay estado mapeado, no creamos nada
    if not event_status:
        log(
            f"🔁 Saltando creación de evento para order {order_id}/{fulfillment_id}: "
            f"no se pudo mapear estado CTT '{ctt_status}'"
        )
        return False

    url = (
        f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/"
        f"{SHOPIFY_API_VERSION}/orders/{order_id}/"
        f"fulfillments/{fulfillment_id}/events.json"
    )

    # happened_at en ISO
    if event_date:
        # Lo usamos tal cual; CTT ya da una fecha parseable por JS/ISO
        happened_at = event_date
    else:
        happened_at = datetime.now(timezone.utc).isoformat()

    payload = {
        "event": {
            "status": event_status,
            "happened_at": happened_at,
            "message": f"Actualizado desde CTT: {ctt_status}",
        }
    }

    try:
        resp = requests.post(url, headers=shopify_headers(), json=payload, timeout=30)
        if resp.status_code >= 400:
            log(
                f"❌ Error al crear fulfillment event para "
                f"{order_id}/{fulfillment_id}: {resp.status_code} {resp.text}"
            )
            return False

        log(
            f"📨 Creado fulfillment event '{event_status}' para "
            f"{order_id}/{fulfillment_id} ({ctt_status})"
        )
        return True
    except Exception as e:
        log(f"❌ Excepción creando fulfillment event: {e}")
        return False


# =========================
# CTT HELPERS
# =========================

def get_ctt_status(tracking_number: str) -> dict:
    """
    Devuelve un dict con:
      {
        "status": "Texto devuelto por CTT",
        "date":   "fecha del último evento" (cadena tal como viene de CTT, o None)
      }

    Implementado usando la misma API que tu Apps Script:
    https://wct.cttexpress.com/p_track_redis.php?sc=...
    """
    if not tracking_number:
        return {"status": "", "date": None}

    api_url = f"https://wct.cttexpress.com/p_track_redis.php?sc={tracking_number}"

    try:
        resp = requests.get(api_url, timeout=20)
        if resp.status_code >= 400:
            return {
                "status": f"ERROR CTT HTTP {resp.status_code}",
                "date": None,
            }

        data = resp.json()

    except Exception as e:
        return {
            "status": f"ERROR al llamar a CTT: {e}",
            "date": None,
        }

    # Estructura similar a la que usas en Apps Script
    eventos = (
        data.get("data", {})
        .get("shipping_history", {})
        .get("events")
    )

    if not eventos:
        return {"status": "Sin eventos", "date": None}

    ultimo = eventos[-1]

    descripcion = ultimo.get("description") or ""
    event_date = ultimo.get("event_date")  # la usamos tal cual para Shopify

    return {
        "status": descripcion,
        "date": event_date,
    }


def map_ctt_status_to_shopify_status(ctt_status: str) -> str:
    """
    Mapea el texto de estado de CTT a los estados de fulfillment event de Shopify:
    - "in_transit"
    - "out_for_delivery"
    - "delivered"
    - "failure"
    - "ready_for_pickup"
    - "attempted_delivery"
    """
    s = normalize_text(ctt_status)

    if not s:
        return ""

    # Entregado
    if "entreg" in s or "reparto finalizado" in s:
        return "delivered"

    # Intentos de entrega / ausente
    if "ausente" in s or "ausencia" in s or "intento" in s:
        return "attempted_delivery"

    # Devoluciones / incidencias gordas
    if "devuelto" in s or "devolucion" in s or "incidencia" in s:
        return "failure"

    # Reparto / clasificación / en ruta
    if (
        "reparto" in s
        or "en reparto" in s
        or "salida a reparto" in s
        or "clasificacion" in s
        or "ruta" in s
    ):
        return "in_transit"

    # Confirmado / admitido
    if (
        "pendiente de recepcion" in s
        or "admitido" in s
        or "aceptado" in s
    ):
        return "in_transit"

    # Por defecto lo tratamos como en tránsito
    return "in_transit"


# =========================
# MAIN
# =========================

def main() -> None:
    if not SHOPIFY_ACCESS_TOKEN:
        log("❌ Falta SHOPIFY_ACCESS_TOKEN en el entorno.")
        return

    if not SHOPIFY_STORE_DOMAIN:
        log("❌ Falta SHOPIFY_STORE_DOMAIN en el entorno.")
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

            # 0) Si este fulfillment YA tiene un delivered en Shopify, no hacemos nada
            if has_delivered_event(order_id, fulfillment_id):
                log(
                    f"⏭️ SKIP {order_id}/{fulfillment_id}: "
                    f"ya tiene un fulfillment event 'delivered' en Shopify. "
                    f"No se crea ningún evento nuevo para este fulfillment."
                )
                continue

            tracking_numbers: t.List[str] = []
            # Shopify puede tener tracking_numbers (lista) o tracking_number (string)
            if fulfillment.get("tracking_numbers"):
                tracking_numbers = [
                    tn for tn in fulfillment["tracking_numbers"] if tn
                ]
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
                    log(
                        f"⚠️ Error con CTT para {order_id}/{fulfillment_id} "
                        f"({tn}): {ctt_status}"
                    )
                    continue

                if ctt_status in ("Sin eventos", "Estado desconocido"):
                    log(
                        f"ℹ️ Pedido {order_id}/{fulfillment_id} ({tn}): "
                        f"{ctt_status}"
                    )
                    continue

                # 1) Creamos el fulfillment event correspondiente
                success = create_fulfillment_event(
                    order_id,
                    fulfillment_id,
                    ctt_status,
                    event_date=ctt_date,
                )

                if success:
                    mapped_status = map_ctt_status_to_shopify_status(ctt_status)

                    if mapped_status == "delivered":
                        log(
                            f"✅ Marcado como entregado en Shopify: "
                            f"{order_id}/{fulfillment_id} ({tn})"
                        )
                    else:
                        log(
                            f"🚚 Actualizado estado {order_id}/{fulfillment_id} "
                            f"({tn}): {ctt_status} -> {mapped_status}"
                        )


if __name__ == "__main__":
    main()
