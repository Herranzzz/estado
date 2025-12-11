#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sincroniza el estado de los envíos de CTT con Shopify y crea fulfillment events.
Incluye protección para NO volver a marcar como entregado un tracking ya entregado.
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

# Límite de pedidos a procesar por ejecución
ORDERS_LIMIT = int(os.getenv("ORDERS_LIMIT", "250"))

LOG_FILE = "logs_actualizacion_envios.txt"
DELIVERED_REGISTRY_FILE = "envios_ya_entregados.txt"  # ← aquí guardamos trackings ya entregados

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
# REGISTRO DE ENTREGADOS
# =========================

def is_already_marked_delivered(tracking_number: str) -> bool:
    """
    Devuelve True si este tracking ya se marcó como entregado en una ejecución anterior.
    """
    if not tracking_number:
        return False

    if not os.path.exists(DELIVERED_REGISTRY_FILE):
        return False

    try:
        with open(DELIVERED_REGISTRY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip() == tracking_number:
                    return True
    except Exception:
        # Si hay algún problema leyendo el fichero, actuamos como si no estuviera
        return False

    return False


def register_delivered(tracking_number: str) -> None:
    """
    Registra el tracking como entregado para que no se vuelva a actualizar.
    """
    if not tracking_number:
        return

    try:
        with open(DELIVERED_REGISTRY_FILE, "a", encoding="utf-8") as f:
            f.write(tracking_number + "\n")
    except Exception:
        # Si falla el guardado, al menos no rompemos el script
        pass


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
        # Confía en que viene en un formato ISO razonable o lo usas tal cual
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
# CTT HELPERS (ADÁPTALO A TU API REAL)
# =========================

def get_ctt_status(tracking_number: str) -> dict:
    """
    Devuelve un dict con:
      {
        "status": "Texto devuelto por CTT",
        "date": "2025-01-01T12:34:56Z" (opcional)
      }

    IMPORTANTE:
    - Sustituye esta función por tu implementación real de CTT.
    - Aquí hay solo un esqueleto para que veas la estructura.
    """
    # TODO: Implementar llamada real a la API de CTT o reutilizar la tuya.
    # Mientras tanto, devolvemos un error controlado:
    return {
        "status": "ERROR: get_ctt_status no está implementado en este esqueleto.",
        "date": None,
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
                # 0) Si ya está registrado como entregado, no hacemos NADA
                if is_already_marked_delivered(tn):
                    log(
                        f"⏭️ SKIP {order_id}/{fulfillment_id} ({tn}): "
                        f"ya estaba registrado como entregado. No se crea nuevo evento."
                    )
                    continue

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

                # 2) Si se ha creado y el estado es de entrega, registramos el tracking
                if success:
                    mapped_status = map_ctt_status_to_shopify_status(ctt_status)
                    is_delivered = mapped_status == "delivered"

                    if is_delivered:
                        register_delivered(tn)
                        log(
                            f"✅ Marcado como entregado y registrado: "
                            f"{order_id}/{fulfillment_id} ({tn})"
                        )
                    else:
                        log(
                            f"🚚 Actualizado estado {order_id}/{fulfillment_id} "
                            f"({tn}): {ctt_status}"
                        )


if __name__ == "__main__":
    main()
