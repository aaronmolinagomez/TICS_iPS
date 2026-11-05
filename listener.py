import time
import json
import os
import socket
import threading
from collections import deque
from datetime import datetime

import pandas as pd
import numpy as np
import joblib
import paho.mqtt.client as mqtt


# ==========================================================
# CONFIGURACIÓN
# ==========================================================

MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

MODELO_PATH = "/app/models/modelo_habitaciones.pkl"
ENCODER_PATH = "/app/models/label_encoder.pkl"
FEATURES_PATH = "/app/models/feature_names.pkl"

prediction_interval = 1.0  # 1 predicción por segundo


# ==========================================================
# ESTADO GLOBAL
# ==========================================================

ultimos_anchors = {}  # anchor_id -> datos

ultimo_mobile = {
    "buffer": deque(maxlen=10)
}

ultimos_lock = threading.Lock()

last_prediction_time = 0

window_master = 0
window_master_lock = threading.Lock()

mqtt_client = None  # <-- NUEVO


# ==========================================================
# INFO INICIAL
# ==========================================================

print("=" * 60)
print("Predictor IPS en VIVO - 1 PREDICCIÓN POR SEGUNDO")
print(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
print(f"Modelo: {MODELO_PATH}")
print("=" * 60)


# ==========================================================
# CARGAR MODELO
# ==========================================================

print("Cargando modelo XGBoost...")

try:

    modelo = joblib.load(MODELO_PATH)
    label_encoder = joblib.load(ENCODER_PATH)
    feature_names = joblib.load(FEATURES_PATH)

    print(f"Modelo cargado. Clases: {label_encoder.classes_}")
    print("Predicciones: 1 por segundo")
    print("=" * 60)

except Exception as e:

    print(f"Error cargando modelo: {e}")
    exit(1)


# ==========================================================
# PREDICCIÓN
# ==========================================================

def predecir_con_lo_que_haya():

    global last_prediction_time
    global mqtt_client

    ahora = time.time()

    if ahora - last_prediction_time < prediction_interval:
        return

    with ultimos_lock:

        if len(ultimos_anchors) < 4:
            return

        if not ultimo_mobile.get("ultimo"):
            return

        ahora_ts = time.time()

        # anchors viejos
        for data in ultimos_anchors.values():
            if ahora_ts - data.get("timestamp", 0) > 5:
                return

        # movil viejo
        if ahora_ts - ultimo_mobile["ultimo"].get("received_at", 0) > 5:
            return

        features_dict = {}

        for anchor in ["A", "B", "C", "D"]:

            data = ultimos_anchors.get(anchor, {})

            features_dict[f"avg_{anchor}"] = data.get("avg", 0.0)
            features_dict[f"median_{anchor}"] = data.get("median", 0.0)
            features_dict[f"delta_{anchor}"] = data.get("delta", 0.0)
            features_dict[f"std_{anchor}"] = data.get("std", 0.0)

        missing = [f for f in feature_names if f not in features_dict]

        if missing:
            return

        X_pred = pd.DataFrame(
            [[features_dict[f] for f in feature_names]],
            columns=feature_names
        )

    try:

        pred_num = modelo.predict(X_pred)[0]
        pred_clase = label_encoder.inverse_transform([pred_num])[0]

        probas = modelo.predict_proba(X_pred)[0]
        confianza = probas[pred_num]

        movil = ultimo_mobile.get("ultimo", {})

        timestamp = datetime.now().strftime("%H:%M:%S")

        print(
            f"[{timestamp}] {pred_clase.upper()} "
            f"(conf: {confianza:.2f}) | "
            f"az:{movil.get('azimuth', 0):.0f}° "
            f"pasos:{movil.get('step', 0)}"
        )

        # ==================================================
        # PUBLICAR PREDICCIÓN POR MQTT
        # ==================================================

        if mqtt_client:

            msg = {
                "zone": pred_clase,
                "confidence": float(confianza),
                "timestamp": time.time()
            }

            mqtt_client.publish(
                "ips/prediction",
                json.dumps(msg)
            )

        last_prediction_time = ahora

    except Exception:
        pass


# ==========================================================
# HILO PREDICCIÓN
# ==========================================================

def prediction_loop():

    while True:

        predecir_con_lo_que_haya()

        time.sleep(0.1)


# ==========================================================
# MAESTRO UDP
# ==========================================================

def udp_master():

    global window_master

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    last_time = time.time()

    while True:

        now = time.time()

        if now - last_time >= 1.0:

            with window_master_lock:

                window_master += 1

                msg = f"SYNC:{window_master}"

            sock.sendto(msg.encode(), ("192.168.1.255", 5000))

            last_time = now

        time.sleep(0.1)


# ==========================================================
# MQTT
# ==========================================================

def on_connect(client, userdata, flags, rc):

    if rc == 0:

        client.subscribe("ips/anchor/#")
        client.subscribe("ips/mobile")


def on_message(client, userdata, msg):

    try:

        topic = msg.topic
        payload = msg.payload.decode()

        data = json.loads(payload)

        # -------------------------
        # ANCHORS
        # -------------------------

        if topic.startswith("ips/anchor/"):

            anchor_id = topic.split("/")[-1]

            with ultimos_lock:

                ultimos_anchors[anchor_id] = {
                    "avg": data.get("avg"),
                    "median": data.get("median"),
                    "delta": data.get("delta"),
                    "std": data.get("std"),
                    "window_id": data.get("window_id"),
                    "timestamp": time.time(),
                }

        # -------------------------
        # MÓVIL
        # -------------------------

        elif topic == "ips/mobile":

            mobile_data = {

                "accel_x": data.get("accel_x"),
                "accel_y": data.get("accel_y"),
                "accel_z": data.get("accel_z"),

                "azimuth": data.get("azimuth"),
                "pitch": data.get("pitch"),
                "roll": data.get("roll"),

                "step": data.get("step"),

                "timestamp": data.get("timestamp"),

                "received_at": time.time(),
            }

            with ultimos_lock:

                ultimo_mobile["buffer"].append(mobile_data)

                ultimo_mobile["ultimo"] = mobile_data

    except Exception as e:

        print(f"Error en on_message: {e}")


# ==========================================================
# MAIN
# ==========================================================

def main():

    global mqtt_client

    udp_thread = threading.Thread(target=udp_master, daemon=True)
    udp_thread.start()

    pred_thread = threading.Thread(target=prediction_loop, daemon=True)
    pred_thread.start()

    mqtt_client = mqtt.Client()

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:

        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

        mqtt_client.loop_forever()

    except KeyboardInterrupt:

        print("\nDetenido")

        mqtt_client.disconnect()


if __name__ == "__main__":

    main()
