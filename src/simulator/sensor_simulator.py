import json, uuid, time, random
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# ── Machine and sensor definitions ──────────────────────────
MACHINES = [
    {"id": "MACHINE_01", "type": "CNC Mill",         "plant": "Plant-A"},
    {"id": "MACHINE_02", "type": "Conveyor Belt",    "plant": "Plant-A"},
    {"id": "MACHINE_03", "type": "Air Compressor",   "plant": "Plant-B"},
    {"id": "MACHINE_04", "type": "Industrial Oven",  "plant": "Plant-B"},
    {"id": "MACHINE_05", "type": "Hydraulic Press",  "plant": "Plant-C"},
]

SENSORS = {
    "temperature": {"min": 60,  "max": 90,  "anomaly": 90,  "unit": "celsius"},
    "vibration":   {"min": 2,   "max": 15,  "anomaly": 15,  "unit": "mm/s"},
    "pressure":    {"min": 100, "max": 160, "anomaly": 160, "unit": "PSI"},
    "power":       {"min": 30,  "max": 60,  "anomaly": 60,  "unit": "kW"},
}

# ── Simulator mode: change this to test different scenarios ─
MODE = "FAILURE"      # NORMAL | ANOMALY | FAILURE
SILENT_MACHINE = "MACHINE_03"  # goes silent in FAILURE mode

def get_value(sensor_type, mode):
    s = SENSORS[sensor_type]
    if mode == "ANOMALY" and random.random() < 0.25:  # ~5% anomaly rate
        return str(round(s["anomaly"] * random.uniform(1.01, 1.15), 2))
    return str(round(random.uniform(s["min"], s["max"]), 2))
    # NOTE: value is a STRING — mirrors real IoT device behaviour

def get_status(sensor_type, value):
    threshold = SENSORS[sensor_type]["anomaly"]
    v = float(value)
    if sensor_type in ["temperature", "pressure"] and v > threshold: return "HIGH"
    if sensor_type in ["vibration", "power"]       and v > threshold: return "WARN"
    return "NORMAL"

def get_shift():
    h = datetime.now().hour
    if 6 <= h < 14:  return "morning"
    if 14 <= h < 22: return "afternoon"
    return "night"

def simulate():
    # ── LOCAL: connects to Mosquitto on localhost ─────────────
    # ── AWS:   change these 2 lines only (see Phase 01 AWS Guide) ──
    client = mqtt.Client(client_id="sensorflow-simulator")
    client.connect("localhost", 1883)
    print(f"Simulator running in {MODE} mode — Ctrl+C to stop")
    try:
        while True:
            for machine in MACHINES:
                if MODE == "FAILURE" and machine["id"] == SILENT_MACHINE:
                    continue  # this machine is offline
                for sensor_type in SENSORS:
                    value  = get_value(sensor_type, MODE)
                    status = get_status(sensor_type, value)
                    msg = {
                        "message_id":   str(uuid.uuid4()),  # unique per reading
                        "machine_id":   machine["id"],
                        "machine_type": machine["type"],
                        "sensor_type":  sensor_type,
                        "value":        value,  # string — intentional
                        "unit":         SENSORS[sensor_type]["unit"],
                        "location":     machine["plant"],
                        "status":       status,
                        "shift":        get_shift(),
                        "ts":           datetime.now(timezone.utc).isoformat(),
                        "firmware_ver": "v2.1.4"
                    }
                    client.publish("sensorflow/readings", json.dumps(msg))
            time.sleep(5)
    except KeyboardInterrupt:
        print("Simulator stopped")
        client.disconnect()

if __name__ == "__main__":
    simulate()