import json, duckdb
import paho.mqtt.client as mqtt

DB_PATH  = "data\\sensorflow.db"
DLQ_PATH = "data\\dlq.jsonl"   # local dead letter queue
SEEN_IDS = set()             # idempotency cache — prevents double-writes

def setup(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS anomaly_log (
            reading_id    VARCHAR(36) PRIMARY KEY,
            machine_id    VARCHAR(20),
            sensor_type   VARCHAR(30),
            reading_value DOUBLE,
            status        VARCHAR(10),
            reading_ts    TIMESTAMP,
            captured_at   TIMESTAMP DEFAULT current_timestamp
        )
    """)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        rid  = data["message_id"]
        if rid in SEEN_IDS or data["status"] == "NORMAL":
            return  # skip normal readings and duplicates
        SEEN_IDS.add(rid)
        con = duckdb.connect(DB_PATH)  # open only for this insert, then release
        con.execute(
            "INSERT OR IGNORE INTO anomaly_log VALUES (?,?,?,?,?,?)",
            [rid, data["machine_id"], data["sensor_type"],
             float(data["value"]), data["status"], data["ts"]]
        )
        con.close()  # release write lock immediately so nightly_job can connect
        print(f"  Anomaly captured: {data['machine_id']} {data['sensor_type']} = {data['value']} [{data['status']}]")
    except Exception as e:
        with open(DLQ_PATH, 'a') as f:
            f.write(json.dumps({'error': str(e), 'payload': msg.payload.decode()}) + '\n')
        print(f"  DLQ: {e}")

# Run setup once at startup then release the write lock immediately.
# DuckDB allows only ONE writer at a time — holding the connection open here
# would block nightly_job.py from connecting at 01:00.
_init_con = duckdb.connect(DB_PATH)
setup(_init_con)
_init_con.close()

client = mqtt.Client(client_id="sensorflow-cdc")
client.on_message = on_message
client.connect("localhost", 1883)
client.subscribe("sensorflow/readings")
print("CDC handler running — capturing anomalies to DuckDB. Run simulator in ANOMALY mode.")
client.loop_forever()