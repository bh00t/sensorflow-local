import json, logging, duckdb
import paho.mqtt.client as mqtt
from pathlib import Path

DB_PATH  = "data\\sensorflow.db"
DLQ_PATH = "data\\dlq.jsonl"
SEEN_IDS = set()   # idempotency -- same message_id twice is a no-op

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "cdc_handler.log"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger("cdc_handler")

def setup(con):
    con.execute('''
        CREATE TABLE IF NOT EXISTS anomaly_log (
            reading_id    VARCHAR(36) PRIMARY KEY,
            machine_id    VARCHAR(20),
            sensor_type   VARCHAR(30),
            reading_value DOUBLE,
            status        VARCHAR(10),
            reading_ts    TIMESTAMP,
            captured_at   TIMESTAMP DEFAULT current_timestamp
        )
    ''')

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        rid  = data["message_id"]
        if rid in SEEN_IDS or data["status"] == "NORMAL":
            logger.debug(f"Skipped: {rid} (duplicate or NORMAL)")
            return
        SEEN_IDS.add(rid)
        # Open write connection only for this INSERT -- lock held milliseconds
        con = duckdb.connect(DB_PATH)
        con.execute(
            "INSERT OR IGNORE INTO anomaly_log VALUES (?,?,?,?,?,?)",
            [rid, data["machine_id"], data["sensor_type"],
             float(data["value"]), data["status"], data["ts"]]
        )
        con.close()   # release write lock immediately
        logger.info(
            f"Anomaly captured: {data['machine_id']} "
            f"{data['sensor_type']} = {data['value']} [{data['status']}]"
        )
    except Exception as e:
        with open(DLQ_PATH, 'a') as f:
            f.write(json.dumps({
                'error': str(e),
                'payload': msg.payload.decode()
            }) + '\n')
        logger.warning(f"DLQ write -- insert failed: {e}", exc_info=True)

# Setup: open write connection once, create table, close immediately.
con_setup = duckdb.connect(DB_PATH)
setup(con_setup)
con_setup.close()

client = mqtt.Client(client_id="sensorflow-cdc")
client.on_message = on_message
client.connect("localhost", 1883)
client.subscribe("sensorflow/readings")

logger.info("CDC handler running -- capturing anomalies to DuckDB")
client.loop_forever()