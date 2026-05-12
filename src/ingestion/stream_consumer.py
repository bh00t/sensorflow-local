import json, time, boto3
from datetime import datetime
import paho.mqtt.client as mqtt

BUFFER_SECONDS = 60  # matches Firehose 60-second buffer
BUCKET         = "sensorflow-local"
buffer         = []
last_flush     = time.time()

# MinIO as local S3 — remove endpoint_url for real AWS
s3 = boto3.client(
    "s3",
    endpoint_url          = "http://localhost:9000",
    aws_access_key_id     = "sensorflow",
    aws_secret_access_key = "sensorflow123",
    region_name           = "ap-south-1"
)

# Writes buffer to MinIO. On failure saves to local fallback — never drops data.
def flush_buffer():
    global buffer
    if not buffer: return
    now = datetime.utcnow()
    key = (f"raw/year={now.year}/month={now.month:02d}/"
           f"day={now.day:02d}/sensorflow-{now.strftime('%H-%M-%S')}.json")
    body = "\n".join(json.dumps(m) for m in buffer)
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode())
        print(f"  Flushed {len(buffer)} records -> {key}")
        buffer = []   # only clear on success
    except Exception as e:
        fallback = "data/consumer_failed.jsonl"
        with open(fallback, 'a') as fh:
            for rec in buffer:
                fh.write(json.dumps(rec) + '\n')
        print(f"  WARN: MinIO write failed ({e}) -- {len(buffer)} records saved to {fallback}")

def on_message(client, userdata, msg):
    global last_flush
    buffer.append(json.loads(msg.payload.decode()))
    if time.time() - last_flush >= BUFFER_SECONDS:
        flush_buffer()
        last_flush = time.time()

client = mqtt.Client(client_id="sensorflow-consumer", clean_session=False)
client.on_message = on_message
client.connect("localhost", 1883)
client.subscribe("sensorflow/readings", qos=1)
print("Consumer running — buffering 60s then writing to MinIO")
client.loop_forever()