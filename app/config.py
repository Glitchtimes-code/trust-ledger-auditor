import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

NODE_URL = os.getenv("NODE_URL", "http://localhost:14265").rstrip("/")
MESSAGES_API_URL = os.getenv("MESSAGES_API_URL", "http://localhost:5555").rstrip("/")
HORNET_NODE_NAME = os.getenv("HORNET_NODE_NAME", "iota-hornet")
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "data" / "audit.db"))

MQTT_HOST = os.getenv("MQTT_HOST", "")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

TEMP_MIN = float(os.getenv("TEMP_MIN", "-5"))
TEMP_MAX = float(os.getenv("TEMP_MAX", "15"))
MAX_FUTURE_SKEW_S = 60
