from dotenv import load_dotenv
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
USER_EVENTS_TOPIC = os.getenv('USER_EVENTS_TOPIC', 'user-events')
CONTENT_METADATA_TOPIC = os.getenv('CONTENT_METADATA_TOPIC', 'content-metadata')
FEATURE_STORE_TOPIC = os.getenv('FEATURE_STORE_TOPIC', 'feature-store')
WATERMARK_DELAY_SECONDS = int(os.getenv('WATERMARK_DELAY_SECONDS', '30'))
LATE_EVENT_PERCENTAGE = int(os.getenv('LATE_EVENT_PERCENTAGE', '5'))
