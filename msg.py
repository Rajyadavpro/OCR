# msg.py
import os
import sys
import logging
from azure.storage.queue import QueueClient

# --- Ensure project root is on sys.path ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from config import settings  # now it should import correctly

def get_queue_length(conn_str: str, queue_name: str) -> int:
    """Return approximate number of messages in the given Azure queue."""
    try:
        queue_client = QueueClient.from_connection_string(conn_str, queue_name)
        props = queue_client.get_queue_properties()
        return props.approximate_message_count
    except Exception as e:
        logging.error(f"❌ Failed to get message count for queue '{queue_name}': {e}")
        return -1

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    input_count = get_queue_length(settings.AZURE_STORAGE_CONNECTION_STRING, settings.INPUT_QUEUE_NAME)
    output_count = get_queue_length(settings.AZURE_STORAGE_CONNECTION_STRING, settings.CLASSIFICATION_QUEUE_NAME)

    print("📊 Azure Queue Status")
    print(f"   📥 Input Queue    ({settings.INPUT_QUEUE_NAME}): {input_count} messages")
    print(f"   📤 Output Queue   ({settings.CLASSIFICATION_QUEUE_NAME}): {output_count} messages")

if __name__ == "__main__":
    main()
