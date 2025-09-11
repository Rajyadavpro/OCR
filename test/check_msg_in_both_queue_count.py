import os
import sys
import logging
from azure.storage.queue import QueueClient

# --- Ensure project root is on sys.path ---
# The repository root is the parent directory of this `test/` folder
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import settings from config.py (project root)
from config import settings


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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    if not settings.AZURE_STORAGE_CONNECTION_STRING:
        logging.error("Missing AZURE_STORAGE_CONNECTION_STRING in environment; cannot query queues.")
        return

    input_count = get_queue_length(
        settings.AZURE_STORAGE_CONNECTION_STRING,
        settings.INPUT_QUEUE_NAME
    )
    output_count = get_queue_length(
        settings.AZURE_STORAGE_CONNECTION_STRING,
        settings.CLASSIFICATION_QUEUE_NAME
    )

    print("\n📊 Azure Queue Status")
    print(f"📥 Input Queue ({settings.INPUT_QUEUE_NAME}): {input_count} messages")
    print(f"📤 Output Queue ({settings.CLASSIFICATION_QUEUE_NAME}): {output_count} messages\n")


if __name__ == "__main__":
    main()
