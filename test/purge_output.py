import os
import sys
import logging
from azure.storage.queue import QueueClient

# --- Ensure project root is on sys.path ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import settings from src/config.py
from src.config import settings


def purge_queue(conn_str: str, queue_name: str):
    """Delete all messages from the given Azure queue."""
    logging.info(f"🗑️ Starting purge of queue '{queue_name}'...")

    queue_client = QueueClient.from_connection_string(conn_str, queue_name)

    deleted_count = 0
    while True:
        messages = list(queue_client.receive_messages(messages_per_page=32, visibility_timeout=5))
        if not messages:
            break

        for msg in messages:
            try:
                queue_client.delete_message(msg.id, msg.pop_receipt)
                deleted_count += 1
            except Exception as e:
                logging.error(f"❌ Failed to delete message {msg.id}: {e}")

    logging.info(f"✅ Completed purge of queue '{queue_name}'. Deleted {deleted_count} messages.")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    purge_queue(settings.AZURE_STORAGE_CONNECTION_STRING, settings.CLASSIFICATION_QUEUE_NAME)


if __name__ == "__main__":
    main()
