import os
import json
import logging
import sys
from datetime import datetime, timezone

# Load .env file if it exists
def load_env_file():
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key not in os.environ:  # Don't override existing env vars
                        os.environ[key] = value

# Load .env before importing anything else
load_env_file()

# Try to import Azure SDK; if missing, show a helpful message and exit with error.
try:
    from azure.storage.queue import QueueClient
except Exception as e:
    print("Missing dependency: azure-storage-queue is not installed.")
    print()
    print("To install locally, activate your venv (if any) and run:")
    print("  pip install azure-storage-queue azure-storage-blob")
    print()
    print("Or use the container which already has dependencies installed:")
    print("  docker exec -it iperformtestocr-ocr-processor-1 python /app/tests/fetch_messages.py ocrinputqueue1")
    sys.exit(1)

LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'artifacts', 'tests')
os.makedirs(LOG_DIR, exist_ok=True)

def fetch(queue_name: str, max_messages: int = 5, delete_after: bool = False):
    conn = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not conn:
        raise RuntimeError('AZURE_STORAGE_CONNECTION_STRING not set')

    qc = QueueClient.from_connection_string(conn, queue_name)
    messages = qc.receive_messages(messages_per_page=max_messages, visibility_timeout=60)
    msgs = list(messages)

    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
    out_path = os.path.join(LOG_DIR, f'fetch_{queue_name}_{timestamp}.json')
    payloads = []
    for m in msgs:
        try:
            payloads.append({'id': getattr(m, 'id', None), 'content': m.content, 'pop_receipt': getattr(m, 'pop_receipt', None)})
        except Exception as e:
            payloads.append({'error': str(e)})

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump({'count': len(payloads), 'messages': payloads}, f, indent=2)

    print(f'Wrote {out_path} (count={len(payloads)})')

    # Also print messages to stdout for immediate visibility
    for i, p in enumerate(payloads):
        print('---')
        print(f'Message {i}:')
        print(p.get('content'))

    # Optionally delete messages after fetching
    if delete_after and msgs:
        for m in msgs:
            try:
                qc.delete_message(m.id, m.pop_receipt)
            except Exception as e:
                print(f'Failed to delete message {getattr(m, "id", None)}: {e}')

if __name__ == '__main__':
    import sys
    q = os.getenv('CLASSIFICATION_QUEUE_NAME', 'ocrresponsequeue1')
    if len(sys.argv) > 1:
        q = sys.argv[1]
    fetch(q)
