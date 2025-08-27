import os
import json
import sys

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

try:
    from azure.storage.queue import QueueClient
except Exception:
    print("Missing dependency: azure-storage-queue not installed.")
    print("Install with: pip install azure-storage-queue azure-storage-blob")
    sys.exit(1)


def main():
    payload_path = os.path.join(os.path.dirname(__file__), 'payload.json')
    with open(payload_path, 'r', encoding='utf-8') as f:
        payload = f.read()

    conn = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not conn:
        raise RuntimeError('AZURE_STORAGE_CONNECTION_STRING not set in environment')

    # Allow one-off override via CLI: first arg is queue name
    queue_name = os.getenv('INPUT_QUEUE_NAME', 'ocrinputqueue1')
    if len(sys.argv) > 1:
        queue_name = sys.argv[1]

    qc = QueueClient.from_connection_string(conn, queue_name)
    qc.send_message(payload)
    print(f'Sent payload to queue "{queue_name}"')


if __name__ == '__main__':
    main()
