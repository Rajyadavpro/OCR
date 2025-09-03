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
    # Use all JSON files found in the payload folder
    payload_dir = os.path.join(os.path.dirname(__file__), 'payload')
    json_files = [f for f in os.listdir(payload_dir) if f.endswith('.json')]
    if not json_files:
        raise FileNotFoundError('No JSON files found in the payload folder.')
    print(f"Found {len(json_files)} JSON files in the payload folder.")

    conn = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not conn:
        raise RuntimeError('AZURE_STORAGE_CONNECTION_STRING not set in environment')

    # Allow one-off override via CLI: first arg is queue name
    queue_name = os.getenv('INPUT_QUEUE_NAME', 'ocrinputqueue1')
    if len(sys.argv) > 1:
        queue_name = sys.argv[1]

    qc = QueueClient.from_connection_string(conn, queue_name)
    uploaded_count = 0
    for json_file in json_files:
        payload_path = os.path.join(payload_dir, json_file)
        with open(payload_path, 'r', encoding='utf-8') as f:
            payload = f.read()
        qc.send_message(payload)
        uploaded_count += 1
        print(f'Sent {json_file} to queue "{queue_name}"')
    print(f'Total files uploaded: {uploaded_count}')


if __name__ == '__main__':
    main()
