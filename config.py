import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv() # Loads variables from .env file into the environment


class Settings:
    # Azure Storage Configuration
    # NOTE: Do NOT keep secrets in source. Provide the connection string via environment
    # variable AZURE_STORAGE_CONNECTION_STRING (for example in a .env file or the container
    # environment). The application will fail-fast if this is not provided.
    AZURE_STORAGE_CONNECTION_STRING: Optional[str] = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    INPUT_QUEUE_NAME: str = os.getenv("INPUT_QUEUE_NAME", "ocrinputqueue1")
    # MODIFIED: Default value updated to match .env file
    CLASSIFICATION_QUEUE_NAME: str = os.getenv("CLASSIFICATION_QUEUE_NAME", "ocrresponsequeue1")
    # ADDED: Uncommented and activated BLOB_CONTAINER_NAME
    BLOB_CONTAINER_NAME: str = os.getenv("BLOB_CONTAINER_NAME", "pdfdocuments")

    # API Configuration
    # MODIFIED: Default value updated to match .env file
    API_URL: str = os.getenv("API_URL", "https://iperformapi.azurewebsites.net/api/")
    # ADDED: Added API_KEY to be read from the environment
    API_KEY: Optional[str] = os.getenv("API_KEY")
    INSERT_OCR_DOCUMENT_ENDPOINT: str = "Document/InsertOcrDocument"

    # Application Settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", 10))
    TESSERACT_TIMEOUT: int = int(os.getenv("TESSERACT_TIMEOUT", 120)) # Timeout for a single page OCR process
    # Local folder to store artifacts (received messages, downloaded files, outgoing messages)
    ARTIFACTS_DIR: str = os.getenv("ARTIFACTS_DIR", "artifacts")
    # Skip API call and send directly to classification queue (useful when API is down)
    SKIP_API_CALL: bool = os.getenv("SKIP_API_CALL", "false").lower() == "true"

settings = Settings()
# Fail fast if connection string isn't provided. This makes missing secret issues
# obvious at startup instead of silently using a fallback value embedded in source.
if not settings.AZURE_STORAGE_CONNECTION_STRING:
    raise RuntimeError(
        "AZURE_STORAGE_CONNECTION_STRING environment variable is required. "
        "Set it in your environment or .env file and remove any hard-coded secrets from source."
    )