# OCR Processor

Small OCR processing service that:
- Reads messages from an Azure Storage input queue.
- Downloads or decodes PDFs from the message payload.
- Runs OCR (Tesseract via PyMuPDF + Pillow) per page.
- Demarcates sub-documents using configured identifiers.
- Sends SubDocumentDetails to a classification queue and (optionally) posts sub-document XML to an API.

## Quick checklist
- [ ] Create a `.env` file (see `.env.example` below) and DO NOT commit it.
- [ ] Install prerequisites (Docker or Python environment).
- [ ] Configure `AZURE_STORAGE_CONNECTION_STRING`, `API_URL`, and queue names.

## Prerequisites
- Docker (recommended for production / Windows local parity)
- Python 3.10+ (for local development)
- Tesseract (installed inside Docker image; for local dev install separately)

## Environment
The app reads configuration from environment variables (a `.env` file is supported).
Important variables:
- `AZURE_STORAGE_CONNECTION_STRING` (required at runtime)
- `INPUT_QUEUE_NAME` (default: `ocrinputqueue1`)
- `CLASSIFICATION_QUEUE_NAME` (default: `ocrresponsequeue1`)
- `ARTIFACTS_DIR` (default: `artifacts`)

Note: API integration is currently disabled. `API_URL` / `API_KEY` are preserved in source history but removed from `.env.example` to avoid confusion. Re-enable the API by restoring `api_client.py` usage and adding the variables back into your `.env`.

Security: Do NOT store real secrets in source control. Keep `.env` out of git.

Tip: Copy `.env.example` to `.env` and fill in values before running the app.

## Run locally (Python)
1. Create and activate a venv (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate
pip install -r requirements.txt
```

2. Create a `.env` file with the required entries (copy from `.env` or add minimal values).
3. Run the processor:

```powershell
python main.py
```

## Run with Docker (recommended)
Build the image and run locally with volumes for logs/artifacts:

```powershell
docker build -t ocr-processor .
docker run -v ${PWD}:/app -v ${PWD}\artifacts:/app/artifacts -v ${PWD}\logs:/app/logs -e AZURE_STORAGE_CONNECTION_STRING="<value>" ocr-processor
```

Or use docker-compose (reads `.env`) from the project `src` folder:

```powershell
docker-compose up --build
```

## Useful scripts
- `test/consolidate.py`: merges `.txt` files inside each artifacts subfolder into a `consolidated_<ts>.txt` inside that subfolder. Run:

```powershell
python test/consolidate.py
```

- `test/send_payload.py` and other helper scripts live under `test/` for manual queue testing.

## Notes on recent small changes
- `ocr_processor.py`: removed an internal `logging.basicConfig` call so the application `main.py` controls logging globally.
- `config.py`: import-time RuntimeError for a missing AZURE connection string was removed so tooling/tests can import the module; the runtime check should still occur at application startup (and `main.py` already logs configuration). If you prefer strict fail-fast on import, move the check back.
- `api_client.py`: API POST now uses a small retry/backoff (3 attempts) to reduce transient network failure impact.

## Recommendations / Next steps
- Pin package versions in `requirements.txt` for reproducible builds.
- Add a `.env.example` without secrets and include `.env` in `.gitignore`.
- Consider adding unit tests for the demarcation logic in `ocr_processor.py` (pure function `demarcate_document`).
- Consider a message-key normalization step when parsing queue messages to avoid casing mismatches.

## Troubleshooting
- If you see import-time failures referencing missing env vars, ensure you run from `main.py` and set `AZURE_STORAGE_CONNECTION_STRING` in the environment or `.env`.
- Logs are written to `logs/` and artifacts to `artifacts/` by default.

## Contact / License
This repo contains internal tooling; update README to include author/license as needed.
