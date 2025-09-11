import os
import json
import logging
import time
import signal
import threading
import base64
import requests
import sys
from datetime import datetime, timezone

# Setup logging FIRST before any other imports
# Create logs directory if it doesn't exist
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)

# Create log filename with timestamp
log_filename = os.path.join(logs_dir, f"ocr_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Graceful shutdown event used by the main loop and long sleeps
shutdown_event = threading.Event()

from azure_service import AzureQueueService
from ocr_processor import get_text_from_pdf, demarcate_document
from data_models import create_subdocument_xml
from config import settings


def _handle_signal(signum, frame):
    logger.info(f"🔔 SIGNAL: Received signal {signum}, initiating graceful shutdown")
    shutdown_event.set()

# Register signal handlers (works on Linux containers)
signal.signal(signal.SIGINT, _handle_signal)
try:
    signal.signal(signal.SIGTERM, _handle_signal)
except Exception:
    # SIGTERM may not be available on some platforms; ignore if registration fails
    pass

def process_message(message_content: dict, input_queue_service: AzureQueueService, output_queue_service: AzureQueueService) -> bool:
    """Process a single queue message for OCR"""
    
    process_start_time = time.time()
    upload_id = message_content.get("UploadDatasheetid", "UNKNOWN")
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
    # Create a unique subfolder for this message in the artifacts directory
    message_folder = os.path.join(settings.ARTIFACTS_DIR, f"{upload_id}_{timestamp}")
    os.makedirs(message_folder, exist_ok=True)
    
    logger.info("🚀 STARTING MESSAGE PROCESSING")
    logger.info(f"📋 UploadDatasheetid: {upload_id}")
    logger.info(f"⏰ Process start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 📁 ARTIFACTS DIRECTORY SETUP
        try:
            os.makedirs(settings.ARTIFACTS_DIR, exist_ok=True)
            os.makedirs(message_folder, exist_ok=True)
            logger.info(f"✅ SUCCESS: Artifacts directory ready: {settings.ARTIFACTS_DIR}")
            logger.info(f"✅ SUCCESS: Message-specific folder ready: {message_folder}")
        except Exception as e:
            logger.error(f"❌ CRITICAL FAILURE: Could not create artifacts directory: {settings.ARTIFACTS_DIR} or message folder: {message_folder} - {e}")
            return False

        # ✅ SUCCESS: Message received
        logger.info("✅ SUCCESS: Message received successfully")

        # Log the full message content for debugging
        logger.info("=" * 80)
        logger.info("RECEIVED MESSAGE CONTENT:")
        logger.info("=" * 80)
        for key, value in message_content.items():
            if isinstance(value, str) and len(value) > 200:
                logger.info(f"{key}: {value[:200]}... (truncated, length: {len(value)})")
            else:
                logger.info(f"{key}: {value}")
        logger.info("=" * 80)

        # 🔍 VALIDATION: Check for required fields
        logger.info("🔍 VALIDATING: Checking required message fields")

        client_filename = message_content.get("ClientFileName")
        if not client_filename:
            logger.error("❌ FAILURE: Message validation failed - missing 'ClientFileName'")
            logger.error(f"📋 Available fields: {list(message_content.keys())}")
            return False

        logger.info(f"✅ SUCCESS: ClientFileName found: {client_filename}")

        # Validate other required fields
        required_fields = ["UploadDatasheetid", "DocReceivedId", "BatchId"]
        missing_fields = []
        for field in required_fields:
            if field not in message_content or message_content[field] is None:
                missing_fields.append(field)

        if missing_fields:
            logger.warning(f"⚠️ WARNING: Missing optional fields: {missing_fields}")
        else:
            logger.info("✅ SUCCESS: All required fields present")

        logger.info(f"🔄 PROCESSING: Starting file processing for {client_filename}")

        # 📄 PDF CONTENT PROCESSING
        logger.info("📄 PROCESSING: Checking for PDF content in message")
        pdf_content = message_content.get("PdfContent")
        pdf_url = message_content.get("FilePath")
        pdf_path = None # Initialize pdf_path

        if pdf_content:
            logger.info("✅ SUCCESS: PDF content found in message (embedded base64)")
            try:
                logger.info("🔄 PROCESSING: Decoding base64 PDF content")
                pdf_data = base64.b64decode(pdf_content)
                logger.info(f"✅ SUCCESS: PDF content decoded, size: {len(pdf_data)} bytes")
                # Save the original payload JSON in the message folder
                payload_json_path = os.path.join(message_folder, "payload.json")
                with open(payload_json_path, 'w', encoding='utf-8') as f:
                    json.dump(message_content, f, ensure_ascii=False, indent=2)
                # Create a permanent path in the message folder
                pdf_filename = f"{upload_id}_{timestamp}_from_message.pdf"
                pdf_path = os.path.join(message_folder, pdf_filename)
                logger.info(f"💾 PROCESSING: Saving PDF to local artifacts: {pdf_path}")
                with open(pdf_path, 'wb') as f:
                    f.write(pdf_data)
                logger.info(f"✅ SUCCESS: PDF content saved locally.")
            except Exception as e:
                logger.error(f"❌ FAILURE: Failed to decode or save PDF content: {e}")
                return False
        
        elif pdf_url:
            logger.info("✅ SUCCESS: PDF URL found in message (FilePath)")
            logger.info(f"🔗 PDF URL: {pdf_url}")
            try:
                logger.info("🌐 PROCESSING: Starting PDF download from URL")
                download_start_time = time.time()
                session = requests.Session()
                session.headers.update({'User-Agent': 'iPerform-OCR-Processor/1.0'})
                response = session.get(pdf_url, stream=True, timeout=60)
                logger.info(f"📊 HTTP Response: {response.status_code} {response.reason}")
                response.raise_for_status() # Raise an exception for bad status codes
                # Save the original payload JSON in the message folder
                payload_json_path = os.path.join(message_folder, "payload.json")
                with open(payload_json_path, 'w', encoding='utf-8') as f:
                    json.dump(message_content, f, ensure_ascii=False, indent=2)
                # Create a permanent path for the downloaded PDF in the message folder
                pdf_filename = f"{upload_id}_{timestamp}_from_url.pdf"
                pdf_path = os.path.join(message_folder, pdf_filename)
                logger.info(f"💾 PROCESSING: Saving downloaded PDF to: {pdf_path}")
                with open(pdf_path, 'wb') as f:
                    downloaded_bytes = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_bytes += len(chunk)
                total_download_duration = time.time() - download_start_time
                logger.info(f"✅ SUCCESS: PDF downloaded and saved in {total_download_duration:.2f} seconds")
                logger.info(f"📊 Total size: {downloaded_bytes:,} bytes")
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ FAILURE: PDF download failed: {e}")
                return False
            except Exception as e:
                logger.error(f"❌ FAILURE: Unexpected error during PDF download: {e}")
                return False
        else:
            logger.warning(f"⚠️ WARNING: No PDF content or URL found for file: {client_filename}. Cannot proceed.")
            return False

        # 🔍 OCR PROCESSING
        try:
            logger.info("🔍 PROCESSING: Starting OCR extraction")
            if pdf_path and os.path.exists(pdf_path):
                logger.info(f"📄 PROCESSING: Running OCR on file: {pdf_path}")
                ocr_start_time = time.time()
                pdf_pages_text = get_text_from_pdf(pdf_path)
                ocr_duration = time.time() - ocr_start_time
                
                if pdf_pages_text:
                    logger.info(f"✅ SUCCESS: OCR completed in {ocr_duration:.2f} seconds")
                    logger.info(f"📊 OCR results: {len(pdf_pages_text)} pages extracted")
                    # Log OCR statistics
                    total_chars = sum(len(page) for page in pdf_pages_text)
                    total_words = sum(len(page.split()) for page in pdf_pages_text)
                    logger.info(f"📊 Total characters extracted: {total_chars:,}")
                    logger.info(f"📊 Total words extracted: {total_words:,}")
                    # 💾 Save OCR text pages locally in the message folder
                    logger.info(f"💾 PROCESSING: Saving {len(pdf_pages_text)} OCR text pages to message folder {message_folder}")
                    try:
                        for i, page_text in enumerate(pdf_pages_text):
                            page_num = i + 1
                            text_filename = f"{upload_id}_{timestamp}_page_{page_num}.txt"
                            text_path = os.path.join(message_folder, text_filename)
                            with open(text_path, 'w', encoding='utf-8') as f:
                                f.write(page_text)
                        logger.info(f"✅ SUCCESS: All OCR text pages saved to {message_folder}")
                    except Exception as e:
                        logger.warning(f"⚠️ WARNING: Could not save OCR text pages: {e}")

                else:
                    logger.error("❌ FAILURE: OCR process returned empty results")
                    return False
            else:
                logger.error(f"❌ FAILURE: PDF path '{pdf_path}' not found or not created. Cannot start OCR.")
                return False
            
        except Exception as e:
            logger.error(f"❌ FAILURE: OCR processing failed: {e}", exc_info=True)
            return False

        # 📋 DOCUMENT DEMARCATION PROCESSING
        logger.info("📋 PROCESSING: Starting document demarcation")
        identifiers = message_content.get("Identifiers", [])

        if not identifiers:
            logger.warning("⚠️ WARNING: No identifiers found in message")
        else:
            logger.info(f"✅ SUCCESS: Found {len(identifiers)} identifiers for processing")

        # Enrich identifiers with message metadata
        for ident in identifiers:
            ident["DocReceivedId"] = message_content.get("DocReceivedId")
            ident["FirmFile"] = message_content.get("FirmFile")
            ident["UploadDatasheetid"] = message_content.get("UploadDatasheetid")
            ident["SessionId"] = message_content.get("SessionId")

        # Log all input to demarcate_document for traceability
        if pdf_pages_text:
            for i, page in enumerate(pdf_pages_text):
                logger.info(f"🔎 DEMARCATION INPUT: Page {i+1}: {page[:1000]}")
        else:
            logger.info("🔎 DEMARCATION INPUT: NO OCR TEXT")
        logger.info(f"🔎 DEMARCATION INPUT: All Identifiers: {json.dumps(identifiers, ensure_ascii=False)}")

        try:
            sub_document_rows = demarcate_document(pdf_pages_text, identifiers)
            logger.info(f"🔎 DEMARCATION OUTPUT: {json.dumps(sub_document_rows, ensure_ascii=False)}")
            if sub_document_rows:
                logger.info(f"✅ SUCCESS: Document demarcation completed. {len(sub_document_rows)} documents found.")
            else:
                logger.error("❌ FAILURE: Document demarcation returned no results")
                return False

        except Exception as e:
            logger.error(f"❌ FAILURE: Document demarcation failed: {e}", exc_info=True)
            return False

        # 📋 XML PAYLOAD CREATION
        logger.info("📋 PROCESSING: Creating XML payload for API submission")
        try:
            xml_payload = create_subdocument_xml(sub_document_rows)
            if not xml_payload:
                logger.error("❌ FAILURE: XML payload creation returned empty result")
                return False
            logger.info(f"✅ SUCCESS: XML payload created. Length: {len(xml_payload)} chars")
                
        except Exception as e:
            logger.error(f"❌ FAILURE: XML payload creation failed: {e}", exc_info=True)
            return False
        
        # 🌐 API SUBMISSION PROCESSING
        # NOTE: API integration removed per configuration - we skip API submission
        logger.info("🌐 PROCESSING: API submission is skipped (API integration removed). Marking as skipped.")
        is_api_success = True
        api_status = "skipped"

        # 📤 QUEUE MESSAGE PROCESSING
        logger.info("📤 PROCESSING: Preparing classification message for output queue")

        try:
            # Send the full demarcation details instead of just IDs
            classification_message = {
                "SubDocumentDetails": {
                    "SubDocumentRow": sub_document_rows
                },
                "ApiStatus": api_status if 'api_status' in locals() else "skipped"
            }

            # Save the sent message in the same message-specific folder
            sent_json_path = os.path.join(message_folder, f"sent_{settings.CLASSIFICATION_QUEUE_NAME}_{timestamp}.json")
            with open(sent_json_path, 'w', encoding='utf-8') as f:
                json.dump(classification_message, f, ensure_ascii=False, indent=2)

            logger.info(f"📡 PROCESSING: Sending SubDocumentDetails to classification queue: {settings.CLASSIFICATION_QUEUE_NAME}")
            logger.info(f"📤 OUTPUT QUEUE MESSAGE: {json.dumps(classification_message, ensure_ascii=False)[:1000]}")
            output_queue_service.send_message(
                json.dumps(classification_message),
                settings.CLASSIFICATION_QUEUE_NAME
            )
            logger.info(f"✅ SUCCESS: SubDocumentDetails message queued to {settings.CLASSIFICATION_QUEUE_NAME}")

        except Exception as e:
            logger.error(f"❌ FAILURE: Queue message processing failed: {e}", exc_info=True)
            return False

        # 📊 FINAL SUCCESS SUMMARY
        total_duration = time.time() - process_start_time
        logger.info("🎉 SUCCESS: Message processing completed successfully!")
        logger.info(f"⏰ Total processing time: {total_duration:.2f} seconds")
        logger.info(f"📋 UploadDatasheetid: {upload_id}")

        return True

    except Exception as e:
        total_duration = time.time() - process_start_time
        logger.error("💥 CRITICAL FAILURE: Unhandled exception in message processing")
        logger.error(f"❌ Error: {str(e)}")
        logger.error(f"📋 UploadDatasheetid: {upload_id}")
        logger.error(f"⏰ Failed after: {total_duration:.2f} seconds")
        logger.error("📋 Stack trace:", exc_info=True)
        return False

def main():
    """Main queue processing loop"""
    logger.info("🚀 STARTING OCR PROCESSOR APPLICATION")
    logger.info(f"📋 Current working directory: {os.getcwd()}")
    logger.info(f"📋 Python version: {sys.version}")
    
    # 🔧 CONFIGURATION VALIDATION
    logger.info("🔧 VALIDATING: Configuration settings")
    logger.info(f"📋 AZURE_STORAGE_CONNECTION_STRING: {'SET' if settings.AZURE_STORAGE_CONNECTION_STRING else 'NOT SET'}")
    logger.info(f"📋 INPUT_QUEUE_NAME: {settings.INPUT_QUEUE_NAME}")
    logger.info(f"📋 CLASSIFICATION_QUEUE_NAME: {settings.CLASSIFICATION_QUEUE_NAME}")
    logger.info(f"📋 ARTIFACTS_DIR: {settings.ARTIFACTS_DIR}")
    logger.info(f"📋 SKIP_API_CALL: {settings.SKIP_API_CALL}")
    
    if not settings.AZURE_STORAGE_CONNECTION_STRING or not settings.INPUT_QUEUE_NAME or not settings.CLASSIFICATION_QUEUE_NAME:
        logger.error("❌ CRITICAL FAILURE: Missing Azure Storage or Queue Name configuration.")
        return
    
    logger.info(f"✅ SUCCESS: Configuration validated.")
    logger.info(f"📥 Input queue: {settings.INPUT_QUEUE_NAME}")
    logger.info(f"📤 Output queue: {settings.CLASSIFICATION_QUEUE_NAME}")

    # 🔗 SERVICE INITIALIZATION
    logger.info("🔗 INITIALIZING: Azure Queue Services")
    try:
        input_queue_service = AzureQueueService(settings.AZURE_STORAGE_CONNECTION_STRING, settings.INPUT_QUEUE_NAME)
        output_queue_service = AzureQueueService(settings.AZURE_STORAGE_CONNECTION_STRING, settings.CLASSIFICATION_QUEUE_NAME)
        logger.info("✅ SUCCESS: Input queue service initialized")
        logger.info("✅ SUCCESS: Output queue service initialized")
    except Exception as e:
        logger.error(f"❌ CRITICAL FAILURE: Failed to initialize queue services: {e}", exc_info=True)
        return
    
    logger.info("✅ SUCCESS: All services initialized successfully. Starting main loop.")
    
    total_messages_processed = 0
    total_messages_succeeded = 0
    total_messages_failed = 0
    
    while not shutdown_event.is_set():
        try:
            logger.info("🔄 POLLING: Checking for new messages in input queue...")
            messages = input_queue_service.receive_messages(max_messages=5)
            
            if messages:
                logger.info(f"✅ SUCCESS: Retrieved {len(messages)} messages.")
                total_messages_processed += len(messages)
                
                for message in messages:
                    logger.info(f"🔄 PROCESSING: Message ID: {message.id}")
                    
                    try:
                        content = message.content
                        logger.info(f"📝 RAW MESSAGE CONTENT (first 500 chars): {content[:500]}")
                        try:
                            # Try to decode base64 first
                            decoded_content = base64.b64decode(content).decode('utf-8')
                            message_data = json.loads(decoded_content)
                            logger.info("✅ SUCCESS: Message decoded as base64 JSON")
                        except:
                            # If base64 fails, try direct JSON
                            message_data = json.loads(content)
                            logger.info("✅ SUCCESS: Message parsed as direct JSON")
                        
                        success = process_message(message_data, input_queue_service, output_queue_service)
                        
                        if success:
                            logger.info(f"🗑️ PROCESSING: Deleting successfully processed message: {message.id}")
                            input_queue_service.delete_message(message.id, message.pop_receipt)
                            logger.info(f"✅ SUCCESS: Message {message.id} processed and deleted from input queue.")
                            total_messages_succeeded += 1
                        else:
                            logger.error(f"❌ FAILURE: Failed to process message {message.id}. It will be reprocessed later.")
                            total_messages_failed += 1
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ FAILURE: Invalid JSON in message {message.id}: {e}. Deleting message.")
                        input_queue_service.delete_message(message.id, message.pop_receipt)
                        total_messages_failed += 1
                        
                    except Exception as e:
                        logger.error(f"❌ FAILURE: Unhandled error processing message {message.id}: {e}", exc_info=True)
                        total_messages_failed += 1
            else:
                logger.info("ℹ️ INFO: No messages in queue. Waiting for 30 seconds.")
                if shutdown_event.wait(30):
                    break
            
        except Exception as e:
            logger.error(f"💥 CRITICAL FAILURE: Error in main loop: {e}", exc_info=True)
            logger.info("😴 WAITING: 30 seconds before retrying.")
            if shutdown_event.wait(30):
                break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🔔 KeyboardInterrupt received - shutting down")
    finally:
        logger.info("🛑 OCR processor exiting")