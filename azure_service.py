import logging
import time
from azure.storage.queue import QueueClient
from typing import List, Optional, Any
from config import settings
import os
import base64
import json
from datetime import datetime, timezone

class AzureQueueService:
    def __init__(self, conn_str: str, queue_name: str):
        """Initialize Azure Queue Service with extensive logging"""
        
        init_start_time = time.time()
        logging.info("🔗 INITIALIZING: Azure Queue Service")
        logging.info(f"📋 Target queue name: {queue_name}")
        
        try:
            # Initialize queue client
            logging.info("🔄 PROCESSING: Creating QueueClient connection")
            self.queue_client = QueueClient.from_connection_string(conn_str, queue_name)
            
            # Test connection
            logging.info("🔍 VALIDATING: Testing queue connection")
            try:
                # Try to get queue properties to validate connection
                properties = self.queue_client.get_queue_properties()
                logging.info("✅ SUCCESS: Queue connection validated")
                logging.info(f"📊 Queue properties retrieved - metadata count: {len(properties.metadata)}")
            except Exception as e:
                logging.warning(f"⚠️ WARNING: Could not validate queue properties: {e}")
                logging.info("🔄 PROCESSING: Continuing with queue initialization")
            
            init_duration = time.time() - init_start_time
            logging.info(f"✅ SUCCESS: AzureQueueService initialized in {init_duration:.2f} seconds for queue: '{queue_name}'")
            
        except Exception as e:
            init_duration = time.time() - init_start_time
            logging.error(f"❌ FAILURE: AzureQueueService initialization failed after {init_duration:.2f} seconds")
            logging.error(f"📊 Error: {e}")
            logging.error(f"📊 Error type: {type(e).__name__}")
            raise
        
        # 📁 ARTIFACTS DIRECTORY SETUP
        logging.info("📁 PROCESSING: Setting up artifacts directory")
        try:
            os.makedirs(settings.ARTIFACTS_DIR, exist_ok=True)
            logging.info(f"✅ SUCCESS: Artifacts directory ready: {settings.ARTIFACTS_DIR}")
        except Exception as e:
            logging.error(f"❌ FAILURE: Could not create artifacts directory: {settings.ARTIFACTS_DIR}")
            logging.error(f"📊 Error: {e}")

    def receive_messages(self, max_messages: int) -> List[Any]:
        """Receive messages from queue with extensive logging"""
        
        receive_start_time = time.time()
        logging.debug(f"📥 PROCESSING: Receiving up to {max_messages} messages from queue")
        
        try:
            # 📡 QUEUE POLLING
            logging.debug("📡 EXECUTING: Queue receive operation")
            poll_start_time = time.time()
            
            messages = self.queue_client.receive_messages(messages_per_page=max_messages, visibility_timeout=300)
            msg_list = list(messages)
            
            poll_duration = time.time() - poll_start_time
            
            if msg_list:
                logging.info(f"✅ SUCCESS: Retrieved {len(msg_list)} messages in {poll_duration:.2f} seconds")
                logging.info(f"📊 Visibility timeout: 300 seconds")
                
                # Log message IDs for tracking
                message_ids = [getattr(msg, 'id', 'N/A') for msg in msg_list]
                logging.info(f"📋 Message IDs: {message_ids}")
                
            else:
                logging.debug(f"ℹ️ INFO: No messages available (polled in {poll_duration:.2f} seconds)")

            # 💾 ARTIFACTS SAVING
            if msg_list:
                logging.info("💾 PROCESSING: Saving received messages to artifacts")
                artifact_start_time = time.time()
                
                for i, msg in enumerate(msg_list):
                    try:
                        message_id = getattr(msg, 'id', f'unknown_{i}')
                        logging.debug(f"💾 PROCESSING: Saving message {i+1}/{len(msg_list)} (ID: {message_id})")
                        
                        timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
                        raw_path = os.path.join(settings.ARTIFACTS_DIR, f"received_{self.queue_client.queue_name}_{timestamp}_{i}.txt")
                        
                        # Save raw message content
                        with open(raw_path, 'w', encoding='utf-8') as f:
                            f.write(msg.content)
                        
                        logging.info(f"✅ SUCCESS: Saved raw queue message to {os.path.basename(raw_path)} (id={message_id})")

                        # 🔍 CONTENT ANALYSIS AND DECODING
                        logging.debug(f"🔍 ANALYZING: Message content structure for message {i+1}")
                        content_length = len(msg.content)
                        logging.debug(f"📊 Raw content length: {content_length} characters")
                        
                        # Try to decode base64 content to JSON if possible
                        try:
                            logging.debug("🔄 PROCESSING: Attempting base64 decode and JSON parse")
                            decoded = base64.b64decode(msg.content).decode('utf-8')
                            json_obj = json.loads(decoded)
                            
                            json_path = raw_path.replace('.txt', '.decoded.json')
                            with open(json_path, 'w', encoding='utf-8') as jf:
                                json.dump(json_obj, jf, indent=2)
                            
                            logging.info(f"✅ SUCCESS: Saved decoded JSON to {os.path.basename(json_path)}")
                            
                            # Log the top-level keys for quick inspection
                            if isinstance(json_obj, dict):
                                keys = list(json_obj.keys())[:10]  # Limit to first 10 keys
                                logging.info(f"📋 Decoded JSON keys: {', '.join(keys)}")
                                if len(json_obj.keys()) > 10:
                                    logging.info(f"📊 Total keys: {len(json_obj.keys())} (showing first 10)")
                                    
                                # Log important fields if present
                                important_fields = ['UploadDatasheetid', 'ClientFileName', 'BatchId', 'DocReceivedId']
                                for field in important_fields:
                                    if field in json_obj:
                                        value = json_obj[field]
                                        logging.info(f"📋 {field}: {value}")
                            else:
                                logging.info(f"📊 Decoded JSON type: {type(json_obj)}")
                                
                        except Exception as decode_e:
                            logging.debug(f"ℹ️ INFO: Message not base64-encoded JSON: {decode_e}")
                            
                            # Try direct JSON parse
                            try:
                                logging.debug("🔄 PROCESSING: Attempting direct JSON parse")
                                json_obj = json.loads(msg.content)
                                json_path = raw_path.replace('.txt', '.direct.json')
                                with open(json_path, 'w', encoding='utf-8') as jf:
                                    json.dump(json_obj, jf, indent=2)
                                logging.info(f"✅ SUCCESS: Saved direct JSON to {os.path.basename(json_path)}")
                            except Exception:
                                logging.debug("ℹ️ INFO: Message content is not JSON format")

                    except Exception as e:
                        logging.error(f"❌ FAILURE: Failed to save received message {i+1} to artifacts: {e}")
                        logging.error(f"📊 Error type: {type(e).__name__}")
                
                artifact_duration = time.time() - artifact_start_time
                logging.info(f"✅ SUCCESS: All messages saved to artifacts in {artifact_duration:.2f} seconds")

            receive_duration = time.time() - receive_start_time
            logging.debug(f"📊 Total receive operation completed in {receive_duration:.2f} seconds")
            return msg_list
            
        except Exception as e:
            receive_duration = time.time() - receive_start_time
            logging.error(f"❌ FAILURE: Failed to receive messages after {receive_duration:.2f} seconds")
            logging.error(f"📊 Queue name: {self.queue_client.queue_name}")
            logging.error(f"📊 Max messages requested: {max_messages}")
            logging.error(f"📊 Error: {e}")
            logging.error(f"📊 Error type: {type(e).__name__}")
            return []

    def delete_message(self, message_id: str, pop_receipt: str):
        """Delete message from queue with extensive logging"""
        
        delete_start_time = time.time()
        logging.info(f"🗑️ PROCESSING: Deleting message from queue")
        logging.info(f"📋 Message ID: {message_id}")
        logging.info(f"📋 Queue: {self.queue_client.queue_name}")
        
        try:
            # 🗑️ MESSAGE DELETION
            logging.debug("🔄 EXECUTING: Queue delete operation")
            self.queue_client.delete_message(message_id, pop_receipt)
            
            delete_duration = time.time() - delete_start_time
            logging.info(f"✅ SUCCESS: Message {message_id} deleted successfully in {delete_duration:.2f} seconds")
            
        except Exception as e:
            delete_duration = time.time() - delete_start_time
            logging.error(f"❌ FAILURE: Failed to delete message after {delete_duration:.2f} seconds")
            logging.error(f"📋 Message ID: {message_id}")
            logging.error(f"📋 Queue: {self.queue_client.queue_name}")
            logging.error(f"📊 Error: {e}")
            logging.error(f"📊 Error type: {type(e).__name__}")

    def send_message(self, message: str, target_queue_name: Optional[str] = None):
        """Send message to queue with extensive logging"""
        send_start_time = time.time()
        target_queue = target_queue_name or self.queue_client.queue_name
        logger = logging.getLogger("azure_service")
        logger.info(f"SENDING TO CLASSIFICATION QUEUE '{target_queue}': {message}")
        logging.info("📤 PROCESSING: Preparing to send message to queue")
        logging.info(f"📊 Message length: {len(message)} characters")
        
        try:
            # 💾 ARTIFACTS SAVING
            logging.info("💾 PROCESSING: Saving outgoing message to artifacts")
            artifact_start_time = time.time()
            
            try:
                timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
                out_path = os.path.join(settings.ARTIFACTS_DIR, f"sent_{target_queue}_{timestamp}.txt")
                
                # Save raw message
                with open(out_path, 'w', encoding='utf-8') as f:
                    f.write(message)
                
                artifact_duration = time.time() - artifact_start_time
                logging.info(f"✅ SUCCESS: Saved outgoing message to {os.path.basename(out_path)} in {artifact_duration:.2f} seconds")
                
                # 🔍 JSON ANALYSIS AND PRETTY PRINTING
                logging.debug("🔍 ANALYZING: Message structure for JSON formatting")
                try:
                    parsed = json.loads(message)
                    json_out = out_path.replace('.txt', '.json')
                    with open(json_out, 'w', encoding='utf-8') as jf:
                        json.dump(parsed, jf, indent=2)
                    
                    logging.info(f"✅ SUCCESS: Saved formatted JSON to {os.path.basename(json_out)}")
                    
                    # Log message structure
                    if isinstance(parsed, dict):
                        keys = list(parsed.keys())
                        logging.info(f"📋 Outgoing message keys: {', '.join(keys)}")
                        logging.info(f"📊 Message fields count: {len(keys)}")
                        
                        # Log important field values
                        for key, value in parsed.items():
                            logging.info(f"📋 {key}: {value}")
                    else:
                        logging.info(f"📊 Outgoing message type: {type(parsed)}")
                        
                except Exception as json_e:
                    logging.debug(f"ℹ️ INFO: Outgoing message not JSON format: {json_e}")
                    
            except Exception as e:
                artifact_duration = time.time() - artifact_start_time
                logging.error(f"❌ FAILURE: Failed to save outgoing message after {artifact_duration:.2f} seconds: {e}")

            # 📡 MESSAGE TRANSMISSION
            logging.info("📡 PROCESSING: Transmitting message to queue")
            transmission_start_time = time.time()
            
            if target_queue_name:
                logging.info("🔄 PROCESSING: Creating target queue client")
                if not settings.AZURE_STORAGE_CONNECTION_STRING:
                    raise ValueError("Azure Storage connection string not available")
                    
                target_client = QueueClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING, target_queue_name)
                
                logging.debug("📡 EXECUTING: Sending message to target queue")
                target_client.send_message(message)
                
                transmission_duration = time.time() - transmission_start_time
                logging.info(f"✅ SUCCESS: Message sent to queue '{target_queue_name}' in {transmission_duration:.2f} seconds")
                
            else:
                logging.debug("📡 EXECUTING: Sending message to default queue")
                self.queue_client.send_message(message)
                
                transmission_duration = time.time() - transmission_start_time
                logging.info(f"✅ SUCCESS: Message sent to queue '{self.queue_client.queue_name}' in {transmission_duration:.2f} seconds")
            
            # 📊 FINAL SUCCESS SUMMARY
            total_duration = time.time() - send_start_time
            logging.info(f"🎉 SUCCESS: Message send operation completed in {total_duration:.2f} seconds")
            logging.info(f"📋 Total message length: {len(message)} characters")
            logging.info(f"📋 Target queue: {target_queue}")
            
        except Exception as e:
            total_duration = time.time() - send_start_time
            logging.error(f"❌ FAILURE: Failed to send message after {total_duration:.2f} seconds")
            logging.error(f"📋 Target queue: {target_queue}")
            logging.error(f"📊 Message length: {len(message)} characters")
            logging.error(f"📊 Error: {e}")
            logging.error(f"📊 Error type: {type(e).__name__}")
            raise