import logging
import requests
import time
from config import settings

def insert_ocr_document(xml_payload: str) -> bool:
    """Posts the generated XML payload to the API with extensive logging."""
    
    # 🌐 API CALL INITIALIZATION
    api_start_time = time.time()
    api_url = f"{settings.API_URL}{settings.INSERT_OCR_DOCUMENT_ENDPOINT}"
    
    logging.info("🌐 API CALL: Starting OCR document insertion")
    logging.info(f"📡 Target URL: {api_url}")
    logging.info(f"📊 Payload size: {len(xml_payload)} characters")
    
    # 🔧 REQUEST PREPARATION
    logging.info("🔧 PREPARING: API request headers and payload")
    
    headers = {
        'Content-Type': 'application/json' # The C# code used JSON wrapper for XML string
    }
    
    # The original C# code wraps the XML in a JSON object with specific parameters.
    # We replicate that structure here.
    service_request = {
        "OperationName": None, # Not specified, can be adapted if needed
        "OperationType": None,
        "ParameterList": [
            {
                "ParamName": "@SubDocXML",
                "ParamVal": xml_payload,
                "ParamDirection": "input"
            }
        ]
    }
    
    # Log request details
    logging.info("✅ SUCCESS: Request prepared successfully")
    logging.info(f"📋 Headers: {headers}")
    logging.info(f"📋 OperationName: {service_request.get('OperationName', 'None')}")
    logging.info(f"📋 OperationType: {service_request.get('OperationType', 'None')}")
    logging.info(f"📋 Parameter count: {len(service_request.get('ParameterList', []))}")
    
    try:
        # 📡 API REQUEST EXECUTION with simple retries/backoff
        logging.info("📡 EXECUTING: HTTP POST request to API (with retries)")
        max_attempts = 3
        backoff_seconds = 2
        response = None

        for attempt in range(1, max_attempts + 1):
            request_start_time = time.time()
            try:
                logging.info(f"Calling API {api_url} to insert OCR document. Payload length: {len(xml_payload)} (attempt {attempt}/{max_attempts})")
                response = requests.post(api_url, json=service_request, headers=headers, timeout=60)
                request_duration = time.time() - request_start_time
                logging.info(f"✅ SUCCESS: HTTP request completed in {request_duration:.2f} seconds")
                logging.info(f"📊 Response status: {response.status_code}")
                break
            except requests.exceptions.RequestException as req_e:
                request_duration = time.time() - request_start_time
                logging.warning(f"⚠️ WARNING: Request attempt {attempt} failed after {request_duration:.2f}s: {req_e}")
                if attempt < max_attempts:
                    logging.info(f"⏳ Backing off for {backoff_seconds} seconds before retry")
                    time.sleep(backoff_seconds)
                    backoff_seconds *= 2
                else:
                    logging.error("❌ FAILURE: All retry attempts for API call failed")
                    raise
        
        # 🔍 RESPONSE VALIDATION
        logging.info("🔍 VALIDATING: API response status")
        
        try:
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            logging.info("✅ SUCCESS: HTTP status validation passed")
        except requests.exceptions.HTTPError as e:
            logging.error(f"❌ FAILURE: HTTP error - {e}")
            logging.error(f"📊 Response text: {response.text}")
            return False
        
        # 📋 RESPONSE PARSING
        logging.info("📋 PROCESSING: Parsing API response")
        
        try:
            response_data = response.json()
            logging.info("✅ SUCCESS: Response parsed as JSON")
            logging.info(f"📊 Response data type: {type(response_data)}")
            
            if isinstance(response_data, list):
                logging.info(f"📊 Response array length: {len(response_data)}")
            elif isinstance(response_data, dict):
                logging.info(f"📊 Response object keys: {list(response_data.keys())}")
                
        except Exception as e:
            logging.error(f"❌ FAILURE: Response is not valid JSON - {e}")
            logging.error(f"📋 Response text: {response.text}")
            logging.error(f"📊 Response content type: {response.headers.get('content-type', 'unknown')}")
            return False

        # 🔍 SUCCESS FLAG DETECTION
        logging.info("🔍 ANALYZING: Response for success indicators")
        
        success = False
        success_path = "unknown"
        
        try:
            if isinstance(response_data, list) and len(response_data) > 0 and isinstance(response_data[0], dict):
                if response_data[0].get('IsSuccess') == True:
                    success = True
                    success_path = "response_data[0].IsSuccess"
                    logging.info("✅ SUCCESS: Found success flag in response_data[0].IsSuccess")
                else:
                    logging.info(f"ℹ️ INFO: response_data[0].IsSuccess = {response_data[0].get('IsSuccess')}")
                    
            elif isinstance(response_data, dict):
                if response_data.get('IsSuccess') == True:
                    success = True
                    success_path = "response_data.IsSuccess"
                    logging.info("✅ SUCCESS: Found success flag in response_data.IsSuccess")
                elif response_data.get('isSuccess') == True:
                    success = True
                    success_path = "response_data.isSuccess"
                    logging.info("✅ SUCCESS: Found success flag in response_data.isSuccess")
                else:
                    logging.info(f"ℹ️ INFO: response_data.IsSuccess = {response_data.get('IsSuccess')}")
                    logging.info(f"ℹ️ INFO: response_data.isSuccess = {response_data.get('isSuccess')}")
                    
        except Exception as e:
            logging.error(f"❌ FAILURE: Error analyzing response for success flag - {e}")
            success = False

        # 📊 FINAL RESULT PROCESSING
        total_duration = time.time() - api_start_time
        
        if success:
            logging.info(f"🎉 SUCCESS: API call completed successfully in {total_duration:.2f} seconds")
            logging.info(f"✅ Success detected via: {success_path}")
            logging.info(f"API call to {api_url} indicated success.")
            return True
        else:
            logging.error(f"❌ FAILURE: API call failed after {total_duration:.2f} seconds")
            logging.error(f"📋 Response did not indicate success")
            logging.error(f"📋 Full response: {response.text}")
            logging.warning(f"API call to {api_url} did not indicate success. Response: {response.text}")
            return False

    except requests.exceptions.Timeout as e:
        total_duration = time.time() - api_start_time
        logging.error(f"❌ FAILURE: API request timeout after {total_duration:.2f} seconds")
        logging.error(f"⏰ Timeout error: {e}")
        logging.error(f"Failed to call API at {api_url}. Error: {e}")
        return False
        
    except requests.exceptions.ConnectionError as e:
        total_duration = time.time() - api_start_time
        logging.error(f"❌ FAILURE: API connection error after {total_duration:.2f} seconds")
        logging.error(f"🌐 Connection error: {e}")
        logging.error(f"Failed to call API at {api_url}. Error: {e}")
        return False
        
    except requests.exceptions.RequestException as e:
        total_duration = time.time() - api_start_time
        logging.error(f"❌ FAILURE: API request exception after {total_duration:.2f} seconds")
        logging.error(f"📡 Request error: {e}")
        logging.error(f"Failed to call API at {api_url}. Error: {e}")
        return False
        
    except Exception as e:
        total_duration = time.time() - api_start_time
        logging.error(f"💥 CRITICAL FAILURE: Unexpected error after {total_duration:.2f} seconds")
        logging.error(f"❌ Error: {e}")
        logging.error(f"📊 Error type: {type(e).__name__}")
        logging.error(f"An unexpected error occurred during API call to {api_url}. Error: {e}")
        return False