# import logging
# import re
# import json
# import glob
# from typing import List, Dict, Any, Tuple
# from pdf2image import convert_from_path
# import pytesseract
# from PIL.Image import Image
# from api_client import insert_ocr_document
# from azure_service import AzureQueueService
# from config import settings
# from data_models import create_subdocument_xml
# import os
# import shutil
# from datetime import datetime, timezone
# import requests
# from urllib.parse import urlparse
# import traceback


# def save_pdf_to_artifacts(pdf_path: str, message_data: Dict[str, Any]) -> str:
#     """Ensure the PDF is available locally under the artifacts folder.
#     Returns the local path to the saved PDF (which may be the original path if already local).
#     Supports local files, UNC paths, HTTP(s) URLs and Azure Blob URLs (if azure-storage-blob is installed).
#     """
#     try:
#         os.makedirs(settings.ARTIFACTS_DIR, exist_ok=True)
#     except Exception:
#         logging.warning(f"Could not create artifacts directory: {settings.ARTIFACTS_DIR}")

#     timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
#     pdf_filename = os.path.basename(urlparse(pdf_path).path) or f"document_{timestamp}.pdf"
#     dest_pdf = os.path.join(settings.ARTIFACTS_DIR, f"input_{message_data.get('UploadDatasheetid')}_{timestamp}_{pdf_filename}")

#     # If it's already a local file path
#     try:
#         if os.path.isfile(pdf_path):
#             try:
#                 shutil.copy2(pdf_path, dest_pdf)
#                 logging.info(f"Copied local PDF to artifacts: {dest_pdf}")
#                 return dest_pdf
#             except Exception as e:
#                 logging.warning(f"Failed to copy local PDF to artifacts: {e}")
#                 return pdf_path
#     except Exception:
#         # os.path.isfile can raise on some path types; ignore and continue
#         pass

#     # HTTP/HTTPS download
#     parsed = urlparse(pdf_path)
#     if parsed.scheme in ("http", "https"):
#         try:
#             resp = requests.get(pdf_path, stream=True, timeout=60)
#             resp.raise_for_status()
#             with open(dest_pdf, 'wb') as f:
#                 for chunk in resp.iter_content(chunk_size=8192):
#                     if chunk:
#                         f.write(chunk)
#             logging.info(f"Downloaded PDF via HTTP to artifacts: {dest_pdf}")
#             return dest_pdf
#         except Exception as e:
#             logging.warning(f"Failed to download PDF from {pdf_path}: {e}")
#             return pdf_path

#     # Try Azure Blob URL if present
#     if '.blob.core.windows.net' in pdf_path:
#         try:
#             from azure.storage.blob import BlobClient
#             try:
#                 # Try from_blob_url (works with SAS or public URLs)
#                 blob_client = BlobClient.from_blob_url(pdf_path)
#                 with open(dest_pdf, 'wb') as f:
#                     stream = blob_client.download_blob()
#                     f.write(stream.readall())
#                 logging.info(f"Downloaded PDF from Azure Blob to artifacts: {dest_pdf}")
#                 return dest_pdf
#             except Exception:
#                 # Fallback: attempt to parse container/blob and use connection string
#                 parsed = urlparse(pdf_path)
#                 path_parts = parsed.path.lstrip('/').split('/', 1)
#                 if len(path_parts) == 2:
#                     container, blob_name = path_parts
#                     blob_client = BlobClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING, container, blob_name)
#                     with open(dest_pdf, 'wb') as f:
#                         stream = blob_client.download_blob()
#                         f.write(stream.readall())
#                     logging.info(f"Downloaded PDF from Azure Blob (conn str) to artifacts: {dest_pdf}")
#                     return dest_pdf
#         except ImportError:
#             logging.warning("azure-storage-blob not installed; cannot download blob URLs directly. Skipping.")
#         except Exception as e:
#             logging.warning(f"Failed to download Azure Blob PDF: {e}")

#     # Otherwise, attempt to treat as a path and copy
#     try:
#         shutil.copy2(pdf_path, dest_pdf)
#         logging.info(f"Copied PDF to artifacts: {dest_pdf}")
#         return dest_pdf
#     except Exception as e:
#         logging.warning(f"Could not save PDF to artifacts for path {pdf_path}: {e}")
#         return pdf_path


# def get_text_from_image(image: Image) -> str:
#     """Extracts text from a single image using Tesseract."""
#     try:
#         logging.debug(f"   🔤 Running Tesseract OCR (timeout: {settings.TESSERACT_TIMEOUT}s)")
#         start_time = datetime.now()
        
#         text = pytesseract.image_to_string(image, timeout=settings.TESSERACT_TIMEOUT)
        
#         end_time = datetime.now()
#         processing_time = (end_time - start_time).total_seconds()
#         logging.debug(f"   ⏱️  OCR completed in {processing_time:.2f} seconds")
        
#         return text
        
#     except RuntimeError as timeout_error:
#         if "timeout" in str(timeout_error).lower():
#             logging.error(f"   ⏰ Tesseract processing timed out after {settings.TESSERACT_TIMEOUT}s: {timeout_error}")
#         else:
#             logging.error(f"   ❌ Tesseract runtime error: {timeout_error}")
#         return ""
#     except Exception as e:
#         logging.error(f"   ❌ Unexpected error during Tesseract OCR: {e}")
#         logging.error(f"   📍 Error type: {type(e).__name__}")
#         return ""

# def get_text_from_document(pdf_path: str) -> List[str]:
#     """Converts a PDF to images and extracts text from each page."""
#     logging.info(f"📖 Starting text extraction from PDF: {pdf_path}")
    
#     # Check if file exists and get basic info
#     try:
#         file_size = os.path.getsize(pdf_path)
#         logging.info(f"   📁 File size: {file_size:,} bytes ({file_size / (1024*1024):.2f} MB)")
#     except Exception as e:
#         logging.warning(f"   ⚠️  Could not get file size: {e}")
    
#     pdf_pages_text = []
    
#     try:
#         logging.info(f"🖼️  Converting PDF to images (DPI: 300)")
#         images = convert_from_path(pdf_path, dpi=300)
#         logging.info(f"   ✅ Successfully converted PDF to {len(images)} page images")
        
#         for i, image in enumerate(images):
#             page_num = i + 1
#             logging.info(f"🔍 Processing page {page_num}/{len(images)}")
            
#             # Log image details
#             try:
#                 width, height = image.size
#                 mode = image.mode
#                 logging.debug(f"   🖼️  Image: {width}x{height} pixels, mode: {mode}")
#             except Exception as e:
#                 logging.debug(f"   ⚠️  Could not get image details: {e}")
            
#             # Extract text using OCR
#             try:
#                 logging.debug(f"   🔤 Starting OCR for page {page_num}")
#                 text = get_text_from_image(image)
                
#                 # Log OCR results
#                 char_count = len(text) if text else 0
#                 word_count = len(text.split()) if text else 0
#                 line_count = text.count('\n') + 1 if text else 0
                
#                 logging.info(f"   📄 OCR completed for page {page_num}:")
#                 logging.info(f"      📏 Characters: {char_count:,}")
#                 logging.info(f"      📝 Words: {word_count:,}")
#                 logging.info(f"      📋 Lines: {line_count}")
                
#                 if char_count == 0:
#                     logging.warning(f"   ⚠️  Page {page_num} appears to be empty or unreadable")
#                 else:
#                     # Show a preview of the extracted text
#                     preview_text = text[:200].replace('\n', ' ').replace('\r', ' ').strip()
#                     if len(text) > 200:
#                         preview_text += "..."
#                     logging.debug(f"   📖 Text preview: '{preview_text}'")
                
#                 pdf_pages_text.append(text)
                
#             except Exception as e:
#                 logging.error(f"   ❌ OCR failed for page {page_num}: {e}")
#                 pdf_pages_text.append("")  # Add empty string for failed pages
    
#     except Exception as e:
#         logging.error(f"❌ Failed to process PDF file {pdf_path}: {e}")
#         logging.error(f"   📍 Error type: {type(e).__name__}")
#         # Try to provide more context
#         if "poppler" in str(e).lower():
#             logging.error(f"   💡 Hint: This might be a Poppler installation issue")
#         elif "timeout" in str(e).lower():
#             logging.error(f"   💡 Hint: PDF processing timed out - file might be too large or complex")
    
#     logging.info(f"📊 TEXT EXTRACTION SUMMARY:")
#     logging.info(f"   📄 Total pages processed: {len(pdf_pages_text)}")
    
#     # Calculate overall statistics
#     total_chars = sum(len(text) for text in pdf_pages_text)
#     total_words = sum(len(text.split()) for text in pdf_pages_text)
#     empty_pages = sum(1 for text in pdf_pages_text if not text.strip())
    
#     logging.info(f"   📏 Total characters: {total_chars:,}")
#     logging.info(f"   📝 Total words: {total_words:,}")
#     logging.info(f"   📄 Empty/failed pages: {empty_pages}")
    
#     if empty_pages > 0:
#         logging.warning(f"   ⚠️  {empty_pages} pages had no extractable text")
    
#     # Show per-page statistics
#     logging.debug(f"📋 PER-PAGE STATISTICS:")
#     for i, text in enumerate(pdf_pages_text, 1):
#         char_count = len(text)
#         word_count = len(text.split())
#         status = "✅" if char_count > 0 else "❌"
#         logging.debug(f"   {status} Page {i}: {char_count:,} chars, {word_count:,} words")
    
#     return pdf_pages_text

# def normalize_text(text: str) -> str:
#     """Replaces multiple whitespace characters with a single space."""
#     return re.sub(r'\s+', ' ', text).strip()

# def get_first_page(pages_text: List[str], identifier: str) -> int:
#     """Finds the first page number containing the identifier."""
#     logging.info(f"🔍 Searching for starting identifier: '{identifier}' across {len(pages_text)} pages")
    
#     for i, text in enumerate(pages_text):
#         normalized_text = normalize_text(text)
#         page_num = i + 1
        
#         # Log detailed search information
#         logging.debug(f"   📄 Page {page_num}: Text length {len(text)} chars, normalized length {len(normalized_text)} chars")
        
#         # Extract first 100 characters for logging context
#         text_preview = normalized_text[:100].replace('\n', ' ').replace('\r', ' ')
#         if len(normalized_text) > 100:
#             text_preview += "..."
#         logging.debug(f"   📄 Page {page_num} preview: '{text_preview}'")
        
#         if identifier.lower() in normalized_text.lower():
#             logging.info(f"✅ Found starting identifier '{identifier}' on page {page_num}")
            
#             # Find the exact position and context
#             lower_text = normalized_text.lower()
#             lower_identifier = identifier.lower()
#             pos = lower_text.find(lower_identifier)
            
#             # Extract context around the found identifier
#             context_start = max(0, pos - 50)
#             context_end = min(len(normalized_text), pos + len(identifier) + 50)
#             context = normalized_text[context_start:context_end]
            
#             logging.info(f"   🎯 Context around identifier: '...{context}...'")
#             logging.info(f"   📍 Position in page: character {pos}")
            
#             return page_num
    
#     logging.warning(f"❌ Starting identifier '{identifier}' not found in any of the {len(pages_text)} pages")
#     return -1

# def get_last_page(pages_text: List[str], identifier: str, start_page: int) -> int:
#     """Finds the last page for a document section."""
#     logging.info(f"🔍 Searching for ending identifier: '{identifier}' starting from page {start_page}")
    
#     for i in range(start_page - 1, len(pages_text)):
#         page_num = i + 1
#         text = pages_text[i]
#         normalized_text = normalize_text(text)
        
#         # Log detailed search information
#         logging.debug(f"   📄 Page {page_num}: Checking for ending identifier")
        
#         # Extract first 100 characters for logging context
#         text_preview = normalized_text[:100].replace('\n', ' ').replace('\r', ' ')
#         if len(normalized_text) > 100:
#             text_preview += "..."
#         logging.debug(f"   📄 Page {page_num} preview: '{text_preview}'")
        
#         if identifier.lower() in normalized_text.lower():
#             logging.info(f"✅ Found ending identifier '{identifier}' on page {page_num}")
            
#             # Find the exact position and context
#             lower_text = normalized_text.lower()
#             lower_identifier = identifier.lower()
#             pos = lower_text.find(lower_identifier)
            
#             # Extract context around the found identifier
#             context_start = max(0, pos - 50)
#             context_end = min(len(normalized_text), pos + len(identifier) + 50)
#             context = normalized_text[context_start:context_end]
            
#             logging.info(f"   🎯 Context around ending identifier: '...{context}...'")
#             logging.info(f"   📍 Position in page: character {pos}")
            
#             return page_num
    
#     logging.warning(f"❌ Ending identifier '{identifier}' not found from page {start_page} onwards, defaulting to last page {len(pages_text)}")
#     return len(pages_text) # Default to the last page if identifier not found

# def apply_fallback_page_mapping(sub_document_rows: List[Dict[str, Any]], unmapped_documents: List[Dict[str, Any]], 
#                                demarcated_ranges: List[Tuple[int, int]], total_pages: int) -> None:
#     """Apply fallback page mapping strategy for documents where identifiers weren't found."""
    
#     logging.info(f"🔄 Starting fallback page mapping process")
#     logging.info(f"   📊 Total pages in document: {total_pages}")
#     logging.info(f"   📋 Number of unmapped documents: {len(unmapped_documents)}")
#     logging.info(f"   🎯 Already demarcated ranges: {demarcated_ranges}")
    
#     # Calculate available page ranges (not yet assigned)
#     used_pages = set()
#     for start, end in demarcated_ranges:
#         for page in range(start, end + 1):
#             used_pages.add(page)
    
#     available_pages = [page for page in range(1, total_pages + 1) if page not in used_pages]
    
#     logging.info(f"📄 Used pages from demarcated ranges: {sorted(list(used_pages))}")
#     logging.info(f"📄 Available pages for fallback mapping: {available_pages}")
    
#     if not available_pages:
#         logging.warning("❌ No available pages for fallback mapping - all pages already assigned")
#         return
    
#     # Custom page distribution based on document type order and specific requirements
#     # This implements the expected distribution: 6615->1-3, 6620->4-5, 6602->6-12
#     custom_distributions = {
#         "6615": {"pages": 3, "priority": 1},  # First 3 pages
#         "6620": {"pages": 2, "priority": 2},  # Next 2 pages  
#         "6602": {"pages": 0, "priority": 3}   # Remaining pages (0 means "rest")
#     }
    
#     logging.info(f"📋 Custom distribution rules: {custom_distributions}")
    
#     # Sort unmapped documents by priority if custom distribution exists
#     unmapped_docs_sorted = []
#     for unmapped_doc in unmapped_documents:
#         doc_type_id = str(unmapped_doc.get("DocumentTypeID"))
#         if doc_type_id in custom_distributions:
#             priority = custom_distributions[doc_type_id]["priority"]
#             unmapped_docs_sorted.append((priority, unmapped_doc))
#             logging.info(f"   🏷️  DocumentTypeID {doc_type_id}: Custom priority {priority}")
#         else:
#             # If not in custom distribution, add at end
#             unmapped_docs_sorted.append((999, unmapped_doc))
#             logging.info(f"   🏷️  DocumentTypeID {doc_type_id}: Default priority 999 (unknown type)")
    
#     # Sort by priority
#     unmapped_docs_sorted.sort(key=lambda x: x[0])
    
#     logging.info(f"📋 Processing order after priority sorting:")
#     for i, (priority, doc) in enumerate(unmapped_docs_sorted, 1):
#         doc_type_id = str(doc.get("DocumentTypeID"))
#         logging.info(f"   {i}. DocumentTypeID {doc_type_id} (priority {priority})")
    
#     current_page_idx = 0
#     for priority, unmapped_doc in unmapped_docs_sorted:
#         doc_type_id = str(unmapped_doc.get("DocumentTypeID"))
        
#         logging.info(f"🔄 Processing DocumentTypeID {doc_type_id} (priority {priority})")
        
#         if doc_type_id in custom_distributions:
#             # Use custom distribution
#             pages_needed = custom_distributions[doc_type_id]["pages"]
#             if pages_needed == 0:  # Special case: take all remaining pages
#                 pages_needed = len(available_pages) - current_page_idx
#                 logging.info(f"   📏 Special case: Taking all remaining pages ({pages_needed})")
#             else:
#                 logging.info(f"   📏 Custom allocation: {pages_needed} pages")
#         else:
#             # Fallback to even distribution for unknown document types
#             remaining_docs = len([d for p, d in unmapped_docs_sorted if p >= priority])
#             remaining_pages = len(available_pages) - current_page_idx
#             pages_needed = max(1, remaining_pages // remaining_docs)
#             logging.info(f"   📏 Even distribution: {pages_needed} pages ({remaining_pages} remaining ÷ {remaining_docs} docs)")
        
#         # Assign pages
#         if current_page_idx < len(available_pages):
#             from_page = available_pages[current_page_idx]
#             end_idx = min(current_page_idx + pages_needed, len(available_pages))
#             to_page = available_pages[end_idx - 1] if end_idx > current_page_idx else from_page
            
#             logging.info(f"   📍 Assigning pages {from_page} to {to_page} (indices {current_page_idx} to {end_idx-1})")
            
#             # Find and update the corresponding row in sub_document_rows
#             row_updated = False
#             for row in sub_document_rows:
#                 if str(row.get("DocumentTypeId")) == doc_type_id:
#                     old_from = row.get("FromPageNumber", 0)
#                     old_to = row.get("ToPageNumber", 0)
#                     old_pages = row.get("NoOfPages", 0)
                    
#                     row["FromPageNumber"] = from_page
#                     row["ToPageNumber"] = to_page
#                     row["NoOfPages"] = to_page - from_page + 1
                    
#                     logging.info(f"✅ Updated DocumentTypeID={doc_type_id}:")
#                     logging.info(f"   📄 Before: pages {old_from}-{old_to} ({old_pages} pages)")
#                     logging.info(f"   📄 After:  pages {from_page}-{to_page} ({to_page - from_page + 1} pages)")
                    
#                     row_updated = True
#                     break
            
#             if not row_updated:
#                 logging.warning(f"⚠️  Could not find matching row for DocumentTypeID {doc_type_id}")
            
#             current_page_idx = end_idx
#         else:
#             logging.warning(f"⚠️  No more available pages for DocumentTypeID {doc_type_id}")
    
#     # Log final mapping summary
#     total_mapped_pages = sum(row.get("NoOfPages", 0) for row in sub_document_rows)
#     logging.info(f"📊 FALLBACK MAPPING SUMMARY:")
#     logging.info(f"   📄 Total pages in document: {total_pages}")
#     logging.info(f"   📄 Total mapped pages: {total_mapped_pages}")
#     logging.info(f"   📋 Document types processed: {len(sub_document_rows)}")
    
#     # Log final page assignments
#     logging.info(f"📋 FINAL PAGE ASSIGNMENTS:")
#     for row in sub_document_rows:
#         doc_type_id = row.get("DocumentTypeId")
#         from_page = row.get("FromPageNumber", 0)
#         to_page = row.get("ToPageNumber", 0)
#         num_pages = row.get("NoOfPages", 0)
#         if from_page > 0:
#             logging.info(f"   📄 DocumentTypeID {doc_type_id}: pages {from_page}-{to_page} ({num_pages} pages)")
#         else:
#             logging.info(f"   ❌ DocumentTypeID {doc_type_id}: No pages assigned")
    
#     if total_mapped_pages < total_pages:
#         unmapped_count = total_pages - total_mapped_pages
#         logging.warning(f"⚠️  {unmapped_count} pages remain unmapped")
#     elif total_mapped_pages > total_pages:
#         overlap_count = total_mapped_pages - total_pages
#         logging.warning(f"⚠️  Page overlap detected: {overlap_count} pages double-assigned")
#     else:
#         logging.info(f"✅ Perfect mapping: All {total_pages} pages assigned exactly once")

# from azure_service import AzureQueueService

# def process_pdf_ocr(pdf_path: str, message_data: Dict[str, Any], queue_service: AzureQueueService) -> None:
#     """Main orchestration logic for processing a single PDF."""
#     upload_id = message_data.get('UploadDatasheetid')
#     logging.info(f"Processing PDF for UploadDatasheetid: {upload_id}")

#     # Check for existing processing (deduplication)
#     try:
#         manifest_pattern = os.path.join(settings.ARTIFACTS_DIR, f"manifest_{upload_id}_*.json")
#         existing_manifests = glob.glob(manifest_pattern)
#         if existing_manifests:
#             logging.warning(f"Skipping already processed message UploadDatasheetid={upload_id}. Existing manifests: {len(existing_manifests)}")
#             return
#     except Exception as e:
#         logging.warning(f"Failed to check for existing manifests: {e}")

#     # Prepare manifest data
#     start_ts = datetime.now(timezone.utc)
#     timestamp = start_ts.strftime('%Y%m%dT%H%M%S%f')
#     manifest: Dict[str, Any] = {
#         'UploadDatasheetid': upload_id,
#         'BatchId': message_data.get('BatchId'),
#         'DocReceivedId': message_data.get('DocReceivedId'),
#         'ClientId': message_data.get('ClientId'),
#         'StartTime': start_ts.isoformat(),
#         'SourcePdf': pdf_path,
#         'LocalPdf': None,
#         'TotalPages': None,
#         'Status': 'processing',
#         'Errors': []
#     }

#     # Ensure artifacts directory exists
#     try:
#         os.makedirs(settings.ARTIFACTS_DIR, exist_ok=True)
#     except Exception:
#         logging.warning(f"Could not create artifacts directory: {settings.ARTIFACTS_DIR}")

#     # Ensure PDF is local and saved to artifacts; use returned local path for OCR
#     logging.info(f"Saving or locating PDF for UploadDatasheetid={message_data.get('UploadDatasheetid')} from source: {pdf_path}")
#     local_pdf = save_pdf_to_artifacts(pdf_path, message_data)
#     manifest['LocalPdf'] = local_pdf
#     logging.info(f"Local PDF path for processing: {local_pdf}")

#     # Run OCR and capture per-page text
#     try:
#         logging.info(f"Starting OCR extraction for local PDF: {local_pdf}")
#         pdf_pages_text = get_text_from_document(local_pdf)
#         logging.info(f"Completed OCR extraction: {len(pdf_pages_text)} pages extracted")
#     except Exception as e:
#         logging.error(f"Unexpected error during OCR conversion: {e}")
#         manifest['Errors'].append(str(e))
#         pdf_pages_text = []
#     if not pdf_pages_text:
#         msg = f"No text could be extracted from {local_pdf}. Aborting."
#         logging.warning(msg)
#         manifest['Errors'].append(msg)
#         manifest['TotalPages'] = 0
#         manifest['Status'] = 'failed'
#         # Write manifest
#         try:
#             manifest_path = os.path.join(settings.ARTIFACTS_DIR, f"manifest_{message_data.get('UploadDatasheetid')}_{timestamp}.json")
#             with open(manifest_path, 'w', encoding='utf-8') as mf:
#                 json.dump(manifest, mf, indent=2)
#             logging.info(f"Saved manifest to artifacts: {manifest_path}")
#         except Exception as e:
#             logging.warning(f"Failed to write manifest: {e}")
#         return

#     total_pages = len(pdf_pages_text)
#     manifest['TotalPages'] = total_pages

#     # Save per-page OCR text into a JSON file and separate text files
#     try:
#         logging.info(f"💾 Saving OCR results to artifacts directory")
        
#         pages_json_path = os.path.join(settings.ARTIFACTS_DIR, f"ocr_pages_{message_data.get('UploadDatasheetid')}_{timestamp}.json")
#         logging.info(f"   📄 Saving consolidated JSON: {os.path.basename(pages_json_path)}")
        
#         with open(pages_json_path, 'w', encoding='utf-8') as pf:
#             json.dump({'pages': pdf_pages_text}, pf, indent=2)
        
#         json_size = os.path.getsize(pages_json_path)
#         logging.info(f"   ✅ Saved consolidated OCR JSON ({json_size:,} bytes)")

#         # Also save individual page text files
#         logging.info(f"   📄 Saving individual page text files...")
        
#         for idx, page_text in enumerate(pdf_pages_text, start=1):
#             try:
#                 page_txt_path = os.path.join(settings.ARTIFACTS_DIR, f"ocr_page_{message_data.get('UploadDatasheetid')}_{timestamp}_p{idx}.txt")
                
#                 with open(page_txt_path, 'w', encoding='utf-8') as ptxt:
#                     ptxt.write(page_text if page_text is not None else "")
                
#                 # Log file details
#                 file_size = os.path.getsize(page_txt_path)
#                 char_count = len(page_text) if page_text else 0
#                 status = "✅" if char_count > 0 else "❌"
                
#                 logging.debug(f"      {status} Page {idx}: {os.path.basename(page_txt_path)} ({file_size:,} bytes, {char_count:,} chars)")
                
#             except Exception as e:
#                 logging.warning(f"      ❌ Failed to write page {idx} text file: {e}")
        
#         logging.info(f"   ✅ Completed saving {len(pdf_pages_text)} individual page files")
        
#     except Exception as e:
#         logging.warning(f"❌ Failed to save per-page OCR texts: {e}")
#         logging.warning(f"   📍 Error type: {type(e).__name__}")
#     identifiers = message_data.get("Identifiers", [])
#     sub_document_rows = []
#     demarcated_ranges: List[Tuple[int, int]] = []
#     unmapped_documents = []  # Track documents where identifiers weren't found

#     logging.info(f"🎯 Starting page demarcation process")
#     logging.info(f"   📊 Total pages in document: {total_pages}")
#     logging.info(f"   📋 Number of identifiers to process: {len(identifiers)}")
    
#     # Log all identifiers for debugging
#     for i, ident in enumerate(identifiers, 1):
#         doc_type_id = ident.get("DocumentTypeID")
#         start_id = ident.get("StartingIdentifier")
#         end_id = ident.get("EndingIdentifier")
#         num_pages = ident.get("NoOfPages", 0)
#         sequence = ident.get("Sequence")
        
#         logging.info(f"   📝 Identifier {i}: DocumentTypeID={doc_type_id}, Sequence={sequence}")
#         logging.info(f"      🔍 Start: '{start_id}', End: '{end_id}', ExpectedPages: {num_pages}")

#     for ident_idx, ident in enumerate(identifiers, 1):
#         logging.info(f"🔄 Processing identifier {ident_idx}/{len(identifiers)}")
#         logging.debug(f"   📝 Full identifier data: {ident}")
        
#         start_id = ident.get("StartingIdentifier")
#         end_id = ident.get("EndingIdentifier")
#         num_pages = ident.get("NoOfPages", 0)
#         doc_type_id = ident.get("DocumentTypeID")
#         sequence = ident.get("Sequence")

#         logging.info(f"   📋 DocumentTypeID: {doc_type_id}, Sequence: {sequence}")
#         logging.info(f"   🔍 Looking for start identifier: '{start_id}'")
#         if end_id:
#             logging.info(f"   🔍 Looking for end identifier: '{end_id}'")
#         if num_pages > 0:
#             logging.info(f"   📏 Expected page count: {num_pages}")

#         from_page, to_page = 0, 0

#         if start_id:
#             first_page = get_first_page(pdf_pages_text, start_id)
#             logging.info(f"   📍 Starting page result: {first_page}")
            
#             if first_page != -1:
#                 last_page = total_pages
#                 logging.info(f"   📍 Initial ending page (default): {last_page}")
                
#                 if num_pages > 0:
#                     calculated_last_page = min(first_page + num_pages - 1, total_pages)
#                     logging.info(f"   📏 Calculated ending page based on NoOfPages ({num_pages}): {calculated_last_page}")
#                     last_page = calculated_last_page
#                 elif end_id:
#                     found_last_page = get_last_page(pdf_pages_text, end_id, first_page)
#                     logging.info(f"   📍 Found ending page using end identifier: {found_last_page}")
#                     last_page = found_last_page

#                 logging.info(f"   📄 Proposed page range: {first_page} to {last_page}")

#                 # Basic overlap check
#                 is_overlapping = any(max(first_page, r[0]) <= min(last_page, r[1]) for r in demarcated_ranges)
#                 if not is_overlapping:
#                     from_page, to_page = first_page, last_page
#                     demarcated_ranges.append((from_page, to_page))
#                     logging.info(f"✅ Accepted range for DocumentTypeID={doc_type_id}: pages {from_page}-{to_page}")
#                     logging.info(f"   📋 Updated demarcated_ranges: {demarcated_ranges}")
#                 else:
#                     overlapping_ranges = [r for r in demarcated_ranges if max(first_page, r[0]) <= min(last_page, r[1])]
#                     logging.warning(f"❌ Range {first_page}-{last_page} overlaps with existing ranges: {overlapping_ranges}")
#                     logging.warning(f"   ⚠️  Skipping overlapping range for identifier '{start_id}' (DocumentTypeID={doc_type_id})")
#             else:
#                 # Identifier not found - add to unmapped list for fallback processing
#                 logging.warning(f"❌ Starting identifier '{start_id}' not found for DocumentTypeID={doc_type_id}")
#                 logging.info(f"   📋 Adding to unmapped documents list for fallback processing")
#                 unmapped_documents.append(ident)
#         else:
#             logging.warning(f"⚠️  No starting identifier provided for DocumentTypeID={doc_type_id}")
#             logging.info(f"   📋 Adding to unmapped documents list for fallback processing")
#             unmapped_documents.append(ident)

#         # Create sub-document row
#         row = {
#             "DocReceivedId": message_data.get("DocReceivedId"),
#             "FromPageNumber": from_page,
#             "ToPageNumber": to_page,
#             "FileNumber": message_data.get("FirmFile"),
#             "DocumentTypeId": doc_type_id,
#             "UploadDataSheetId": message_data.get("UploadDatasheetid"),
#             "TotalNumberOfpages": total_pages,
#             "NoOfPages": to_page - from_page + 1 if from_page > 0 else 0,
#             "Sequence": sequence,
#             "SessionId": message_data.get("SessionId")
#         }
#         sub_document_rows.append(row)
        
#         logging.info(f"   📄 Created sub-document row:")
#         logging.info(f"      📋 DocumentTypeId: {row['DocumentTypeId']}")
#         logging.info(f"      📄 Pages: {row['FromPageNumber']}-{row['ToPageNumber']} ({row['NoOfPages']} pages)")
#         logging.info(f"      🔢 Sequence: {row['Sequence']}")
#         logging.debug(f"      📝 Full row data: {row}")

#     logging.info(f"📊 INITIAL DEMARCATION SUMMARY:")
#     logging.info(f"   📄 Total pages: {total_pages}")
#     logging.info(f"   ✅ Successfully demarcated: {len(demarcated_ranges)} ranges")
#     logging.info(f"   ❌ Unmapped documents: {len(unmapped_documents)}")
#     logging.info(f"   📋 Demarcated ranges: {demarcated_ranges}")
    
#     # Show pages coverage
#     covered_pages = set()
#     for start, end in demarcated_ranges:
#         for page in range(start, end + 1):
#             covered_pages.add(page)
    
#     uncovered_pages = [page for page in range(1, total_pages + 1) if page not in covered_pages]
#     logging.info(f"   📄 Pages covered by identifiers: {sorted(list(covered_pages))}")
#     logging.info(f"   📄 Pages not covered: {uncovered_pages}")

#     # Apply fallback page mapping for unmapped documents
#     if unmapped_documents:
#         logging.info(f"🔄 Applying fallback page mapping for {len(unmapped_documents)} unmapped document types")
#         unmapped_doc_types = [str(doc.get("DocumentTypeID")) for doc in unmapped_documents]
#         logging.info(f"   📋 Unmapped DocumentTypeIDs: {unmapped_doc_types}")
#         apply_fallback_page_mapping(sub_document_rows, unmapped_documents, demarcated_ranges, total_pages)
#     else:
#         logging.info(f"✅ All documents successfully demarcated using identifiers - no fallback needed")

#     try:
#         if sub_document_rows:
#             xml_payload = create_subdocument_xml(sub_document_rows)
#             # Save XML payload to artifacts
#             try:
#                 xml_path = os.path.join(settings.ARTIFACTS_DIR, f"ocr_payload_{message_data.get('UploadDatasheetid')}_{timestamp}.xml")
#                 with open(xml_path, 'w', encoding='utf-8') as xf:
#                     xf.write(xml_payload)
#                 logging.info(f"Saved XML payload to artifacts: {xml_path}")
#                 manifest['XmlPath'] = xml_path
#             except Exception as e:
#                 logging.warning(f"Failed to save XML payload to artifacts: {e}")

#             logging.info(f"Calling insert_ocr_document for UploadDatasheetid={message_data.get('UploadDatasheetid')}")
#             success = insert_ocr_document(xml_payload)
#             manifest['ApiInsertSuccess'] = bool(success)
#             logging.info(f"insert_ocr_document returned: {success}")
#             if success:
#                 logging.info(f"Successfully inserted OCR data for {message_data.get('UploadDatasheetid')}")
#                 # Send message to the next queue
#                 classification_message = json.dumps({
#                     "BatchId": message_data.get("BatchId"),
#                     "UploadDatasheetid": message_data.get("UploadDatasheetid")
#                 })
#                 logging.info(f"Sending classification message to queue {settings.CLASSIFICATION_QUEUE_NAME}")
#                 queue_service.send_message(classification_message, settings.CLASSIFICATION_QUEUE_NAME)
#             else:
#                 logging.error(f"Failed to insert OCR data for {message_data.get('UploadDatasheetid')}")
#                 manifest['Errors'].append('API insert failed')
#         else:
#             manifest['Errors'].append('No sub_document_rows generated')
#             manifest['Status'] = 'failed'
#     except Exception as e:
#         tb = traceback.format_exc()
#         logging.error(f"Unhandled error during OCR processing: {e}\n{tb}")
#         manifest['Errors'].append(str(e))
#         manifest['Errors'].append(tb)
#         manifest['Status'] = 'failed'

#     # Finalize manifest
#     end_ts = datetime.now(timezone.utc)
#     manifest['EndTime'] = end_ts.isoformat()
#     manifest['DurationSeconds'] = (end_ts - start_ts).total_seconds()
#     if manifest.get('Status') != 'failed':
#         manifest['Status'] = 'succeeded' if manifest.get('ApiInsertSuccess') else manifest.get('Status', 'partial')

#     try:
#         manifest_path = os.path.join(settings.ARTIFACTS_DIR, f"manifest_{message_data.get('UploadDatasheetid')}_{timestamp}.json")
#         with open(manifest_path, 'w', encoding='utf-8') as mf:
#             json.dump(manifest, mf, indent=2)
#         logging.info(f"Saved manifest to artifacts: {manifest_path}")
#     except Exception as e:
#         logging.warning(f"Failed to write manifest: {e}")




import fitz  # PyMuPDF
import pytesseract
import logging
import re
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# Setup logger for this module
logger = logging.getLogger(__name__)

def _ocr_page_image(image: Image) -> str:
    """Performs OCR on a single image object."""
    try:
        return pytesseract.image_to_string(image, lang='eng')
    except Exception as e:
        logger.error(f"Pytesseract failed on an image: {e}")
        return ""

def get_text_from_pdf(pdf_path: str) -> list[str]:
    """Extracts text from each page of a PDF in parallel using OCR."""
    page_texts = []
    try:
        doc = fitz.open(pdf_path)
        page_texts = [""] * doc.page_count
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_page = {}
            for page_num in range(doc.page_count):
                page = doc.load_page(page_num)
                pix = page.get_pixmap(dpi=300)
                img = Image.open(BytesIO(pix.tobytes("png")))
                future = executor.submit(_ocr_page_image, img)
                future_to_page[future] = page_num

            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    text = future.result()
                    page_texts[page_num] = text
                    logger.info(f"Successfully processed page {page_num + 1}/{doc.page_count}")
                except Exception as e:
                    logger.error(f"Page {page_num + 1} generated an exception: {e}")
                    
        return page_texts
    except Exception as e:
        logger.error(f"Failed to process PDF '{pdf_path}': {e}")
        return []

def normalize_text(text: str) -> str:
    """Replaces all whitespace sequences with a single space."""
    return re.sub(r'\s+', ' ', text).strip()

def _is_page_demarcated(page_num: int, demarcated_ranges: list) -> bool:
    """Checks if a page falls within an already identified document range."""
    return any(start <= page_num <= end for start, end in demarcated_ranges)

def get_first_page(pdf_pages_text: list, identifier: str) -> int:
    """Finds the first page containing the identifier."""
    for i, page_text in enumerate(pdf_pages_text):
        if normalize_text(identifier) in normalize_text(page_text):
            return i + 1
    return -1

def get_last_page(pdf_pages_text: list, first_page: int, end_identifier: Optional[str]) -> int:
    """Finds the last page based on the end identifier or returns the last page of the doc."""
    if end_identifier:
        for i in range(first_page - 1, len(pdf_pages_text)):
            if normalize_text(end_identifier) in normalize_text(pdf_pages_text[i]):
                return i + 1
    return len(pdf_pages_text)
    
def demarcate_document(pdf_pages_text: list, identifiers: list) -> list[dict]:
    """Processes identifiers to find sub-document page ranges."""
    demarcated_ranges = []
    sub_document_rows = []
    total_pages = len(pdf_pages_text)

    for ident in sorted(identifiers, key=lambda x: int(x.get("Sequence", 999))):
        start_id = ident.get("StartingIdentifier")
        end_id = ident.get("EndingIdentifier")
        no_of_pages = int(ident.get("NoOfPages", 0))

        from_page, to_page = 0, 0
        
        if start_id:
            first_page = get_first_page(pdf_pages_text, start_id)
            if first_page != -1 and not _is_page_demarcated(first_page, demarcated_ranges):
                if no_of_pages > 0:
                    last_page = min(first_page + no_of_pages - 1, total_pages)
                else:
                    last_page = get_last_page(pdf_pages_text, first_page, end_id)
                
                if last_page != -1:
                    from_page, to_page = first_page, last_page
                    demarcated_ranges.append((from_page, to_page))

        # Create sub-document row data
        sub_doc_row = {
            "DocReceivedId": ident.get("DocReceivedId", ""),
            "FromPageNumber": from_page,
            "ToPageNumber": to_page,
            "FileNumber": ident.get("FirmFile", ""),
            "DocumentTypeId": ident.get("DocumentTypeID", ""),
            "UploadDataSheetId": ident.get("UploadDatasheetid", ""),
            "TotalNumberOfpages": total_pages,
            "NoOfPages": to_page - from_page + 1 if from_page > 0 else 0,
            "Sequence": ident.get("Sequence", ""),
            "SessionId": ident.get("SessionId", "")
        }
        
        # Log the sub-document row being added
        logger.info(f"📋 Adding sub-document row:")
        logger.info(f"   DocReceivedId: {sub_doc_row['DocReceivedId']}")
        logger.info(f"   FromPageNumber: {sub_doc_row['FromPageNumber']}")
        logger.info(f"   ToPageNumber: {sub_doc_row['ToPageNumber']}")
        logger.info(f"   FileNumber: {sub_doc_row['FileNumber']}")
        logger.info(f"   DocumentTypeId: {sub_doc_row['DocumentTypeId']}")
        logger.info(f"   UploadDataSheetId: {sub_doc_row['UploadDataSheetId']}")
        logger.info(f"   TotalNumberOfpages: {sub_doc_row['TotalNumberOfpages']}")
        logger.info(f"   NoOfPages: {sub_doc_row['NoOfPages']}")
        logger.info(f"   Sequence: {sub_doc_row['Sequence']}")
        logger.info(f"   SessionId: {sub_doc_row['SessionId']}")
        
        sub_document_rows.append(sub_doc_row)

    return sub_document_rows
