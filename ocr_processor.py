# import fitz  # PyMuPDF
# import pytesseract
# import logging
# import re
# from PIL import Image
# from io import BytesIO
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from typing import Optional
# from typing import Optional, List, Dict

# # Use the application's logging configuration
# logger = logging.getLogger(__name__)


# # ---------------- OCR PART ----------------
# def _ocr_page_image(image: Image) -> str:
#     """Performs OCR on a single image object."""
#     try:
#         return pytesseract.image_to_string(image, lang="eng")
#     except Exception as e:
#         logger.error(f"Pytesseract failed on an image: {e}")
#         return ""


# def get_text_from_pdf(pdf_path: str) -> list[str]:
#     """Extracts text from each page of a PDF in parallel using OCR."""
#     page_texts = []
#     try:
#         doc = fitz.open(pdf_path)
#         page_texts = [""] * doc.page_count

#         with ThreadPoolExecutor(max_workers=5) as executor:
#             future_to_page = {}
#             for page_num in range(doc.page_count):
#                 page = doc.load_page(page_num)
#                 pix = page.get_pixmap(dpi=300)
#                 img = Image.open(BytesIO(pix.tobytes("png")))
#                 future = executor.submit(_ocr_page_image, img)
#                 future_to_page[future] = page_num

#             for future in as_completed(future_to_page):
#                 page_num = future_to_page[future]
#                 try:
#                     text = future.result()
#                     page_texts[page_num] = text
#                     logger.info(f"✅ Processed page {page_num + 1}/{doc.page_count}")
#                 except Exception as e:
#                     logger.error(f"Page {page_num + 1} failed: {e}")

#         return page_texts
#     except Exception as e:
#         logger.error(f"Failed to process PDF '{pdf_path}': {e}")
#         return []


# def normalize_text(text: str) -> str:
#     """Replaces all whitespace sequences with a single space."""
#     return re.sub(r"\s+", " ", text).strip()


# def _is_page_demarcated(page_num: int, demarcated_ranges: list) -> bool:
#     """Checks if a page falls within an already identified document range."""
#     return any(start <= page_num <= end for start, end in demarcated_ranges)


# # ---------------- DEMARCATION PART ----------------
# def get_first_page(pdf_pages_text: List[str], identifier: str, identifier_plus1: str, exact_match: bool, occurrence: int) -> int:
#     """Finds the first page index (1-based) containing the identifier, adjusted by identifier_plus1 if present."""
#     normalized_identifier = normalize_text(identifier)
#     seen = 0
#     initial_start_page_idx = -1

#     # Find the initial start page based on the primary identifier
#     for i, page_text in enumerate(pdf_pages_text):
#         normalized_page = normalize_text(page_text)
#         if exact_match:
#             matches = re.findall(rf"\b{re.escape(normalized_identifier)}\b", normalized_page)
#         else:
#             matches = [normalized_identifier] if normalized_identifier in normalized_page else []

#         if matches:
#             seen += len(matches)
#             if seen >= occurrence:
#                 initial_start_page_idx = i
#                 break
    
#     if initial_start_page_idx == -1:
#         return -1

#     # --- NEW LOGIC for StartingIdentifierPlus1 ---
#     # If identifier_plus1 is provided, search for it starting from the page where the primary identifier was found.
#     # The page where identifier_plus1 is found becomes the new start page.
#     if identifier_plus1:
#         normalized_plus1 = normalize_text(identifier_plus1)
#         for i in range(initial_start_page_idx, len(pdf_pages_text)):
#             normalized_page = normalize_text(pdf_pages_text[i])
#             if (exact_match and re.search(rf"\b{re.escape(normalized_plus1)}\b", normalized_page)) or \
#                (not exact_match and normalized_plus1 in normalized_page):
#                 return i + 1  # Return the 1-based page number where identifier_plus1 was found
#     # --- END NEW LOGIC ---

#     return initial_start_page_idx + 1


# def get_last_page(pdf_pages_text: List[str], first_page: int, end_identifier: Optional[str], end_identifier_minus1: Optional[str], exact_match: bool) -> int:
#     """Finds the last page index (1-based) for the document, adjusted by end_identifier_minus1 if present."""
#     potential_end_page_idx = len(pdf_pages_text) - 1

#     # Find the potential end page based on the primary end identifier
#     if end_identifier:
#         normalized_identifier = normalize_text(end_identifier)
#         for i in range(first_page - 1, len(pdf_pages_text)):
#             normalized_page = normalize_text(pdf_pages_text[i])
#             if (exact_match and re.search(rf"\b{re.escape(normalized_identifier)}\b", normalized_page)) or \
#                (not exact_match and normalized_identifier in normalized_page):
#                 potential_end_page_idx = i
#                 break
    
#     # --- NEW LOGIC for EndingIdentifierMinus1 ---
#     # If end_identifier_minus1 is provided, search backwards from the potential_end_page_idx
#     # to find it. The page where it's found becomes the new end page.
#     if end_identifier_minus1:
#         normalized_minus1 = normalize_text(end_identifier_minus1)
#         # Search backwards from the potential end page down to the start page
#         for i in range(potential_end_page_idx, first_page - 2, -1):
#             normalized_page = normalize_text(pdf_pages_text[i])
#             if (exact_match and re.search(rf"\b{re.escape(normalized_minus1)}\b", normalized_page)) or \
#                (not exact_match and normalized_minus1 in normalized_page):
#                 return i # Return the 1-based page number where end_identifier_minus1 was found
#     # --- END NEW LOGIC ---

#     return potential_end_page_idx + 1


# def demarcate_document(pdf_pages_text: List[str], identifiers: List[Dict]) -> List[Dict]:
#     """Processes identifiers to find sub-document page ranges with full rules."""
#     demarcated_ranges = []
#     sub_document_rows = []
#     total_pages = len(pdf_pages_text)

#     for ident in sorted(identifiers, key=lambda x: int(x.get("Sequence", 999))):
#         start_id = ident.get("StartingIdentifier", "").strip()
#         # --- ADDED: Get Plus1 and Minus1 identifiers ---
#         start_id_plus1 = ident.get("StartingIdentifierPlus1", "").strip()
#         end_id = ident.get("EndingIdentifier", "").strip()
#         end_id_minus1 = ident.get("EndingIdentifierMinus1", "").strip()
#         # --- END ADDED ---
        
#         no_of_pages = int(ident.get("NoOfPages", 0))
#         occurrence = int(ident.get("Occurence", 1))
        
#         start_minus_n_val = ident.get("StartingMinusN", "0").strip()
#         start_minus_n = int(start_minus_n_val) if start_minus_n_val and start_minus_n_val != " " else 0
        
#         end_minus_n_val = ident.get("EndingMinusN", "0").strip()
#         end_minus_n = int(end_minus_n_val) if end_minus_n_val and end_minus_n_val != " " else 0
#         exact_match = ident.get("StartingIdentifier", "").startswith("ExactMatch:")

#         if exact_match:
#             start_id = start_id.replace("ExactMatch:", "", 1).strip()

#         from_page, to_page = 0, 0

#         if start_id:
#             # --- MODIFIED: Pass new identifiers to functions ---
#             first_page = get_first_page(pdf_pages_text, start_id, start_id_plus1, exact_match, occurrence)
#             if first_page != -1:
#                 from_page = max(1, first_page - start_minus_n)

#                 if no_of_pages > 0:
#                     to_page = min(from_page + no_of_pages - 1, total_pages)
#                 else:
#                     last_page = get_last_page(pdf_pages_text, first_page, end_id, end_id_minus1, exact_match)
#                     to_page = max(from_page, last_page - end_minus_n)
#             # --- END MODIFIED ---

#                 # Avoid overlaps (simple check, might need more sophisticated logic depending on requirements)
#                 if not _is_page_demarcated(from_page, demarcated_ranges):
#                     demarcated_ranges.append((from_page, to_page))

#         # Build sub-document row
#         sub_doc_row = {
#             "DocReceivedId": ident.get("DocReceivedId"),
#             "FromPageNumber": from_page,
#             "ToPageNumber": to_page,
#             "FileNumber": ident.get("FirmFile"),
#             "DocumentTypeId": ident.get("DocumentTypeID"),
#             "UploadDataSheetId": ident.get("UploadDatasheetid"),
#             "TotalNumberOfpages": total_pages,
#             "NoOfPages": to_page - from_page + 1 if from_page > 0 else 0,
#             "Sequence": ident.get("Sequence"),
#             "SessionId": ident.get("SessionId")
#         }

#         logger.info(f"📋 Added sub-document row: {sub_doc_row}")
#         sub_document_rows.append(sub_doc_row)

#     return sub_document_rows

# # ---------------- MAIN PROCESS ----------------
# def process_pdf(pdf_path: str, identifiers: list) -> list[dict]:
#     """Full pipeline: OCR + demarcation"""
#     logger.info(f"🚀 Starting OCR for {pdf_path}")
#     pdf_pages_text = get_text_from_pdf(pdf_path)

#     if not pdf_pages_text:
#         logger.warning("No OCR text extracted, returning empty result.")
#         return []

#     logger.info("📑 Starting demarcation process")
#     results = demarcate_document(pdf_pages_text, identifiers)

#     logger.info(f"✅ Completed demarcation: {len(results)} sub-documents found")
#     return results



import fitz  # PyMuPDF
import pytesseract
import logging
import re
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Tuple

# Use the application's logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ---------------- OCR PART (Unchanged) ----------------
def _ocr_page_image(image: Image) -> str:
    """Performs OCR on a single image object."""
    try:
        return pytesseract.image_to_string(image, lang="eng")
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
                    logger.info(f"✅ Processed page {page_num + 1}/{doc.page_count}")
                except Exception as e:
                    logger.error(f"Page {page_num + 1} failed: {e}")

        return page_texts
    except Exception as e:
        logger.error(f"Failed to process PDF '{pdf_path}': {e}")
        return []

# ---------------- HELPER FUNCTIONS (To match C# logic) ----------------
def normalize_text(text: str) -> str:
    """Replaces all whitespace sequences with a single space."""
    return re.sub(r"\s+", " ", text).strip()

def _is_exact_match(identifier: str) -> bool:
    """Helper to check for 'ExactMatch:' prefix, like C#'s IsContainsOnly."""
    return identifier.strip().lower().startswith("exactmatch:")

def _clean_exact_match_identifier(identifier: str) -> str:
    """Helper to remove 'ExactMatch:' prefix."""
    return re.sub(r"^ExactMatch:", "", identifier.strip(), flags=re.IGNORECASE).strip()

def _is_page_contains_only(page_text: str, identifier: str) -> bool:
    """Checks if the normalized page text is an exact match to the identifier."""
    return page_text.lower() == identifier.lower()

def count_occurrences(page_text: str, identifier: str) -> int:
    """Counts non-overlapping occurrences of an identifier in text, case-insensitive."""
    if not page_text or not identifier:
        return 0
    return len(re.findall(re.escape(identifier), page_text, re.IGNORECASE))

def _is_page_demarcated(page_num: int, demarcated_ranges: List[Tuple[int, int]]) -> bool:
    """Checks if a page falls within any already identified document range."""
    return any(start <= page_num <= end for start, end in demarcated_ranges)

def _is_range_overlapping(new_range: Tuple[int, int], demarcated_ranges: List[Tuple[int, int]]) -> bool:
    """
    Checks if the new page range overlaps with any existing demarcated ranges.
    This is the direct Python equivalent of the C# IsPageRangeOverlapping logic.
    """
    new_start, new_end = new_range
    # A zero-page range cannot overlap
    if new_start == 0 or new_end == 0:
        return False
        
    for start, end in demarcated_ranges:
        # Classic interval overlap check
        if new_start <= end and new_end >= start:
            logger.warning(f"Overlap detected for range {new_range}. It overlaps with existing range {(start, end)}.")
            return True
    return False


# ---------------- REWRITTEN DEMARCATION LOGIC (To match C#) ----------------

def get_first_page(
    pdf_pages_text: List[str],
    start_id: str,
    start_id_plus1: str,
    occurrence: int,
    start_offset: int,
    demarcated_ranges: List[Tuple[int, int]]
) -> int:
    """
    Finds the first page based on C# logic.
    - If start_id_plus1 is present, it's the primary search key, and start_offset is ADDED.
    - Otherwise, start_id is the key, and no offset is applied.
    """
    occurrence_counter = 0

    # Branch 1: Logic for StartingIdentifierPlus1 (alternateIdentifiers in C#)
    if start_id_plus1:
        plus1_identifiers = [normalize_text(i) for i in start_id_plus1.split('|') if i.strip()]
        for page_num, page_text in enumerate(pdf_pages_text):
            
            # C# checks demarcation on the *potential* resulting page
            potential_page = page_num + 1 + start_offset
            if _is_page_demarcated(potential_page, demarcated_ranges):
                continue
                
            normalized_page = normalize_text(page_text)
            for identifier in plus1_identifiers:
                if _is_exact_match(identifier):
                    clean_id = _clean_exact_match_identifier(identifier)
                    if _is_page_contains_only(normalized_page, clean_id):
                        occurrence_counter += 1
                elif identifier.lower() in normalized_page.lower():
                    occurrence_counter += count_occurrences(normalized_page, identifier)
            
            if occurrence_counter >= occurrence:
                return potential_page # Return page number + offset

    # Branch 2: Logic for StartingIdentifier (primary identifiers in C#)
    elif start_id:
        start_identifiers = [normalize_text(i) for i in start_id.split('|') if i.strip()]
        for page_num, page_text in enumerate(pdf_pages_text):

            if _is_page_demarcated(page_num + 1, demarcated_ranges):
                continue
                
            normalized_page = normalize_text(page_text)
            for identifier in start_identifiers:
                if _is_exact_match(identifier):
                    clean_id = _clean_exact_match_identifier(identifier)
                    if _is_page_contains_only(normalized_page, clean_id):
                        occurrence_counter += 1
                elif identifier.lower() in normalized_page.lower():
                    occurrence_counter += count_occurrences(normalized_page, identifier)
            
            if occurrence_counter >= occurrence:
                return page_num + 1 # Return 1-based page number
    
    return -1 # Not found

def get_last_page(
    pdf_pages_text: List[str],
    first_page: int,
    end_id: str,
    end_id_minus1: str,
    end_offset: int
) -> int:
    """
    Finds the last page based on C# logic.
    - Searches FORWARD from first_page.
    - If end_id_minus1 is found, end_offset is SUBTRACTED from that page number.
    - If only end_id is found, that page number is the end page.
    - If no identifiers, it's the last page of the PDF.
    """
    total_pages = len(pdf_pages_text)

    # Branch 1: Logic for EndingIdentifierMinus1 (alternateIdentifiers in C#)
    if end_id_minus1:
        minus1_identifiers = [normalize_text(i) for i in end_id_minus1.split('|') if i.strip()]
        # Search forward from the start page
        for page_num in range(first_page - 1, total_pages):
            normalized_page = normalize_text(pdf_pages_text[page_num])
            for identifier in minus1_identifiers:
                found = False
                if _is_exact_match(identifier):
                    if _is_page_contains_only(normalized_page, _clean_exact_match_identifier(identifier)):
                        found = True
                elif identifier.lower() in normalized_page.lower():
                    found = True
                
                if found:
                    potential_last_page = (page_num + 1) - end_offset
                    # C# includes a sanity check
                    if first_page <= potential_last_page:
                        return potential_last_page
                    else:
                        return -1 # Invalid range

    # Branch 2: Logic for EndingIdentifier (primary identifiers in C#)
    elif end_id:
        end_identifiers = [normalize_text(i) for i in end_id.split('|') if i.strip()]
        # Search forward from the start page
        for page_num in range(first_page - 1, total_pages):
            normalized_page = normalize_text(pdf_pages_text[page_num])
            for identifier in end_identifiers:
                found = False
                if _is_exact_match(identifier):
                    if _is_page_contains_only(normalized_page, _clean_exact_match_identifier(identifier)):
                        found = True
                elif identifier.lower() in normalized_page.lower():
                    found = True

                if found:
                    return page_num + 1
    
    # Branch 3: No ending identifier provided
    else:
        return total_pages

    return -1 # Not found

def demarcate_document(pdf_pages_text: List[str], identifiers: List[Dict]) -> List[Dict]:
    """Processes identifiers to find sub-document page ranges with full C# rules."""
    demarcated_ranges = []
    sub_document_rows = []
    total_pages = len(pdf_pages_text)

    # Sort by sequence to process in the correct order
    for ident in sorted(identifiers, key=lambda x: int(x.get("Sequence", 999))):
        start_id = ident.get("StartingIdentifier", "").strip()
        start_id_plus1 = ident.get("StartingIdentifierPlus1", "").strip()
        end_id = ident.get("EndingIdentifier", "").strip()
        end_id_minus1 = ident.get("EndingIdentifierMinus1", "").strip()
        
        no_of_pages = int(ident.get("NoOfPages", 0))
        occurrence = int(ident.get("Occurence", 1))
        if occurrence == 0: occurrence = 1
        
        # In C# code, this is a POSITIVE offset for start, and NEGATIVE for end
        start_offset = int(ident.get("StartingMinusN", "0").strip() or 0)
        end_offset = int(ident.get("EndingMinusN", "0").strip() or 0)
        
        from_page, to_page = 0, 0
        first_page = -1

        # C# implies that if no start identifiers are provided, it starts on page 1
        if not start_id and not start_id_plus1:
            first_page = 1
        else:
            first_page = get_first_page(pdf_pages_text, start_id, start_id_plus1, occurrence, start_offset, demarcated_ranges)

        if first_page > 0:
            from_page = first_page
            
            # Rule 1: Fixed number of pages has highest priority for end page
            if no_of_pages > 0:
                to_page = min(from_page + no_of_pages - 1, total_pages)
            else:
                # Rule 2: Find end page using identifiers
                to_page = get_last_page(pdf_pages_text, from_page, end_id, end_id_minus1, end_offset)
            
            # Final validation and overlap check
            if to_page > 0 and to_page >= from_page:
                if not _is_range_overlapping((from_page, to_page), demarcated_ranges):
                    demarcated_ranges.append((from_page, to_page))
                else:
                    # If overlap, invalidate this document as per C# logic
                    from_page, to_page = 0, 0
            else:
                # If last page wasn't found or created invalid range
                from_page, to_page = 0, 0

        # Build sub-document row, even if unsuccessful (pages will be 0)
        sub_doc_row = {
            "DocReceivedId": ident.get("DocReceivedId"),
            "FromPageNumber": from_page,
            "ToPageNumber": to_page,
            "FileNumber": ident.get("FirmFile"),
            "DocumentTypeId": ident.get("DocumentTypeID"),
            "UploadDataSheetId": ident.get("UploadDatasheetid"),
            "TotalNumberOfpages": total_pages,
            "NoOfPages": to_page - from_page + 1 if from_page > 0 and to_page > 0 else 0,
            "Sequence": ident.get("Sequence"),
            "SessionId": ident.get("SessionId")
        }

        logger.info(f"📋 Demarcation result for Sequence {ident.get('Sequence')}: Pages {from_page}-{to_page}")
        sub_document_rows.append(sub_doc_row)

    return sub_document_rows

# ---------------- MAIN PROCESS ----------------
def process_pdf(pdf_path: str, identifiers: list) -> list[dict]:
    """Full pipeline: OCR + demarcation"""
    logger.info(f"🚀 Starting OCR for {pdf_path}")
    pdf_pages_text = get_text_from_pdf(pdf_path)

    if not pdf_pages_text:
        logger.warning("No OCR text extracted, returning empty result.")
        return []

    logger.info("📑 Starting demarcation process")
    results = demarcate_document(pdf_pages_text, identifiers)

    logger.info(f"✅ Completed demarcation: {len(results)} sub-documents found")
    return results