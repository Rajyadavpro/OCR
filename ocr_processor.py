import fitz  # PyMuPDF
import pytesseract
import logging
import re
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from typing import Optional, List, Dict

# Setup logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ---------------- OCR PART ----------------
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


def normalize_text(text: str) -> str:
    """Replaces all whitespace sequences with a single space."""
    return re.sub(r"\s+", " ", text).strip()


def _is_page_demarcated(page_num: int, demarcated_ranges: list) -> bool:
    """Checks if a page falls within an already identified document range."""
    return any(start <= page_num <= end for start, end in demarcated_ranges)


# ---------------- DEMARCATION PART ----------------
def get_first_page(pdf_pages_text: List[str], identifier: str, identifier_plus1: str, exact_match: bool, occurrence: int) -> int:
    """Finds the first page index (1-based) containing the identifier, adjusted by identifier_plus1 if present."""
    normalized_identifier = normalize_text(identifier)
    seen = 0
    initial_start_page_idx = -1

    # Find the initial start page based on the primary identifier
    for i, page_text in enumerate(pdf_pages_text):
        normalized_page = normalize_text(page_text)
        if exact_match:
            matches = re.findall(rf"\b{re.escape(normalized_identifier)}\b", normalized_page)
        else:
            matches = [normalized_identifier] if normalized_identifier in normalized_page else []

        if matches:
            seen += len(matches)
            if seen >= occurrence:
                initial_start_page_idx = i
                break
    
    if initial_start_page_idx == -1:
        return -1

    # --- NEW LOGIC for StartingIdentifierPlus1 ---
    # If identifier_plus1 is provided, search for it starting from the page where the primary identifier was found.
    # The page where identifier_plus1 is found becomes the new start page.
    if identifier_plus1:
        normalized_plus1 = normalize_text(identifier_plus1)
        for i in range(initial_start_page_idx, len(pdf_pages_text)):
            normalized_page = normalize_text(pdf_pages_text[i])
            if (exact_match and re.search(rf"\b{re.escape(normalized_plus1)}\b", normalized_page)) or \
               (not exact_match and normalized_plus1 in normalized_page):
                return i + 1  # Return the 1-based page number where identifier_plus1 was found
    # --- END NEW LOGIC ---

    return initial_start_page_idx + 1


def get_last_page(pdf_pages_text: List[str], first_page: int, end_identifier: Optional[str], end_identifier_minus1: Optional[str], exact_match: bool) -> int:
    """Finds the last page index (1-based) for the document, adjusted by end_identifier_minus1 if present."""
    potential_end_page_idx = len(pdf_pages_text) - 1

    # Find the potential end page based on the primary end identifier
    if end_identifier:
        normalized_identifier = normalize_text(end_identifier)
        for i in range(first_page - 1, len(pdf_pages_text)):
            normalized_page = normalize_text(pdf_pages_text[i])
            if (exact_match and re.search(rf"\b{re.escape(normalized_identifier)}\b", normalized_page)) or \
               (not exact_match and normalized_identifier in normalized_page):
                potential_end_page_idx = i
                break
    
    # --- NEW LOGIC for EndingIdentifierMinus1 ---
    # If end_identifier_minus1 is provided, search backwards from the potential_end_page_idx
    # to find it. The page where it's found becomes the new end page.
    if end_identifier_minus1:
        normalized_minus1 = normalize_text(end_identifier_minus1)
        # Search backwards from the potential end page down to the start page
        for i in range(potential_end_page_idx, first_page - 2, -1):
            normalized_page = normalize_text(pdf_pages_text[i])
            if (exact_match and re.search(rf"\b{re.escape(normalized_minus1)}\b", normalized_page)) or \
               (not exact_match and normalized_minus1 in normalized_page):
                return i + 1 # Return the 1-based page number where end_identifier_minus1 was found
    # --- END NEW LOGIC ---

    return potential_end_page_idx + 1


def demarcate_document(pdf_pages_text: List[str], identifiers: List[Dict]) -> List[Dict]:
    """Processes identifiers to find sub-document page ranges with full rules."""
    demarcated_ranges = []
    sub_document_rows = []
    total_pages = len(pdf_pages_text)

    for ident in sorted(identifiers, key=lambda x: int(x.get("Sequence", 999))):
        start_id = ident.get("StartingIdentifier", "").strip()
        # --- ADDED: Get Plus1 and Minus1 identifiers ---
        start_id_plus1 = ident.get("StartingIdentifierPlus1", "").strip()
        end_id = ident.get("EndingIdentifier", "").strip()
        end_id_minus1 = ident.get("EndingIdentifierMinus1", "").strip()
        # --- END ADDED ---
        
        no_of_pages = int(ident.get("NoOfPages", 0))
        occurrence = int(ident.get("Occurence", 1))
        
        start_minus_n_val = ident.get("StartingMinusN", "0").strip()
        start_minus_n = int(start_minus_n_val) if start_minus_n_val and start_minus_n_val != " " else 0
        
        end_minus_n_val = ident.get("EndingMinusN", "0").strip()
        end_minus_n = int(end_minus_n_val) if end_minus_n_val and end_minus_n_val != " " else 0
        exact_match = ident.get("StartingIdentifier", "").startswith("ExactMatch:")

        if exact_match:
            start_id = start_id.replace("ExactMatch:", "", 1).strip()

        from_page, to_page = 0, 0

        if start_id:
            # --- MODIFIED: Pass new identifiers to functions ---
            first_page = get_first_page(pdf_pages_text, start_id, start_id_plus1, exact_match, occurrence)
            if first_page != -1:
                from_page = max(1, first_page - start_minus_n)

                if no_of_pages > 0:
                    to_page = min(from_page + no_of_pages - 1, total_pages)
                else:
                    last_page = get_last_page(pdf_pages_text, first_page, end_id, end_id_minus1, exact_match)
                    to_page = max(from_page, last_page - end_minus_n)
            # --- END MODIFIED ---

                # Avoid overlaps (simple check, might need more sophisticated logic depending on requirements)
                if not _is_page_demarcated(from_page, demarcated_ranges):
                    demarcated_ranges.append((from_page, to_page))

        # Build sub-document row
        sub_doc_row = {
            "DocReceivedId": ident.get("DocReceivedId"),
            "FromPageNumber": from_page,
            "ToPageNumber": to_page,
            "FileNumber": ident.get("FirmFile"),
            "DocumentTypeId": ident.get("DocumentTypeID"),
            "UploadDataSheetId": ident.get("UploadDatasheetid"),
            "TotalNumberOfpages": total_pages,
            "NoOfPages": to_page - from_page + 1 if from_page > 0 else 0,
            "Sequence": ident.get("Sequence"),
            "SessionId": ident.get("SessionId")
        }

        logger.info(f"📋 Added sub-document row: {sub_doc_row}")
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