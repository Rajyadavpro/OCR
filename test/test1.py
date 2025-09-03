import os
import re
import io
import json
import fitz  # PyMuPDF
import pytesseract
import requests
import logging
from PIL import Image
from typing import List, Tuple, Dict

from dotenv import load_dotenv
from azure.storage.queue import QueueClient

# ---------------- Load environment ----------------
load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", "artifacts")
INPUT_QUEUE_NAME = os.getenv("INPUT_QUEUE_NAME")
CLASSIFICATION_QUEUE_NAME = os.getenv("CLASSIFICATION_QUEUE_NAME")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# ---------------- Utility functions ----------------

def NormalizeText(text: str) -> str:
    return text.strip().lower()


def IsPageDemarcated(Page: int, DemarcatedRanges: List[Tuple[int, int]]) -> bool:
    for start, end in DemarcatedRanges:
        if start <= Page <= end:
            return True
    return False


def IsPageRangeOverlapping(FirstPage: int, LastPage: int, DemarcatedRanges: List[Tuple[int, int]]) -> bool:
    for start, end in DemarcatedRanges:
        if not (LastPage < start or FirstPage > end):
            return True
    return False


def CountOccurrences(text: str, substring: str) -> int:
    return len(re.findall(re.escape(substring), text, re.IGNORECASE))


# ---------------- Identifier search logic ----------------

def GetFirstPageFromIdentifiers(Pages: List[str], StartingIdentifier: str,
                                StartingIdentifierPlus1: str, Occurence: int,
                                StartingMinusN: int, DemarcatedRanges: List[Tuple[int, int]],
                                TotalPages: int) -> int:
    PageFound = -1
    OccurrenceCount = 0

    for i, page in enumerate(Pages):
        if IsPageDemarcated(i + 1, DemarcatedRanges):
            continue

        norm_text = NormalizeText(page)
        if NormalizeText(StartingIdentifier) in norm_text:
            OccurrenceCount += 1
            if OccurrenceCount == Occurence:
                PageFound = i + 1 - StartingMinusN
                break

    if PageFound == -1 and StartingIdentifierPlus1:
        for i, page in enumerate(Pages):
            if IsPageDemarcated(i + 1, DemarcatedRanges):
                continue
            if NormalizeText(StartingIdentifierPlus1) in NormalizeText(page):
                PageFound = i + 1 - StartingMinusN
                break

    return max(1, PageFound)


def GetLastPageFromIdentifiers(Pages: List[str], EndingIdentifier: str,
                               EndingIdentifierMinus1: str, FirstPage: int,
                               NoOfPages: int, StartingMinusN: int,
                               DemarcatedRanges: List[Tuple[int, int]],
                               TotalPages: int) -> int:
    PageFound = -1

    if NoOfPages > 0:
        PageFound = min(TotalPages, FirstPage + NoOfPages - 1)

    else:
        for i in range(FirstPage, len(Pages) + 1):
            if IsPageDemarcated(i, DemarcatedRanges):
                continue
            if NormalizeText(EndingIdentifier) in NormalizeText(Pages[i - 1]):
                PageFound = i + StartingMinusN
                break

        if PageFound == -1 and EndingIdentifierMinus1:
            for i in range(FirstPage, len(Pages) + 1):
                if IsPageDemarcated(i, DemarcatedRanges):
                    continue
                if NormalizeText(EndingIdentifierMinus1) in NormalizeText(Pages[i - 1]):
                    PageFound = i - 1
                    break

    return min(TotalPages, PageFound if PageFound != -1 else TotalPages)


# ---------------- Queue handling ----------------

def AddToQueue(msg: dict, ConnStr: str, QueueName: str):
    queue_client = QueueClient.from_connection_string(ConnStr, QueueName)
    queue_client.send_message(json.dumps(msg))
    logger.info(f"✅ Message pushed to queue {QueueName}")


def InsertOcrDocument(XmlStr: str, ConnStr: str, QueueName: str):
    msg = {"ocrResult": XmlStr}
    AddToQueue(msg, ConnStr, QueueName)


def ReadFromQueue(ConnStr: str, QueueName: str, max_messages: int = 1):
    queue_client = QueueClient.from_connection_string(ConnStr, QueueName)
    messages = queue_client.receive_messages(messages_per_page=max_messages)
    for msg in messages:
        logger.info(f"📥 Received message from {QueueName}")
        queue_client.delete_message(msg)  # remove after reading
        return json.loads(msg.content)
    return None


# ---------------- OCR Processing ----------------

def DownloadPDF(url: str, save_path: str) -> str:
    logger.info(f"⬇️ Downloading PDF from {url}")
    response = requests.get(url)
    response.raise_for_status()
    with open(save_path, "wb") as f:
        f.write(response.content)
    return save_path


def GetTextFromDocument(PdfPath: str) -> List[str]:
    """Extract text from PDF pages using PyMuPDF + pytesseract OCR."""
    doc = fitz.open(PdfPath)
    pages_text = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        pix = page.get_pixmap()
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        text = pytesseract.image_to_string(img)
        pages_text.append(text)

    return pages_text


def ProcessPDFOCR(PdfPath: str, Identifiers: List[Dict],
                  StorageConnectionString: str, OutputQueueName: str):
    """Main driver to process PDF via OCR and send to queue."""

    PdfPagesText = GetTextFromDocument(PdfPath)
    TotalPages = len(PdfPagesText)
    DemarcatedRanges: List[Tuple[int, int]] = []

    for identifier in Identifiers:
        FirstPage = GetFirstPageFromIdentifiers(
            PdfPagesText,
            identifier.get("StartingIdentifier", ""),
            identifier.get("StartingIdentifierPlus1", ""),
            int(identifier.get("Occurence", 1)),
            int(identifier.get("StartingMinusN", 0)),
            DemarcatedRanges,
            TotalPages
        )

        LastPage = GetLastPageFromIdentifiers(
            PdfPagesText,
            identifier.get("EndingIdentifier", ""),
            identifier.get("EndingIdentifierMinus1", ""),
            FirstPage,
            int(identifier.get("NoOfPages", 0)),
            int(identifier.get("StartingMinusN", 0)),
            DemarcatedRanges,
            TotalPages
        )

        if not IsPageRangeOverlapping(FirstPage, LastPage, DemarcatedRanges):
            DemarcatedRanges.append((FirstPage, LastPage))
            SubDocumentRow = {
                "DocumentTypeID": identifier.get("DocumentTypeID"),
                "DocumentTypeName": identifier.get("DocumentTypeName"),
                "FromPageNumber": FirstPage,
                "ToPageNumber": LastPage
            }
            InsertOcrDocument(json.dumps(SubDocumentRow), StorageConnectionString, OutputQueueName)


# ---------------- Main Entry ----------------

if __name__ == "__main__":
    logger.info("🚀 Starting OCR Pipeline with Azure Queue")

    # Step 1: Read message from input queue
    msg = ReadFromQueue(AZURE_STORAGE_CONNECTION_STRING, INPUT_QUEUE_NAME)
    if not msg:
        logger.info("⚠️ No messages in input queue")
        exit(0)

    file_url = msg.get("FilePath")
    identifiers = msg.get("Identifiers", [])  # expected in JSON

    # Step 2: Download PDF
    pdf_path = os.path.join(ARTIFACTS_DIR, "input.pdf")
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    DownloadPDF(file_url, pdf_path)

    # Step 3: Process PDF with OCR + demarcation
    ProcessPDFOCR(pdf_path, identifiers, AZURE_STORAGE_CONNECTION_STRING, CLASSIFICATION_QUEUE_NAME)

    logger.info("✅ OCR Processing complete")
