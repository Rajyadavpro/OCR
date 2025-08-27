import logging
import traceback
from typing import List, Dict, Any
from lxml import etree


def create_subdocument_xml(rows: List[Dict[str, Any]]) -> str:
    """Creates the XML payload from a list of sub-document row data.

    Logs the start and end of XML generation and returns the XML string.
    Returns an empty string on failure.
    """
    try:
        logging.info(f"Generating XML for {len(rows)} sub-document rows")

        root = etree.Element("SubDocumentDetails")

        for row_data in rows:
            row_element = etree.SubElement(root, "SubDocumentRow")
            for key, value in row_data.items():
                child = etree.SubElement(row_element, key)
                child.text = str(value if value is not None else "")

        # Convert the XML tree to a string
        xml_string = etree.tostring(root, pretty_print=True, encoding='unicode')
        logging.info(f"Generated XML payload length: {len(xml_string)} characters")
        return xml_string

    except Exception as e:
        logging.error(f"Failed to generate XML payload: {e}")
        logging.debug(traceback.format_exc())
        return ""