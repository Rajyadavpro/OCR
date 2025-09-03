import re
from typing import List, Tuple, Dict, Any

#region Helper Functions

def normalize_text(text: str) -> str:
    """Replaces tabs, line breaks, and multiple spaces with a single space."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

def is_page_demarcated(page: int, demarcated_page_ranges: List[Tuple[int, int]]) -> bool:
    """Checks if a page is already part of a demarcated range."""
    return any(start <= page <= end for start, end in demarcated_page_ranges)

def is_page_range_overlapping(new_first: int, new_last: int, demarcated_page_ranges: List[Tuple[int, int]]) -> bool:
    """Checks if a new page range overlaps with existing ones."""
    return any(new_first <= end and new_last >= start for start, end in demarcated_page_ranges)

def is_contains_only(identifier: str) -> bool:
    """Checks if an identifier is an 'ExactMatch' condition."""
    return identifier.lower().startswith("exactmatch:")

def clean_contains_only(identifier: str) -> str:
    """Removes the 'ExactMatch:' prefix and trims whitespace."""
    return identifier[len("exactmatch:"):].strip()

def is_page_contains_only(page_text: str, identifier: str) -> bool:
    """Checks if a page's text is an exact match to the identifier after cleaning."""
    cleaned_page_text = page_text.replace("\n", "").replace("\r", "").strip()
    return cleaned_page_text.lower() == identifier.lower()

def count_occurrences(page_text: str, identifier: str) -> int:
    """Counts non-overlapping occurrences of an identifier in text, case-insensitively."""
    if not page_text or not identifier:
        return 0
    # Use regex findall for accurate, non-overlapping counting
    return len(re.findall(re.escape(identifier), page_text, re.IGNORECASE))

#endregion

def get_first_page_from_identifiers(
    pdf_pages_text: List[str],
    identifiers: str,
    alternate_identifiers: str,
    occurrence: int,
    starting_plus_n: int,
    demarcated_page_ranges: List[Tuple[int, int]]
) -> int:
    """
    Finds the starting page of a sub-document based on identifiers.
    Returns a 1-based page number or -1 if not found.
    """
    pdf_page_count = len(pdf_pages_text)
    identifier_list = [i.strip() for i in identifiers.split('|') if i.strip()]
    alternate_identifier_list = [i.strip() for i in (alternate_identifiers or "").split('|') if i.strip()]

    occurrence = occurrence if occurrence > 0 else 1
    occurrence_counter = 0

    if alternate_identifier_list:
        # This logic mirrors the C# code's adjustment for StartingMinusN/StartingPlusN
        starting_plus_n = max(2, starting_plus_n)
        
        for page_num in range(pdf_page_count):
            # Check if the *target* page is already demarcated
            if is_page_demarcated(page_num + starting_plus_n, demarcated_page_ranges):
                continue
            
            normalized_page_text = normalize_text(pdf_pages_text[page_num])
            
            for identifier in alternate_identifier_list:
                occurrences_found = 0
                if is_contains_only(identifier):
                    clean_id = clean_contains_only(identifier)
                    if is_page_contains_only(normalized_page_text, clean_id):
                        occurrences_found = 1
                elif identifier.lower() in normalized_page_text.lower():
                    occurrences_found = count_occurrences(normalized_page_text, identifier)
                
                if occurrences_found > 0:
                    occurrence_counter += occurrences_found
                    if occurrence_counter >= occurrence:
                        return page_num + starting_plus_n
    elif identifier_list:
        for page_num in range(pdf_page_count):
            if is_page_demarcated(page_num + 1, demarcated_page_ranges):
                continue

            normalized_page_text = normalize_text(pdf_pages_text[page_num])

            for identifier in identifier_list:
                occurrences_found = 0
                if is_contains_only(identifier):
                    clean_id = clean_contains_only(identifier)
                    if is_page_contains_only(normalized_page_text, clean_id):
                        occurrences_found = 1
                elif identifier.lower() in normalized_page_text.lower():
                    occurrences_found = count_occurrences(normalized_page_text, identifier)

                if occurrences_found > 0:
                    occurrence_counter += occurrences_found
                    if occurrence_counter >= occurrence:
                        return page_num + 1
    
    return -1

def get_last_page_from_identifiers(
    pdf_pages_text: List[str],
    identifiers: str,
    alternate_identifiers: str,
    first_page: int,
    no_of_pages: int,
    ending_minus_n: int,
    demarcated_page_ranges: List[Tuple[int, int]]
) -> int:
    """
    Finds the ending page of a sub-document.
    Returns a 1-based page number or -1 if not found.
    """
    total_pdf_page_count = len(pdf_pages_text)

    # Case 1: A fixed number of pages is specified
    if no_of_pages >= 1:
        last_page = first_page + no_of_pages - 1
        if not is_page_demarcated(last_page, demarcated_page_ranges):
            return min(last_page, total_pdf_page_count)
        return -1 # Cannot demarcate if the page is already taken

    identifier_list = [i.strip() for i in identifiers.split('|') if i.strip()]
    alternate_identifier_list = [i.strip() for i in (alternate_identifiers or "").split('|') if i.strip()]

    # Case 2: Use alternate ending identifiers (EndingIdentifierMinus1)
    if alternate_identifier_list:
        # Adjust ending_minus_n similar to C# logic
        ending_minus_n = max(0, ending_minus_n - 1)
        
        # Start searching from the first page of the sub-document
        for page_num in range(first_page - 1, total_pdf_page_count):
            if is_page_demarcated(page_num, demarcated_page_ranges):
                continue
            
            normalized_page_text = normalize_text(pdf_pages_text[page_num])
            
            for identifier in alternate_identifier_list:
                found = False
                if is_contains_only(identifier):
                    if is_page_contains_only(normalized_page_text, clean_contains_only(identifier)):
                        found = True
                elif identifier.lower() in normalized_page_text.lower():
                    found = True
                
                if found:
                    # Ensure the calculated last page is not before the first page
                    if first_page <= (page_num - ending_minus_n):
                        return page_num - ending_minus_n
                    else:
                        return -1 # Invalid range

    # Case 3: Use primary ending identifiers
    elif identifier_list:
        for page_num in range(first_page - 1, total_pdf_page_count):
            if is_page_demarcated(page_num + 1, demarcated_page_ranges):
                continue
            
            normalized_page_text = normalize_text(pdf_pages_text[page_num])
            
            for identifier in identifier_list:
                found = False
                if is_contains_only(identifier):
                    if is_page_contains_only(normalized_page_text, clean_contains_only(identifier)):
                        found = True
                elif identifier.lower() in normalized_page_text.lower():
                    found = True
                
                if found:
                    return page_num + 1

    # Case 4: No ending identifier found, demarcate to the end of the document
    else:
        return total_pdf_page_count

    return -1 # Return -1 if no condition is met

def process_demarcation(pdf_pages_text: List[str], identifiers_rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Main function to process PDF text and split it into sub-documents.
    """
    demarcated_page_ranges = []
    sub_documents = []
    pdf_page_count = len(pdf_pages_text)

    for rule in identifiers_rules:
        first_page = 0
        last_page = 0
        
        # Extract rules with defaults
        starting_identifier = rule.get("StartingIdentifier", "")
        starting_identifier_plus1 = rule.get("StartingIdentifierPlus1", "")
        ending_identifier = rule.get("EndingIdentifier", "")
        ending_identifier_minus1 = rule.get("EndingIdentifierMinus1", "")
        occurrence = rule.get("Occurence", 1)
        no_of_pages = rule.get("NoOfPages", 0)
        starting_minus_n = rule.get("StartingMinusN", 0) # Corresponds to StartingPlusN in C#
        
        # In C#, EndingMinusN is used for the alternate ending identifier logic
        # We pass StartingMinusN to GetLastPageFromIdentifiers as the C# code does
        ending_minus_n_param = starting_minus_n 

        # Rule: If no starting identifiers, it's the first page of the document
        if not starting_identifier and not starting_identifier_plus1:
            first_page = 1
        else:
            first_page = get_first_page_from_identifiers(
                pdf_pages_text,
                starting_identifier,
                starting_identifier_plus1,
                occurrence,
                starting_minus_n,
                demarcated_page_ranges
            )

        if first_page > 0 and first_page <= pdf_page_count:
            last_page = get_last_page_from_identifiers(
                pdf_pages_text,
                ending_identifier,
                ending_identifier_minus1,
                first_page,
                no_of_pages,
                ending_minus_n_param,
                demarcated_page_ranges
            )

            if last_page > 0 and last_page >= first_page and not is_page_range_overlapping(first_page, last_page, demarcated_page_ranges):
                demarcated_page_ranges.append((first_page, last_page))
            else:
                # Invalidate if overlap or invalid range
                first_page, last_page = 0, 0
        else:
            first_page = 0 # Invalidate if start page not found

        sub_documents.append({
            "DocumentTypeID": rule.get("DocumentTypeID"),
            "FromPage": first_page,
            "ToPage": last_page
        })

    return sub_documents

# Example Usage
if __name__ == '__main__':
    # Sample data mimicking a 10-page PDF
    sample_pdf_text = [
        "Page 1: Document Cover Sheet",
        "Page 2: Start of Invoices",
        "Page 3: Invoice #123 content",
        "Page 4: Invoice #456 content",
        "Page 5: End of Invoices section",
        "Page 6: Some random text",
        "Page 7: Start of Reports",
        "Page 8: Report data page 1",
        "Page 9: Report data page 2",
        "Page 10: Final page of the document",
    ]

    # Sample rules similar to the 'identifiers' list in C#
    sample_rules = [
        {
            "DocumentTypeID": "INVOICES",
            "StartingIdentifier": "Start of Invoices",
            "EndingIdentifier": "End of Invoices",
            "Occurence": 1,
            "NoOfPages": 0,
            "StartingIdentifierPlus1": "",
            "EndingIdentifierMinus1": "",
            "StartingMinusN": 0,
        },
        {
            "DocumentTypeID": "REPORTS",
            "StartingIdentifier": "Start of Reports",
            "EndingIdentifier": "", # No ending ID, should go to the end
            "Occurence": 1,
            "NoOfPages": 0,
            "StartingIdentifierPlus1": "",
            "EndingIdentifierMinus1": "",
            "StartingMinusN": 0,
        },
        {
            "DocumentTypeID": "COVER_SHEET",
            "StartingIdentifier": "", # No starting ID, should be page 1
            "EndingIdentifier": "",
            "Occurence": 1,
            "NoOfPages": 1, # Fixed page length
            "StartingIdentifierPlus1": "",
            "EndingIdentifierMinus1": "",
            "StartingMinusN": 0,
        }
    ]

    demarcated_docs = process_demarcation(sample_pdf_text, sample_rules)

    print("Demarcation Results:")
    for doc in demarcated_docs:
        print(f"  - Document Type: {doc['DocumentTypeID']}, From Page: {doc['FromPage']}, To Page: {doc['ToPage']}")