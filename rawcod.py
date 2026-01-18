"""
Google Document AI OCR Processing Script
=========================================
Uses GOOGLE_APPLICATION_CREDENTIALS environment variable for authentication.
No hard-coded credential paths.

Environment Variables Required:
  GOOGLE_APPLICATION_CREDENTIALS  - Path to Google Cloud service account JSON
  DOCAI_PROCESSOR_ID             - (Optional) Processor ID, or use docai_processor_id.txt
  DOCAI_LOCATION                 - (Optional) Location, defaults to "us"
"""
import os
import sys
import json
from datetime import datetime
from pathlib import Path
import uuid

# ============================================================
# CONFIGURATION
# ============================================================
BASE_DIR = Path(__file__).resolve().parent
PROCESSOR_ID_PATH = BASE_DIR / "docai_processor_id.txt"
OUTPUT_DIR = BASE_DIR / "output"


def validate_credentials() -> str:
    """
    Validate Google Cloud credentials.
    
    Checks:
    1. GOOGLE_APPLICATION_CREDENTIALS env var is set
    2. The file it points to exists
    3. Returns the path for logging
    
    Raises:
        RuntimeError: If credentials are missing or invalid
    """
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    print(f"[OCR] GOOGLE_APPLICATION_CREDENTIALS = {creds_path}")
    
    if not creds_path:
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS not set.\n"
            "Set it to your service account JSON file path:\n"
            "  Windows: $env:GOOGLE_APPLICATION_CREDENTIALS = 'C:\\path\\to\\docai.json'\n"
            "  Linux:   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/docai.json"
        )
    
    if not os.path.exists(creds_path):
        raise RuntimeError(
            f"Google OCR credentials not found.\n"
            f"File does not exist: {creds_path}\n"
            f"Check your GOOGLE_APPLICATION_CREDENTIALS environment variable."
        )
    
    print(f"[OCR] [OK] Credentials file exists: {creds_path}")
    return creds_path


def load_project_id(creds_path: str) -> str:
    """
    Load project_id from the credentials JSON file.
    """
    try:
        with open(creds_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        project_id = data.get("project_id")
        if not project_id:
            raise ValueError("project_id not found in credentials file")
        print(f"[OCR] [OK] Project ID: {project_id}")
        return project_id
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in credentials file: {e}")


def load_processor_id() -> str:
    """
    Load processor ID from:
    1. DOCAI_PROCESSOR_ID environment variable
    2. docai_processor_id.txt file in project root
    """
    # Try env var first
    env_id = os.getenv("DOCAI_PROCESSOR_ID")
    if env_id:
        print(f"[OCR] [OK] Processor ID (from env): {env_id}")
        return env_id.strip()
    
    # Try file
    if PROCESSOR_ID_PATH.exists():
        proc_id = PROCESSOR_ID_PATH.read_text(encoding="utf-8").strip()
        if proc_id:
            print(f"[OCR] [OK] Processor ID (from file): {proc_id}")
            return proc_id
    
    raise RuntimeError(
        "Processor ID not found.\n"
        f"Set DOCAI_PROCESSOR_ID env var or create: {PROCESSOR_ID_PATH}"
    )


def get_mime_type(file_path: str) -> str:
    """
    Get MIME type based on file extension for Document AI supported formats.
    """
    ext = file_path.lower().split('.')[-1]
    mime_types = {
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'png': 'image/png',
        'bmp': 'image/bmp',
        'gif': 'image/gif',
        'tiff': 'image/tiff',
        'tif': 'image/tiff',
        'webp': 'image/webp',
        'pdf': 'application/pdf'
    }
    return mime_types.get(ext, 'application/octet-stream')


def main() -> int:
    """
    Main OCR processing function.
    Returns 0 on success, 1 on failure.
    """
    print("=" * 60)
    print("[OCR] Google Document AI - Starting")
    print("=" * 60)
    
    # ============================================================
    # STEP 1: Validate credentials (MUST be done before import)
    # ============================================================
    try:
        creds_path = validate_credentials()
        project_id = load_project_id(creds_path)
        processor_id = load_processor_id()
        location = os.getenv("DOCAI_LOCATION", "us")
        print(f"[OCR] [OK] Location: {location}")
    except RuntimeError as e:
        print(f"[OCR] [FATAL] {e}")
        return 1
    
    # ============================================================
    # STEP 2: Import Document AI client (after env var is validated)
    # ============================================================
    try:
        from google.cloud import documentai
        print("[OCR] [OK] Document AI client imported")
    except ImportError as e:
        print(f"[OCR] [FATAL] Cannot import google-cloud-documentai: {e}")
        print("       Install with: pip install google-cloud-documentai")
        return 1
    
    # ============================================================
    # STEP 3: Get input file
    # ============================================================
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = input("[OCR] Enter the file path (image or PDF): ").strip().strip('"')
    
    print(f"[OCR] Input file: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"[OCR] [ERROR] File not found: {file_path}")
        return 1
    
    mime_type = get_mime_type(file_path)
    print(f"[OCR] MIME type: {mime_type}")
    
    if mime_type == 'application/octet-stream':
        print(f"[OCR] [ERROR] Unsupported file type")
        return 1
    
    # ============================================================
    # STEP 4: Process document with Document AI
    # ============================================================
    try:
        print("[OCR] Initializing Document AI client...")
        client = documentai.DocumentProcessorServiceClient()
        processor_name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
        print(f"[OCR] Processor: {processor_name}")
        
        # Load document
        with open(file_path, "rb") as f:
            file_content = f.read()
        print(f"[OCR] File size: {len(file_content):,} bytes")
        
        raw_document = {"content": file_content, "mime_type": mime_type}
        request = {"name": processor_name, "raw_document": raw_document}
        
        print("[OCR] Sending request to Document AI...")
        result = client.process_document(request=request)
        document = result.document
        print("[OCR] [OK] OCR processing completed successfully")
        
    except Exception as e:
        print(f"[OCR] [ERROR] Document AI processing failed: {e}")
        return 1
    
    # ============================================================
    # STEP 5: Structure the extracted data
    # ============================================================
    structured_invoice = {
        "header": {},
        "line_items": [],
        "footer": {}
    }
    
    header_fields = [
        'invoice_id', 'invoice_date', 'supplier_name', 'supplier_address',
        'supplier_tax_id', 'supplier_iban', 'receiver_name', 'receiver_address',
        'receiver_tax_id', 'invoice_type'
    ]
    footer_fields = ['net_amount', 'total_tax_amount', 'vat', 'total_amount']
    
    for entity in document.entities:
        entity_type = entity.type_
        
        entity_details = {
            "value": entity.mention_text,
            "properties": {}
        }
        
        for child in entity.properties:
            child_type = child.type_.split('/')[-1]
            entity_details["properties"][child_type] = child.mention_text
        
        if entity_type in header_fields:
            structured_invoice["header"][entity_type] = entity_details
        elif entity_type == 'line_item':
            structured_invoice["line_items"].append(entity_details)
        elif entity_type in footer_fields:
            structured_invoice["footer"][entity_type] = entity_details
        else:
            structured_invoice["header"][entity_type] = entity_details
    
    # Add transaction ID
    transaction_id = str(uuid.uuid4())
    structured_invoice["transaction_id"] = transaction_id
    
    # ============================================================
    # STEP 6: Save output
    # ============================================================
    current_date = datetime.now().strftime("%Y%m%d")
    base_filename = os.path.splitext(os.path.basename(file_path))[0]
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_filename = OUTPUT_DIR / f"{base_filename}_structured_{current_date}.json"
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(structured_invoice, f, indent=2, ensure_ascii=False)
    
    print(f"[OCR] [OK] Transaction ID: {transaction_id}")
    print(f"TRANSACTION_ID:{transaction_id}")
    print(f"Structured JSON saved to: {output_filename}")
    print("=" * 60)
    print("[OCR] SUCCESS")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
