"""
Batch Document AI processing for files in ./input.
Saves processed JSON to ./output with a *_processed.json suffix.

Uses GOOGLE_APPLICATION_CREDENTIALS environment variable for authentication.
No hard-coded credential paths.

Environment Variables Required:
  GOOGLE_APPLICATION_CREDENTIALS  - Path to Google Cloud service account JSON
  DOCAI_PROCESSOR_ID             - (Optional) Processor ID, or use docai_processor_id.txt
  DOCAI_LOCATION                 - (Optional) Location, defaults to "us"
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict

from google.cloud import documentai
from google.protobuf.json_format import MessageToJson

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
PROCESSOR_ID_PATH = BASE_DIR / "docai_processor_id.txt"


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
    
    print(f"[DOCAI] GOOGLE_APPLICATION_CREDENTIALS = {creds_path}")
    
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
    
    print(f"[DOCAI] [OK] Credentials file exists: {creds_path}")
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
        print(f"[DOCAI] [OK] Project ID: {project_id}")
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
        print(f"[DOCAI] [OK] Processor ID (from env): {env_id}")
        return env_id.strip()
    
    # Try file
    if PROCESSOR_ID_PATH.exists():
        proc_id = PROCESSOR_ID_PATH.read_text(encoding="utf-8").strip()
        if proc_id:
            print(f"[DOCAI] [OK] Processor ID (from file): {proc_id}")
            return proc_id
    
    raise RuntimeError(
        "Processor ID not found.\n"
        f"Set DOCAI_PROCESSOR_ID env var or create: {PROCESSOR_ID_PATH}"
    )


def get_mime_type(path: Path) -> str:
    ext = path.suffix.lower().lstrip(".")
    mime_map: Dict[str, str] = {
        "pdf": "application/pdf",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "tif": "image/tiff",
        "tiff": "image/tiff",
        "bmp": "image/bmp",
        "gif": "image/gif",
        "webp": "image/webp",
    }
    return mime_map.get(ext, "application/octet-stream")


def process_file(client: documentai.DocumentProcessorServiceClient, name: str, path: Path) -> Path:
    with open(path, "rb") as f:
        content = f.read()
    request = {"name": name, "raw_document": {"content": content, "mime_type": get_mime_type(path)}}
    result = client.process_document(request=request)
    doc_json = json.loads(MessageToJson(result.document._pb))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{path.stem}_processed.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(doc_json, f, ensure_ascii=False, indent=2)
    return out_path


def main() -> int:
    print("=" * 60)
    print("[DOCAI] Batch Document AI Processing - Starting")
    print("=" * 60)
    
    # Validate credentials first
    try:
        creds_path = validate_credentials()
        project_id = load_project_id(creds_path)
        processor_id = load_processor_id()
        location = os.getenv("DOCAI_LOCATION", "us")
        print(f"[DOCAI] [OK] Location: {location}")
    except RuntimeError as e:
        print(f"[DOCAI] [FATAL] {e}")
        return 1

    client = documentai.DocumentProcessorServiceClient()
    name = client.processor_path(project_id, location, processor_id)
    print(f"[DOCAI] Using processor: {name}")

    if not INPUT_DIR.exists():
        print(f"[WARN] Input directory not found: {INPUT_DIR}")
        return 1

    files = [p for p in INPUT_DIR.iterdir() if p.is_file() and get_mime_type(p) != "application/octet-stream"]
    if not files:
        print(f"[WARN] No supported files in {INPUT_DIR}")
        return 1

    for path in files:
        start = datetime.utcnow()
        try:
            out_path = process_file(client, name, path)
            elapsed = (datetime.utcnow() - start).total_seconds()
            print(f"[OK] {path.name} -> {out_path.name} ({elapsed:.1f}s)")
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[ERROR] Failed to process {path.name}: {exc}")
            return 1

    print("[DONE] Document AI processing complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
