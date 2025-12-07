"""
IO utilities for the data-fetch framework.
Handles file paths, timestamped run IDs, and raw data dumps.
"""

import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Union, Optional, Any
import os
import tempfile


# Base paths
BASE_DIR = Path(__file__).parent.parent.parent

# Check if running in cloud environment (Streamlit Cloud, Heroku, etc.)
# Use temp directory for outputs in cloud environments
IS_CLOUD_ENV = os.getenv("STREAMLIT_SERVER_ENV") is not None or os.getenv("DYNO") is not None

if IS_CLOUD_ENV:
    # Use temp directory in cloud
    OUTPUTS_DIR = Path(tempfile.gettempdir()) / "data-fetch" / "outputs"
    RAW_DIR = OUTPUTS_DIR / "raw"
    EXCEL_DIR = OUTPUTS_DIR / "excel"
else:
    # Use local outputs directory
    OUTPUTS_DIR = BASE_DIR / "outputs"
    RAW_DIR = OUTPUTS_DIR / "raw"
    EXCEL_DIR = OUTPUTS_DIR / "excel"

CONFIG_DIR = BASE_DIR / "config"


def ensure_dir(path: Path) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path to ensure exists
    
    Returns:
        The path that was ensured
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def generate_run_id(prefix: str = "") -> str:
    """
    Generate a unique run ID with timestamp.
    
    Args:
        prefix: Optional prefix for the run ID
    
    Returns:
        Unique run ID string (e.g., "theblock_20240115_103045_a1b2c3")
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Add a short random hash for uniqueness
    random_hash = hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()[:6]
    
    if prefix:
        return f"{prefix}_{timestamp}_{random_hash}"
    return f"{timestamp}_{random_hash}"


def get_output_path(
    filename: str,
    output_type: str = "excel",
    site_id: Optional[str] = None,
    run_id: Optional[str] = None,
) -> Path:
    """
    Get the output path for a file.
    
    Args:
        filename: Name of the file (with extension)
        output_type: Type of output ("excel" or "raw")
        site_id: Optional site identifier for organizing files
        run_id: Optional run ID for versioning
    
    Returns:
        Full path to the output file
    """
    if output_type == "excel":
        base_dir = EXCEL_DIR
    elif output_type == "raw":
        base_dir = RAW_DIR
    else:
        base_dir = OUTPUTS_DIR / output_type
    
    # Create subdirectory structure
    if site_id:
        base_dir = base_dir / site_id
    if run_id:
        base_dir = base_dir / run_id
    
    ensure_dir(base_dir)
    return base_dir / filename


def save_raw_response(
    content: Union[str, bytes, dict],
    filename: str,
    site_id: str,
    run_id: Optional[str] = None,
    content_type: str = "auto",
) -> Path:
    """
    Save raw response content to the outputs/raw directory.
    
    Args:
        content: The content to save (string, bytes, or dict)
        filename: Name for the file
        site_id: Site identifier for organizing files
        run_id: Optional run ID for versioning
        content_type: Type of content ("json", "html", "text", or "auto")
    
    Returns:
        Path to the saved file
    """
    if run_id is None:
        run_id = generate_run_id(site_id)
    
    # Auto-detect content type
    if content_type == "auto":
        if isinstance(content, dict):
            content_type = "json"
        elif isinstance(content, str):
            if content.strip().startswith(("{", "[")):
                content_type = "json"
            elif content.strip().startswith("<"):
                content_type = "html"
            else:
                content_type = "text"
        else:
            content_type = "binary"
    
    # Add appropriate extension if not present
    if not any(filename.endswith(ext) for ext in [".json", ".html", ".txt", ".bin"]):
        ext_map = {"json": ".json", "html": ".html", "text": ".txt", "binary": ".bin"}
        filename = filename + ext_map.get(content_type, ".txt")
    
    output_path = get_output_path(filename, "raw", site_id, run_id)
    
    # Save based on content type
    if content_type == "json":
        if isinstance(content, dict):
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(content, f, indent=2, ensure_ascii=False, default=str)
        else:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(content if isinstance(content, str) else content.decode("utf-8"))
    elif content_type == "binary":
        with open(output_path, "wb") as f:
            f.write(content if isinstance(content, bytes) else content.encode("utf-8"))
    else:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content if isinstance(content, str) else str(content))
    
    return output_path


def load_raw_response(
    filename: str,
    site_id: str,
    run_id: str,
    as_json: bool = False,
) -> Union[str, dict]:
    """
    Load a previously saved raw response.
    
    Args:
        filename: Name of the file
        site_id: Site identifier
        run_id: Run ID
        as_json: If True, parse as JSON
    
    Returns:
        File content as string or dict
    """
    file_path = get_output_path(filename, "raw", site_id, run_id)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Raw response file not found: {file_path}")
    
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    if as_json:
        return json.loads(content)
    return content


def generate_site_id(url: str, page_slug: Optional[str] = None) -> str:
    """
    Generate a unique site ID from a URL.
    
    Args:
        url: The website URL
        page_slug: Optional slug for the specific page
    
    Returns:
        Site ID string (e.g., "theblock_co_btc_eth_volume_a1b2c3")
    """
    from urllib.parse import urlparse
    import re
    
    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "").replace(".", "_")
    
    # Generate slug from path if not provided
    if page_slug is None:
        path = parsed.path.strip("/")
        # Clean up the path to create a slug
        page_slug = re.sub(r"[^a-zA-Z0-9]+", "_", path)[:30]
    
    # Add a short hash for uniqueness
    url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
    
    # Combine parts
    parts = [domain]
    if page_slug:
        parts.append(page_slug)
    parts.append(url_hash)
    
    return "_".join(parts).lower().strip("_")


def get_config_path() -> Path:
    """Get the path to the websites.yaml config file."""
    return CONFIG_DIR / "websites.yaml"


def timestamp_now() -> str:
    """Get current timestamp in ISO format."""
    return datetime.utcnow().isoformat() + "Z"

