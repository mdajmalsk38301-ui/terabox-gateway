"""
terabox_dl_lib.py -- Wraps the independent TeraboxDL PyPI package
(pip install terabox-downloader) as an alternative extraction method.

This is a separately-maintained codebase from terabox_client.py /
terabox_direct.py, so it may succeed where those fail (different token
handling, different request shape TeraBox hasn't specifically blocked).

Drop into src/terabox_gateway/ in your fork.

Requires: pip install terabox-downloader
(imported as `from TeraboxDL import TeraboxDL`)
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Union

from TeraboxDL import TeraboxDL


def _load_cookie_pool() -> List[str]:
    """Reuse the same COOKIE_POOL / COOKIE_JSON env vars as terabox_direct.py,
    just reformatted into the 'lang=en; ndus=VALUE;' string TeraboxDL expects."""
    pool_raw = os.environ.get("COOKIE_POOL")
    ndus_values = []

    if pool_raw:
        try:
            values = json.loads(pool_raw)
            if isinstance(values, list):
                ndus_values = [str(v).strip() for v in values if str(v).strip()]
        except Exception as e:
            logging.error(f"Failed to parse COOKIE_POOL: {e}")

    if not ndus_values:
        single = os.environ.get("COOKIE_JSON")
        if single:
            ndus_values = [single.strip()]

    return [f"lang=en; ndus={v};" for v in ndus_values]


def _sync_fetch(cookie_string: str, url: str) -> Dict[str, Any]:
    """Blocking call into the TeraboxDL library -- run via asyncio.to_thread."""
    terabox = TeraboxDL(cookie_string)
    return terabox.get_file_info(url)


async def fetch_via_teraboxdl(url: str, password: str = "") -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Try each cookie in the pool against the TeraboxDL library until one works."""

    cookie_pool = _load_cookie_pool()
    if not cookie_pool:
        return {"error": "No TeraBox cookies configured", "errno": -1}

    last_error: Dict[str, Any] = {"error": "All cookies failed via TeraboxDL", "errno": -1}

    for idx, cookie_string in enumerate(cookie_pool):
        try:
            info = await asyncio.to_thread(_sync_fetch, cookie_string, url)
        except Exception as e:
            logging.error(f"TeraboxDL cookie {idx} raised exception: {e}")
            last_error = {"error": str(e), "errno": -1}
            continue

        if isinstance(info, dict) and info.get("error"):
            logging.warning(f"TeraboxDL cookie {idx} failed: {info['error']}")
            last_error = {"error": info["error"], "errno": -1}
            continue

        if isinstance(info, dict) and info.get("download_link"):
            logging.info(f"TeraboxDL succeeded with cookie index {idx}")
            return [{
                "filename": info.get("file_name", "Unknown"),
                "size": info.get("file_size", ""),
                "size_bytes": 0,
                "download_link": info.get("download_link", ""),
                "is_directory": False,
                "thumbnails": {"original": info.get("thumbnail", "")} if info.get("thumbnail") else {},
                "thumbnail": info.get("thumbnail", ""),
                "path": "",
                "fs_id": "",
            }]

        last_error = {"error": "No download_link in TeraboxDL response", "errno": -1}

    return last_error
