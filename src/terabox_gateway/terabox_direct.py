"""
terabox_direct.py -- Direct TeraBox extraction with cookie rotation.

No third-party proxy dependency. Fetches the share page HTML directly,
extracts jsToken, and calls TeraBox's own list API. Supports rotating
across multiple TeraBox accounts (cookies) so a single flagged session
doesn't take the whole service down -- the same resilience pattern
commercial extractors like iTeraPlay rely on.

Drop into src/terabox_gateway/ in your fork, alongside terabox_client.py.

Environment variables:
  COOKIE_POOL   JSON array of ndus cookie strings, e.g.
                ["cookie1value", "cookie2value", "cookie3value"]
                Falls back to single COOKIE_JSON if COOKIE_POOL isn't set.
  COOKIE_JSON   Single ndus cookie string (existing variable, still works).
"""

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Union
from urllib.parse import parse_qs, urlparse

import aiohttp

from .config import headers
from .utils import get_formatted_size

TERABOX_BASE = "https://www.terabox.com"
JS_TOKEN_PATTERN = re.compile(r"fn%28%22(.*?)%22%29")

# ---- Cookie pool ------------------------------------------------------

def _load_cookie_pool() -> List[str]:
    """Load one or more ndus cookie values, from COOKIE_POOL (JSON array)
    or falling back to the single COOKIE_JSON value."""
    pool_raw = os.environ.get("COOKIE_POOL")
    if pool_raw:
        try:
            values = json.loads(pool_raw)
            if isinstance(values, list) and values:
                return [str(v).strip() for v in values if str(v).strip()]
        except Exception as e:
            logging.error(f"Failed to parse COOKIE_POOL as JSON: {e}")

    single = os.environ.get("COOKIE_JSON")
    if single:
        return [single.strip()]

    return []


def _cookie_dict(ndus_value: str) -> Dict[str, str]:
    return {"ndus": ndus_value} if ndus_value else {}


# ---- jsToken cache (per surl, short TTL) -------------------------------

_token_cache: Dict[str, Dict[str, Any]] = {}
_TOKEN_TTL_SECONDS = 300  # 5 minutes -- avoids re-fetching the share page
                          # on every request, which itself reduces the
                          # request volume that can trigger anti-bot flags.


def _get_cached_token(surl: str):
    entry = _token_cache.get(surl)
    if entry and (time.time() - entry["ts"]) < _TOKEN_TTL_SECONDS:
        return entry["token"], entry["cookie_index"]
    return None, None


def _set_cached_token(surl: str, token: str, cookie_index: int):
    _token_cache[surl] = {"token": token, "ts": time.time(), "cookie_index": cookie_index}


# ---- Helpers ------------------------------------------------------------

def _extract_surl(url: str) -> str:
    parsed = urlparse(url)
    if "surl=" in parsed.query:
        surl = parse_qs(parsed.query)["surl"][0]
    elif "/s/" in parsed.path:
        surl = parsed.path.split("/s/")[1].split("/")[0].split("?")[0]
    else:
        raise ValueError("Could not extract surl from URL")
    if surl.startswith("1"):
        surl = surl[1:]
    return surl


async def _try_extract_with_cookie(
    session: aiohttp.ClientSession, surl: str, password: str
):
    """One attempt: fetch share page, extract jsToken, call list API.
    Returns (files_or_error_dict, got_verify_challenge: bool)."""

    init_params = {"surl": surl}
    if password:
        init_params["pwd"] = password

    try:
        async with session.get(f"{TERABOX_BASE}/share/init", params=init_params, timeout=15) as resp:
            html = await resp.text()
    except Exception as e:
        return {"error": f"Could not reach TeraBox: {e}", "errno": -1}, False

    match = JS_TOKEN_PATTERN.search(html)
    if not match:
        # Treat "no token in page" the same as a verify challenge --
        # it usually means this session/cookie isn't trusted right now.
        return {"error": "jsToken not found (cookie likely flagged)", "errno": 4000020}, True
    js_token = match.group(1)

    list_params = {
        "app_id": "250528",
        "web": "1",
        "channel": "chunlei",
        "clienttype": "0",
        "jsToken": js_token,
        "shorturl": surl,
        "root": "1",
    }
    if password:
        list_params["pwd"] = password

    try:
        async with session.get(f"{TERABOX_BASE}/share/list", params=list_params, timeout=15) as resp:
            data = await resp.json(content_type=None)
    except Exception as e:
        return {"error": f"Could not fetch file list: {e}", "errno": -1}, False

    errno = data.get("errno", -1)
    if errno in (400141, 4000020):
        return {"error": data.get("errmsg", "Verification required"), "errno": errno}, True
    if errno != 0:
        return {"error": data.get("errmsg", "Unknown TeraBox error"), "errno": errno}, False

    raw_files = data.get("list", [])
    if not raw_files:
        return {"error": "No files found in this share", "errno": -1}, False

    results = []
    for item in raw_files:
        thumbs = item.get("thumbs") or {}
        thumb_url = thumbs.get("url3", "")
        results.append({
            "filename": item.get("server_filename", "Unknown"),
            "size": get_formatted_size(item.get("size", 0)),
            "size_bytes": item.get("size", 0),
            "download_link": item.get("dlink", ""),
            "is_directory": item.get("isdir") == "1",
            "thumbnails": {"original": thumb_url} if thumb_url else {},
            "thumbnail": thumb_url,
            "path": item.get("path", ""),
            "fs_id": item.get("fs_id", ""),
        })

    return results, False


# ---- Public entrypoint ---------------------------------------------------

async def fetch_download_link_direct(
    url: str, password: str = ""
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Fetch file list + direct download links, rotating across the cookie
    pool if one account gets a verify challenge from TeraBox."""

    try:
        surl = _extract_surl(url)
    except ValueError as e:
        return {"error": str(e), "errno": -1}

    cookie_pool = _load_cookie_pool()
    if not cookie_pool:
        return {"error": "No TeraBox cookies configured (set COOKIE_JSON or COOKIE_POOL)", "errno": -1}

    last_error: Dict[str, Any] = {"error": "All cookies exhausted", "errno": -1}

    for idx, ndus_value in enumerate(cookie_pool):
        cookies = _cookie_dict(ndus_value)
        async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
            result, was_verify_challenge = await _try_extract_with_cookie(session, surl, password)

            if isinstance(result, list):
                logging.info(f"Success using cookie index {idx}/{len(cookie_pool)-1}")
                return result

            last_error = result
            if was_verify_challenge:
                logging.warning(
                    f"Cookie index {idx} flagged (verify challenge). "
                    f"Trying next cookie in pool ({idx+1}/{len(cookie_pool)-1} remaining)..."
                )
                continue
            else:
                # Non-verify error (bad URL, file genuinely gone, etc.) --
                # no point retrying with other cookies, it'll fail the same way.
                return result

    return last_error
