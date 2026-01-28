import os
import json
import asyncio
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

GOOGLE_STORAGE_URL = os.getenv("GOOGLE_STORAGE_URL")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")


if not GOOGLE_STORAGE_URL or not GOOGLE_API_KEY:
    raise RuntimeError("GOOGLE_STORAGE_URL или GOOGLE_API_KEY не заданы")


async def gs_post(action: str, payload: dict | None = None) -> dict:
    body = {
        "api_key": GOOGLE_API_KEY,
        "action": action
    }

    if payload:
        body.update(payload)

    req = Request(
        GOOGLE_STORAGE_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )

    try:
        resp = await asyncio.to_thread(urlopen, req, timeout=15)
        return json.loads(resp.read().decode("utf-8"))

    except HTTPError as e:
        return {
            "success": False,
            "error": f"HTTP {e.code}"
        }

    except URLError as e:
        return {
            "success": False,
            "error": "network_error"
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
