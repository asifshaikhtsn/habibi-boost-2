"""
Country geolocation:
- Primary: MaxMind GeoLite2 Country local MMDB.
- Fallback: IPinfo Lite.
- Emergency fallback: ip-api.com when credentials are not configured.

GeoLite2 data created by MaxMind, available from https://www.maxmind.com.
IP address data fallback powered by IPinfo: https://ipinfo.io.
"""
import asyncio
import io
import os
import tarfile
import tempfile
from pathlib import Path

import aiohttp

try:
    import geoip2.database
    from geoip2.errors import AddressNotFoundError
except ImportError:
    geoip2 = None
    AddressNotFoundError = Exception

MAXMIND_DOWNLOAD_URL = (
    "https://download.maxmind.com/geoip/databases/"
    "GeoLite2-Country/download?suffix=tar.gz"
)
DB_PATH = Path(
    os.environ.get("MAXMIND_DB_PATH")
    or (Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())) / "GeoLite2-Country.mmdb")
)


async def _ensure_maxmind_db():
    if DB_PATH.exists() and DB_PATH.stat().st_size > 1024:
        return True

    account_id = os.environ.get("MAXMIND_ACCOUNT_ID", "").strip()
    license_key = os.environ.get("MAXMIND_LICENSE_KEY", "").strip()
    if not account_id or not license_key:
        print("[Geo] MaxMind credentials not configured; skipping GeoLite2 DB.")
        return False

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        timeout = aiohttp.ClientTimeout(total=180)
        auth = aiohttp.BasicAuth(account_id, license_key)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(MAXMIND_DOWNLOAD_URL, auth=auth, allow_redirects=True) as resp:
                if resp.status != 200:
                    print(f"[Geo] MaxMind DB download failed: HTTP {resp.status}")
                    return False
                archive = await resp.read()

        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tf:
            member = next(
                (m for m in tf.getmembers() if m.isfile() and m.name.endswith("/GeoLite2-Country.mmdb")),
                None,
            )
            if member is None:
                print("[Geo] GeoLite2-Country.mmdb not found in downloaded archive.")
                return False
            src = tf.extractfile(member)
            if src is None:
                return False
            tmp_path = DB_PATH.with_name(DB_PATH.name + ".tmp")
            tmp_path.write_bytes(src.read())
            tmp_path.replace(DB_PATH)

        print(f"[Geo] GeoLite2 Country DB ready: {DB_PATH}")
        return True
    except Exception as exc:
        print(f"[Geo] MaxMind DB setup failed: {type(exc).__name__}: {exc}")
        return False


async def _lookup_maxmind(ips):
    result = {}
    if geoip2 is None:
        print("[Geo] geoip2 package unavailable; skipping MaxMind.")
        return result
    if not await _ensure_maxmind_db():
        return result

    try:
        with geoip2.database.Reader(str(DB_PATH)) as reader:
            for ip in ips:
                try:
                    record = reader.country(ip)
                    code = (record.country.iso_code or "").upper()
                    if code:
                        result[ip] = code
                except AddressNotFoundError:
                    continue
                except Exception:
                    continue
    except Exception as exc:
        print(f"[Geo] MaxMind lookup failed: {type(exc).__name__}: {exc}")
    return result


async def _lookup_ipinfo(ips):
    token = os.environ.get("IPINFO_TOKEN", "").strip()
    if not token or not ips:
        return {}

    result = {}
    semaphore = asyncio.Semaphore(25)
    timeout = aiohttp.ClientTimeout(total=15)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def one(ip):
            async with semaphore:
                for attempt in range(2):
                    try:
                        async with session.get(
                            f"https://api.ipinfo.io/lite/{ip}",
                            params={"token": token},
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json(content_type=None)
                                code = str(
                                    data.get("country")
                                    or data.get("country_code")
                                    or ""
                                ).upper()
                                if len(code) == 2:
                                    return ip, code
                            elif resp.status in (401, 403):
                                return ip, ""
                    except Exception:
                        pass
                    await asyncio.sleep(0.25 * (attempt + 1))
            return ip, ""

        rows = await asyncio.gather(*(one(ip) for ip in ips))

    for ip, code in rows:
        if code:
            result[ip] = code
    return result


async def _lookup_ipapi(ips):
    """Emergency compatibility fallback so existing workflows never lose geolocation."""
    if not ips:
        return {}

    result = {}
    batches = [ips[i:i + 100] for i in range(0, len(ips), 100)]
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for batch in batches:
            for attempt in range(3):
                try:
                    async with session.post(
                        "http://ip-api.com/batch",
                        json=[{"query": ip, "fields": "status,countryCode,query"} for ip in batch],
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            if isinstance(data, list):
                                for entry in data:
                                    if isinstance(entry, dict) and entry.get("status") == "success":
                                        ip = str(entry.get("query", ""))
                                        code = str(entry.get("countryCode", "")).upper()
                                        if ip and len(code) == 2:
                                            result[ip] = code
                                break
                except Exception:
                    pass
                await asyncio.sleep(min(5, attempt + 1))
            await asyncio.sleep(1.5)
    return result


async def geolocate_ips(ips):
    unique_ips = list(dict.fromkeys(str(ip).strip() for ip in ips if str(ip).strip()))
    if not unique_ips:
        return {}

    result = await _lookup_maxmind(unique_ips)
    maxmind_count = len(result)

    unresolved = [ip for ip in unique_ips if ip not in result]
    ipinfo_result = await _lookup_ipinfo(unresolved)
    result.update(ipinfo_result)

    unresolved = [ip for ip in unique_ips if ip not in result]
    ipapi_result = await _lookup_ipapi(unresolved)
    result.update(ipapi_result)

    print(
        "[Geo] Country lookup: "
        f"total={len(unique_ips)}, MaxMind={maxmind_count}, "
        f"IPinfo={len(ipinfo_result)}, ip-api={len(ipapi_result)}, "
        f"unresolved={len(unique_ips) - len(result)}"
    )
    return result
