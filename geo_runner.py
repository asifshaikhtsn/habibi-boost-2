import asyncio
import importlib.util
import json
import re
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

import aiohttp
from geo_country import geolocate_ips

try:
    from aiohttp_socks import ProxyConnector
    HAS_SOCKS = True
except ImportError:
    ProxyConnector = None
    HAS_SOCKS = False

ROOT = Path.cwd()
DATA_DIR = ROOT / "data"
COUNTRY_DIR = ROOT / "country"
DEAD_FILE = DATA_DIR / "dead_proxies.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ADDRESS_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{1,5})")
PROTOCOLS = ("HTTP", "HTTPS", "SOCKS4", "SOCKS5")

# Failed proxies are temporary quarantine entries, never permanent bans.
FAILURE_COOLDOWNS = (2 * 3600, 6 * 3600, 12 * 3600, 24 * 3600)
FAILURE_RECORD_TTL = 14 * 24 * 3600

# Speed controls. Existing healthy proxies are ALWAYS revalidated separately.
MAX_POOL_NEW_PER_RUN = 15000
MAX_BOOST_NEW_PER_RUN = 20000
REQUEST_TIMEOUT = 7
POOL_CONCURRENCY = 200
BOOST_CONCURRENCY = 150

HTTP_TEST_URLS = ("http://example.com/", "http://httpbin.org/ip")
HTTPS_TEST_URLS = ("https://example.com/", "https://httpbin.org/ip")


def normalize_protocol(value, default="HTTP"):
    value = str(value or default).strip().upper().replace("-", "").replace(" ", "")
    if value in PROTOCOLS:
        return value
    if value.startswith("HTTPS"):
        return "HTTPS"
    if value.startswith("SOCKS5"):
        return "SOCKS5"
    if value.startswith("SOCKS4"):
        return "SOCKS4"
    return "HTTP"


def address_of(value):
    match = ADDRESS_RE.search(str(value or ""))
    return match.group(1) if match else ""


def record_key(address, protocol):
    return f"{normalize_protocol(protocol)}|{address}"


class HealthStore:
    def __init__(self, path=DEAD_FILE):
        self.path = Path(path)
        self.failures = {}
        self.legacy_migrated = 0
        self.load()

    def load(self):
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return

        if data.get("version") == 2 and isinstance(data.get("failures"), dict):
            now = time.time()
            for row in data["failures"].values():
                if not isinstance(row, dict):
                    continue
                address = address_of(row.get("address"))
                protocol = normalize_protocol(row.get("protocol"))
                last_failed = float(row.get("last_failed") or 0)
                if not address:
                    continue
                if last_failed and now - last_failed > FAILURE_RECORD_TTL:
                    continue
                self.failures[record_key(address, protocol)] = {
                    "address": address,
                    "protocol": protocol,
                    "failures": max(1, int(row.get("failures") or 1)),
                    "last_failed": last_failed,
                    "next_retry": float(row.get("next_retry") or 0),
                }
            return

        # Old format was a forever-dead address list. It is intentionally not
        # migrated into active quarantine because proxies can recover later.
        legacy = data.get("dead", []) if isinstance(data, dict) else []
        if isinstance(legacy, list):
            self.legacy_migrated = len(legacy)
            if self.legacy_migrated:
                print(f"[Health] Released {self.legacy_migrated} legacy permanent-dead entries for future retest")

    def quarantined(self, address, protocol, now=None):
        now = now or time.time()
        row = self.failures.get(record_key(address, protocol))
        return bool(row and float(row.get("next_retry") or 0) > now)

    def success(self, address, protocol):
        self.failures.pop(record_key(address, protocol), None)

    def failure(self, address, protocol):
        key = record_key(address, protocol)
        now = time.time()
        old = self.failures.get(key) or {}
        count = max(0, int(old.get("failures") or 0)) + 1
        cooldown = FAILURE_COOLDOWNS[min(count - 1, len(FAILURE_COOLDOWNS) - 1)]
        self.failures[key] = {
            "address": address,
            "protocol": normalize_protocol(protocol),
            "failures": count,
            "last_failed": now,
            "next_retry": now + cooldown,
        }

    def active_count(self):
        now = time.time()
        return sum(1 for row in self.failures.values() if float(row.get("next_retry") or 0) > now)

    def save(self):
        now = time.time()
        self.failures = {
            key: row
            for key, row in self.failures.items()
            if now - float(row.get("last_failed") or 0) <= FAILURE_RECORD_TTL
        }
        active = [row for row in self.failures.values() if float(row.get("next_retry") or 0) > now]
        payload = {
            "version": 2,
            "policy": {
                "type": "temporary_quarantine",
                "cooldowns_seconds": list(FAILURE_COOLDOWNS),
                "record_ttl_seconds": FAILURE_RECORD_TTL,
                "key": "protocol|ip:port",
            },
            "failures": dict(sorted(self.failures.items())),
            "dead": sorted({row["address"] for row in active}),
            "active_count": len(active),
            "tracked_count": len(self.failures),
            "updated": now,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def dedup_records(rows):
    output = []
    seen = set()
    for row in rows:
        address = address_of(row.get("address") or row.get("proxy"))
        if not address:
            continue
        protocol = normalize_protocol(row.get("protocol"))
        key = (address, protocol)
        if key in seen:
            continue
        seen.add(key)
        output.append({
            "address": address,
            "protocol": protocol,
            "country": str(row.get("country") or "").upper(),
            "source": str(row.get("source") or ""),
        })
    return output


def rotate_take(rows, limit, slot_seconds=1800, base=0):
    if not rows or len(rows) <= limit:
        return list(rows), 0
    slot = int(time.time() // slot_seconds)
    start = (base + slot * limit) % len(rows)
    ordered = rows[start:] + rows[:start]
    return ordered[:limit], start


async def preflight_urls():
    urls = list(dict.fromkeys(HTTP_TEST_URLS + HTTPS_TEST_URLS))
    timeout = aiohttp.ClientTimeout(total=6)
    good = set()
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def check(url):
            try:
                async with session.get(url, allow_redirects=True) as resp:
                    if 200 <= resp.status < 500:
                        return url
            except Exception:
                pass
            return None

        results = await asyncio.gather(*(check(url) for url in urls))
        good.update(url for url in results if url)
    print(f"[Validate] Endpoint preflight: {len(good)}/{len(urls)} reachable")
    return good


def urls_for(protocol, reachable):
    wanted = HTTP_TEST_URLS if normalize_protocol(protocol) == "HTTP" else HTTPS_TEST_URLS
    return [url for url in wanted if url in reachable]


async def test_proxy(address, protocol, semaphore, reachable, http_session, attempts):
    urls = urls_for(protocol, reachable)
    if not urls:
        return None

    protocol = normalize_protocol(protocol)
    async with semaphore:
        for attempt in range(attempts):
            for url in urls:
                try:
                    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    if protocol in ("SOCKS4", "SOCKS5"):
                        if not HAS_SOCKS:
                            return None
                        connector = ProxyConnector.from_url(f"{protocol.lower()}://{address}")
                        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                            async with session.get(url, allow_redirects=True) as resp:
                                if 200 <= resp.status < 400:
                                    return True
                    else:
                        async with http_session.get(
                            url,
                            proxy=f"http://{address}",
                            timeout=timeout,
                            allow_redirects=True,
                        ) as resp:
                            if 200 <= resp.status < 400:
                                return True
                except Exception:
                    continue
            if attempt + 1 < attempts:
                await asyncio.sleep(0.12)
    return False


async def validate(records, health, reachable, existing=False, concurrency=POOL_CONCURRENCY):
    if not records:
        return [], [], []

    attempts = 2 if existing else 1
    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=max(250, concurrency * 2), ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 2)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as http_session:
        results = await asyncio.gather(*(
            test_proxy(row["address"], row["protocol"], semaphore, reachable, http_session, attempts)
            for row in records
        ))

    working, failed, inconclusive = [], [], []
    for row, result in zip(records, results):
        if result is True:
            health.success(row["address"], row["protocol"])
            working.append(row)
        elif result is False:
            health.failure(row["address"], row["protocol"])
            failed.append(row)
        else:
            inconclusive.append(row)
            if existing:
                working.append(row)

    # Never wipe a previously healthy pool because validation infrastructure had
    # a bad run. A true 100% collapse is much less likely than endpoint trouble.
    if existing and len(records) >= 20 and not working and len(failed) == len(records):
        print("[Validate] Safeguard triggered: preserving previous healthy pool after 100% failure")
        for row in failed:
            health.success(row["address"], row["protocol"])
        working = list(records)
        inconclusive.extend(failed)
        failed = []

    return working, failed, inconclusive


def load_country_tree():
    rows = []
    if not COUNTRY_DIR.exists():
        return rows
    for cc_dir in COUNTRY_DIR.iterdir():
        if not cc_dir.is_dir():
            continue
        country = cc_dir.name.upper()
        for path in cc_dir.iterdir():
            if not path.is_file():
                continue
            protocol = normalize_protocol(path.stem)
            if protocol not in PROTOCOLS:
                continue
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except Exception:
                continue
            for line in lines:
                address = address_of(line)
                if address:
                    rows.append({
                        "address": address,
                        "protocol": protocol,
                        "country": country,
                        "source": "existing",
                    })
    return dedup_records(rows)


def load_live_json(default_protocol="HTTP"):
    path = DATA_DIR / "live_proxies.json"
    if not path.exists():
        path = DATA_DIR / "all_proxies.json"
    rows = []
    if not path.exists():
        return rows
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return rows
    for item in data.get("proxies", []):
        if not isinstance(item, dict):
            continue
        address = address_of(item.get("proxy") or item.get("address"))
        if not address:
            continue
        rows.append({
            "address": address,
            "protocol": normalize_protocol(item.get("protocol"), default_protocol),
            "country": str(item.get("country") or "XX").upper(),
            "source": str(item.get("source") or "existing"),
        })
    return dedup_records(rows)


async def apply_country(records, only_missing=False):
    if not records:
        return 0
    ips = []
    for row in records:
        if only_missing and row.get("country") not in ("", "XX", None):
            continue
        ips.append(row["address"].rsplit(":", 1)[0])
    ips = list(dict.fromkeys(ips))
    country_map = await geolocate_ips(ips) if ips else {}
    unresolved = 0
    for row in records:
        ip = row["address"].rsplit(":", 1)[0]
        if ip in country_map:
            row["country"] = country_map[ip]
        elif row.get("country") in ("", None):
            row["country"] = "XX"
            unresolved += 1
        elif row.get("country") == "XX":
            unresolved += 1
    return unresolved


def write_country_tree(records, root_all_files=False):
    if COUNTRY_DIR.exists():
        shutil.rmtree(COUNTRY_DIR)
    COUNTRY_DIR.mkdir(parents=True, exist_ok=True)

    grouped = defaultdict(set)
    protocol_sets = defaultdict(set)
    for row in records:
        country = str(row.get("country") or "XX").upper()
        protocol = normalize_protocol(row.get("protocol"))
        grouped[(country, protocol)].add(row["address"])
        protocol_sets[protocol].add(row["address"])

    country_counts = defaultdict(int)
    protocol_counts = defaultdict(int)
    for (country, protocol), addresses in grouped.items():
        folder = COUNTRY_DIR / country
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{protocol.lower()}.txt").write_text(
            "\n".join(sorted(addresses)) + "\n", encoding="utf-8"
        )
        country_counts[country] += len(addresses)
        protocol_counts[protocol] += len(addresses)

    if root_all_files:
        for protocol in PROTOCOLS:
            addresses = sorted(protocol_sets.get(protocol, set()))
            (COUNTRY_DIR / f"all_{protocol.lower()}.txt").write_text(
                "\n".join(addresses) + "\n" if addresses else "", encoding="utf-8"
            )
        all_addresses = sorted({row["address"] for row in records})
        (COUNTRY_DIR / "all.txt").write_text(
            "\n".join(all_addresses) + "\n" if all_addresses else "", encoding="utf-8"
        )

    return dict(country_counts), dict(protocol_counts)


def write_live_json(records):
    rows = []
    for row in records:
        item = {
            "proxy": row["address"],
            "country": str(row.get("country") or "XX").upper(),
            "protocol": normalize_protocol(row.get("protocol")),
        }
        if row.get("source"):
            item["source"] = row["source"]
        rows.append(item)
    payload = {"proxies": rows, "count": len(rows), "updated": time.time()}
    (DATA_DIR / "live_proxies.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (DATA_DIR / "all_proxies.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_target(path):
    target = Path(path).resolve()
    spec = importlib.util.spec_from_file_location("proxy_target_module", target)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {target}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def effective_concurrency(module, default):
    try:
        configured = int(getattr(module, "CONCURRENCY", default) or default)
    except Exception:
        configured = default
    return min(250, max(default, configured))


async def run_source_pool(module, mode):
    print(f"[PoolV2.1] {mode}: revalidate healthy -> rotate new candidates -> merge")
    health = HealthStore()
    reachable = await preflight_urls()
    concurrency = effective_concurrency(module, POOL_CONCURRENCY)

    existing = load_country_tree()
    still, dead_existing, old_inconclusive = await validate(
        existing, health, reachable, existing=True, concurrency=concurrency
    )
    print(
        f"[Persistent] loaded={len(existing)}, healthy={len(still)}, "
        f"dead={len(dead_existing)}, inconclusive={len(old_inconclusive)}"
    )

    scraped = []
    per_source = {}
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if mode == "walla":
            sources = list(module.SOURCES)
            results = await asyncio.gather(*(
                module.scrape_source(session, src) for src in sources
            ), return_exceptions=True)
            for src, items in zip(sources, results):
                if isinstance(items, Exception):
                    items = []
                name = str(src.get("name") or src.get("id") or "source")
                per_source[name] = len(items)
                for row in items:
                    scraped.append({
                        "address": row.get("address"),
                        "protocol": row.get("protocol") or "HTTP",
                        "country": row.get("country") or "",
                        "source": name,
                    })
        else:
            scrapers = list(module.SCRAPERS)
            names = getattr(module, "SOURCE_NAMES", {})
            results = await asyncio.gather(*(
                scraper(session) for scraper in scrapers
            ), return_exceptions=True)
            for scraper, items in zip(scrapers, results):
                if isinstance(items, Exception):
                    items = []
                name = str(names.get(scraper) or getattr(scraper, "__name__", "source"))
                per_source[name] = len(items)
                for row in items:
                    scraped.append({
                        "address": row.get("address"),
                        "protocol": row.get("protocol") or "HTTP",
                        "country": row.get("country") or "",
                        "source": name,
                    })

    scraped = dedup_records(scraped)
    still_keys = {(row["address"], row["protocol"]) for row in still}
    eligible_all = []
    quarantine_skipped = 0
    for row in scraped:
        key = (row["address"], row["protocol"])
        if key in still_keys:
            continue
        if health.quarantined(row["address"], row["protocol"]):
            quarantine_skipped += 1
            continue
        eligible_all.append(row)

    candidates, rotation_start = rotate_take(
        eligible_all, MAX_POOL_NEW_PER_RUN, slot_seconds=1800
    )
    print(
        f"[New] scraped_unique={len(scraped)}, eligible_total={len(eligible_all)}, "
        f"validating={len(candidates)}, rotation_start={rotation_start}, "
        f"quarantine_skipped={quarantine_skipped}"
    )

    working_new, dead_new, new_inconclusive = await validate(
        candidates, health, reachable, existing=False, concurrency=concurrency
    )
    merged = dedup_records(still + working_new)
    unresolved = await apply_country(merged, only_missing=False)
    country_counts, protocol_counts = write_country_tree(merged, root_all_files=False)
    health.save()

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engine": "pool-v2.1",
        "health_policy": "temporary_quarantine_2h_6h_12h_24h",
        "sources": sorted(per_source),
        "per_source": dict(sorted(per_source.items(), key=lambda x: -x[1])),
        "total_scraped": len(scraped),
        "eligible_new": len(eligible_all),
        "validated": len(candidates),
        "rotation_start": rotation_start,
        "still_working": len(still),
        "working_new": len(working_new),
        "working": len(merged),
        "dead_existing": len(dead_existing),
        "dead_new": len(dead_new),
        "dead_total": health.active_count(),
        "dead_tracked": len(health.failures),
        "quarantine_skipped": quarantine_skipped,
        "inconclusive_new": len(new_inconclusive),
        "geolocated": len(merged) - unresolved,
        "stored_count": len(merged),
        "no_country_count": unresolved,
        "country_count": len(country_counts),
        "protocol_counts": dict(sorted(protocol_counts.items())),
        "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
    }
    (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


async def fetch_boost_source(url):
    if not url:
        return ""
    timeout = aiohttp.ClientTimeout(total=90)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                if resp.status != 200:
                    print(f"[BoostV2.1] source HTTP {resp.status}")
                    return ""
                return await resp.text()
    except Exception as exc:
        print(f"[BoostV2.1] source error: {exc}")
        return ""


def boost_config(module):
    if hasattr(module, "PROTO"):
        protocol = normalize_protocol(getattr(module, "PROTO"))
        url = str(getattr(module, "PROXRIPPER_URL", ""))
    elif hasattr(module, "PROXRIPPER_HTTPS"):
        protocol = "HTTPS"
        url = str(getattr(module, "PROXRIPPER_HTTPS"))
    else:
        protocol = "HTTP"
        url = str(getattr(module, "PROXRIPPER_HTTP", ""))
    skip = max(0, int(getattr(module, "SKIP_FIRST", 0) or 0))
    configured_max = max(1, int(getattr(module, "MAX_PROXIES", 50000) or 50000))
    budget = min(configured_max, MAX_BOOST_NEW_PER_RUN)
    return protocol, url, skip, budget


async def run_boost(module):
    protocol, source_url, base_skip, budget = boost_config(module)
    health = HealthStore()
    reachable = await preflight_urls()
    concurrency = effective_concurrency(module, BOOST_CONCURRENCY)
    print(
        f"[BoostV2.1] protocol={protocol}, base_skip={base_skip}, "
        f"new_budget={budget}, concurrency={concurrency}"
    )

    existing = load_live_json(protocol)
    for row in existing:
        row["protocol"] = protocol
    still, dead_existing, old_inconclusive = await validate(
        existing, health, reachable, existing=True, concurrency=concurrency
    )

    text = await fetch_boost_source(source_url)
    source_addresses = []
    seen = set()
    for line in text.splitlines():
        address = address_of(line)
        if address and address not in seen:
            seen.add(address)
            source_addresses.append(address)

    still_keys = {(row["address"], protocol) for row in still}
    eligible = []
    quarantine_skipped = 0
    for address in source_addresses:
        if (address, protocol) in still_keys:
            continue
        if health.quarantined(address, protocol):
            quarantine_skipped += 1
            continue
        eligible.append({
            "address": address,
            "protocol": protocol,
            "country": "",
            "source": "ProxRipper",
        })

    # Each child keeps its own base offset, but rotates hourly and can refill past
    # the old fixed 50k boundary. Shaikh later deduplicates overlap safely.
    if eligible:
        slot = int(time.time() // 3600)
        start = (base_skip + slot * max(1000, budget // 4)) % len(eligible)
        ordered = eligible[start:] + eligible[:start]
        candidates = ordered[:budget]
    else:
        start = 0
        candidates = []

    print(
        f"[BoostV2.1] source_unique={len(source_addresses)}, eligible={len(eligible)}, "
        f"validating={len(candidates)}, start={start}, quarantine_skipped={quarantine_skipped}"
    )
    working_new, dead_new, new_inconclusive = await validate(
        candidates, health, reachable, existing=False, concurrency=concurrency
    )

    merged = dedup_records(still + working_new)
    unresolved = await apply_country(merged, only_missing=True)
    write_live_json(merged)
    country_counts, protocol_counts = write_country_tree(merged, root_all_files=True)
    health.save()

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engine": "boost-v2.1",
        "health_policy": "temporary_quarantine_2h_6h_12h_24h",
        "protocol": protocol,
        "source_unique": len(source_addresses),
        "eligible_new": len(eligible),
        "validated": len(candidates),
        "rotation_start": start,
        "still_working": len(still),
        "working_new": len(working_new),
        "working": len(merged),
        "dead_existing": len(dead_existing),
        "dead_new": len(dead_new),
        "dead_total": health.active_count(),
        "dead_tracked": len(health.failures),
        "quarantine_skipped": quarantine_skipped,
        "inconclusive_new": len(new_inconclusive),
        "no_country_count": unresolved,
        "country_count": len(country_counts),
        "protocol_counts": dict(sorted(protocol_counts.items())),
        "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
    }
    (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


async def run_shaikh(module):
    print("[ShaikhV2.1] 10 boost repos -> persistent revalidation -> protocol normalize -> MaxMind final country")
    health = HealthStore()
    reachable = await preflight_urls()
    concurrency = effective_concurrency(module, POOL_CONCURRENCY)

    existing = load_live_json("HTTP")
    still, dead_existing, old_inconclusive = await validate(
        existing, health, reachable, existing=True, concurrency=concurrency
    )

    sources = list(module.SOURCES)
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        results = await asyncio.gather(*(
            module.fetch_live(session, repo) for repo, _default in sources
        ), return_exceptions=True)

    per_repo = {}
    child_rows = []
    for (repo, default_protocol), items in zip(sources, results):
        if isinstance(items, Exception):
            items = []
        per_repo[repo] = len(items)
        for item in items:
            if not isinstance(item, dict):
                continue
            address = address_of(item.get("proxy") or item.get("address"))
            if not address:
                continue
            child_rows.append({
                "address": address,
                "protocol": normalize_protocol(item.get("protocol"), default_protocol),
                "country": str(item.get("country") or "XX").upper(),
                "source": repo,
            })

    child_rows = dedup_records(child_rows)
    still_keys = {(row["address"], row["protocol"]) for row in still}
    new_rows = []
    seen_new = set()
    for row in child_rows:
        key = (row["address"], row["protocol"])
        if key in still_keys or key in seen_new:
            continue
        seen_new.add(key)
        # Child live output is fresh evidence that an old Shaikh failure recovered.
        health.success(row["address"], row["protocol"])
        new_rows.append(row)

    merged = dedup_records(still + new_rows)
    unresolved = await apply_country(merged, only_missing=False)
    write_live_json(merged)
    country_counts, protocol_counts = write_country_tree(merged, root_all_files=True)
    health.save()

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engine": "shaikh-v2.1",
        "health_policy": "temporary_quarantine_2h_6h_12h_24h",
        "sources": [repo for repo, _default in sources],
        "per_repo": per_repo,
        "still_working": len(still),
        "new_fetched": len(child_rows),
        "new_deduped": len(new_rows),
        "unique": len(merged),
        "dead_existing": len(dead_existing),
        "dead_total": health.active_count(),
        "dead_tracked": len(health.failures),
        "no_country_count": unresolved,
        "protocol_counts": dict(sorted(protocol_counts.items())),
        "country_count": len(country_counts),
        "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
    }
    (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


async def fallback(module):
    async def patched_geolocate_batch(*args, **kwargs):
        ips = kwargs.get("ips")
        if ips is None and args:
            ips = args[-1]
        return await geolocate_ips(ips or [])

    if hasattr(module, "geolocate_batch"):
        module.geolocate_batch = patched_geolocate_batch
    await module.main()


async def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python geo_runner.py <aggregator-script>")

    target_path = sys.argv[1]
    module = load_target(target_path)
    target_name = Path(target_path).name.lower()

    if target_name == "aggregate.py" and hasattr(module, "LIVE_JSON_URL") and hasattr(module, "SOURCES"):
        await run_shaikh(module)
    elif hasattr(module, "SCRAPERS") and hasattr(module, "SOURCE_NAMES"):
        await run_source_pool(module, "habibi")
    elif hasattr(module, "scrape_source") and hasattr(module, "SOURCES"):
        await run_source_pool(module, "walla")
    elif any(hasattr(module, name) for name in ("PROXRIPPER_URL", "PROXRIPPER_HTTP", "PROXRIPPER_HTTPS")):
        await run_boost(module)
    else:
        print("[Runner] Unknown target shape; compatibility mode")
        await fallback(module)


if __name__ == "__main__":
    asyncio.run(main())
