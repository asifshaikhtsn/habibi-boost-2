import asyncio
import importlib.util
import sys
import urllib.request
from pathlib import Path

PROTO = "http"
PROXRIPPER_URL = "https://raw.githubusercontent.com/Mohammedcha/ProxRipper/main/full_proxies/http.txt"
SKIP_FIRST = 100000
MAX_PROXIES = 50000
CONCURRENCY = 150


def _load_shared_engine():
    root = Path(__file__).resolve().parent
    files = {
        "geo_country.py": "https://raw.githubusercontent.com/asifshaikhtsn/Walla/main/geo_country.py",
        "geo_runner.py": "https://raw.githubusercontent.com/asifshaikhtsn/Walla/main/geo_runner.py",
    }
    for name, url in files.items():
        path = root / name
        if not path.exists():
            urllib.request.urlretrieve(url, path)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    spec = importlib.util.spec_from_file_location("geo_runner", root / "geo_runner.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def main():
    engine = _load_shared_engine()
    await engine.run_boost(sys.modules[__name__])


if __name__ == "__main__":
    asyncio.run(main())
