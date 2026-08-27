# habibi-boost-2 - ProxRipper THIRD 50k Booster

Third 50k proxy booster (100k-150k) from [ProxRipper](https://github.com/Mohammedcha/ProxRipper).

- **Source:** `https://raw.githubusercontent.com/Mohammedcha/ProxRipper/main/full_proxies/http.txt` - **skip first 100k, take remaining ~50k (100,000-150,126)**
- **Pipeline:** Load persistent dead list -> fetch third 50k -> dead-first filter (remove already dead before validate) -> validate via `httpbin.org/ip` (100 concurrency) -> update dead list (never deleted) -> geolocate working via `ip-api.com/batch` -> save `data/live_proxies.json`, `data/dead_proxies.json`, `country/<CC>/http.txt`
- **Schedule:** Every 1 hour + manual dispatch
- **Dead list:** Shared logic with `habibi-boost`/`habibi-boost-1` (initially copied 49k dead), persistent per repo

## Data
- `data/dead_proxies.json` - persistent dead proxies (never deleted)
- `data/live_proxies.json` - working proxies with country
- `country/<CC>/http.txt` - per-country sorted
- `country/all_http.txt` - all working
