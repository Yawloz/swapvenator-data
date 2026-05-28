"""
SwapHunter Data Pipeline v5
Sources:
  - CashbackForex API  -> all brokers except Exness (raw swap points, quote currency per lot)
  - Exness GraphQL API -> exness-std only (CBF data incomplete for Exness majors)
  - HF Markets         -> hf-premium/pro/zero via Playwright (3 separate account types)

Swap values stored as raw points (quote currency per lot per day).
contractSize stored per entry for USD conversion in the arb engine.
USD conversion happens in the FRONTEND, NOT here.

NOTE: BlackBull Energies contractSize from CBF shows 1 — needs manual verification
against BlackBull website (likely 100 barrels/lot like other brokers).
"""

import json
import time
import urllib.request
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────
# SYMBOL CONFIG
# ─────────────────────────────────────────────────────
OUR_SYMBOLS_SET = {
    "EURUSD", "GBPUSD", "USDJPY", "GBPJPY", "USDCAD", "EURAUD", "EURJPY",
    "AUDCAD", "AUDJPY", "AUDNZD", "AUDUSD", "CADJPY", "EURCAD", "EURCHF",
    "EURGBP", "EURNZD", "GBPCAD", "GBPCHF", "NZDCAD", "NZDJPY", "NZDUSD",
    "USDCHF", "CHFJPY", "AUDCHF", "GBPNZD", "NZDCHF", "SILVER", "GOLD",
    "CADCHF", "GBPAUD", "UKOIL", "USOIL", "NATGAS",
}

CBF_SYMBOL_MAP = {
    # Metals
    "XAUUSD":    "GOLD",    "XAGUSD":    "SILVER",
    # Energies — standard names
    "XBRUSD":    "UKOIL",   "XTIUSD":    "USOIL",    "XNGUSD":    "NATGAS",
    "UKOUSD":    "UKOIL",   "USOUSD":    "USOIL",
    "BRENT":     "UKOIL",   "WTI":       "USOIL",    "NATGAS":    "NATGAS",
    "SpotBrent": "UKOIL",   "SpotCrude": "USOIL",    "NatGas":    "NATGAS",
    # Tickmill specific
    "NAT.GAS":   "NATGAS",
    # Vantage specific (NG-C already in CBF_SYMBOL_MAP via Commodities group)
    "NG-C":      "NATGAS",
    # Exness energies (canonical already)
    "UKOIL":     "UKOIL",   "USOIL":     "USOIL",
    # XM uses canonical names directly
    "GOLD":      "GOLD",    "SILVER":    "SILVER",
    # IronFX energies (SpotCmdty group)
    "BRENTCash":   "UKOIL",  "NAT.GASCash": "NATGAS",  "WTICash":     "USOIL",
    # Nulled — skip these
    "UKOUSDft":  None,      "CL-OIL":    None,
    "Rolltest":  None,      "GCM25":     None,
    "Gasoline":  None,
    # IronFX duplicate/alternate entries — skip
    "XAGUSD-":   None,      "XAUUSD-":   None,
    "XAUEUR-":   None,      "XPDUSD-":   None,       "XPTUSD-":   None,

}

# ─────────────────────────────────────────────────────
# CASHBACKFOREX BROKER CONFIG
# ─────────────────────────────────────────────────────
CBF_BASE = "https://spreads-api.cashbackforex.com/api/swapratesforbroker"

# pages: {group_name: num_pages} — only needed for paginated groups
# strip_suffix: list of suffixes to strip from symbol names
CBF_BROKERS = {
    3133: {
        "key": "icmarkets",
        "groups": ["Forex Majors", "Forex Minors", "Metals", "Energies"],
    },
    1149: {
        "key": "vantage",
        "groups": ["Forex Raw ECN", "Gold +", "Silver", "Oil", "Commodities"],
        "strip_suffix": ["+"],
    },
    970: {
        "key": "blackbull",
        "groups": ["Forex Majors", "Forex", "Commodities", "Energies"],
        # BlackBull energies: CBF API returns CS=1 for BRENT/WTI (1 unit per lot) — confirmed from CBF data.
        # NATGAS=1000 units per lot. UKOIL/USOIL stay at CBF value (1).
        "contractSize_override": {"NATGAS": 1000},
    },

    451: {
        "key": "tickmill",
        "groups": ["Forex", "CFD-Crude-Oil", "CFD-2", ""],
        "pages": {"": 2},  # page 2 of empty group has Gold/Silver
        # Tickmill energies: UKOIL/USOIL = 100 barrels per lot, NATGAS = 10000 MMBtu per lot.
        # CBF API reports wrong CS — override with real specs from broker site.
        "contractSize_override": {"UKOIL": 100, "USOIL": 100, "NATGAS": 10000},
    },
    278: {
        "key": "xm",
        "groups": ["Forex 2", "Forex 3", "Spot Metals"],
    },
    500: {
        "key": "fpmarkets",
        "groups": ["Forex R", "Metals 1 R", "Metals 2 R", "Commodity"],
        "pages": {"Forex R": 2},
    },
    957: {
        "key": "fusionmarkets",
        "groups": ["Forex", "Commodities", "Energy"],
    },
    1101: {
        "key": "eightcap",
        "groups": ["Forex", "Oil UK", "Oil US", "Metals"],
    },


    291: {
        "key": "ironfx",
        "groups": ["MajorFX", "MinorFX", "SpotCmdty", "SpotSilver", "SpotGold"],
        # SpotCmdty: only BRENTCash, NAT.GASCash, WTICash (cash energies)
        # SpotSilver: only XAGUSD (swapType "1" = InMoney)
        # SpotGold: only XAUUSD (swapType "1" = InMoney)
        # XAGUSD-, XAUUSD- etc. nulled in CBF_SYMBOL_MAP
    },
}

# ─────────────────────────────────────────────────────
# CBF FETCH
# ─────────────────────────────────────────────────────
def fetch_cbf_page(cbf_id, group, page=1, retries=3):
    import urllib.parse
    group_encoded = urllib.parse.quote(group, safe='')
    url = (f"{CBF_BASE}/{cbf_id}"
           f"?currentPage={page}&countPerPage=100&search=&group={group_encoded}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://fxverify.com",
        "Referer": "https://fxverify.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
            return data.get("swapRates", {}).get("swapRates", [])
        except Exception as e:
            print(f"    CBF error [{cbf_id}] group={group} page={page} attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return []


def strip_symbol_suffix(name, suffixes):
    for suffix in suffixes:
        if name.endswith(suffix):
            return name[:-len(suffix)]
    return name


def run_cbf():
    print("── CashbackForex ──")
    output = {}

    for cbf_id, config in CBF_BROKERS.items():
        broker_key = config["key"]
        print(f"  {broker_key} (CBF ID: {cbf_id})")
        broker_data = {}
        pages_config = config.get("pages", {})
        strip_suffixes = config.get("strip_suffix", [])
        cs_override = config.get("contractSize_override", {})
        exclude_syms = config.get("exclude_symbols", set())

        for group in config["groups"]:
            num_pages = pages_config.get(group, 1)
            for page in range(1, num_pages + 1):
                rows = fetch_cbf_page(cbf_id, group, page)
                for item in rows:
                    raw_name = item.get("name", "")

                    if strip_suffixes:
                        raw_name = strip_symbol_suffix(raw_name, strip_suffixes)

                    swap_type = item.get("swapType", "")

                    canon = CBF_SYMBOL_MAP.get(raw_name, raw_name)
                    if canon is None or canon not in OUR_SYMBOLS_SET:
                        continue
                    if canon in exclude_syms:
                        continue
                    if canon in broker_data:
                        continue  # first occurrence wins

                    lv = item.get("swapLong")
                    sv = item.get("swapShort")
                    cs = cs_override.get(canon) or item.get("contractSize")

                    # Normalize numeric swapType strings (IronFX uses "0"/"1")
                    if swap_type == "0":
                        swap_type = "InPoints"
                    elif swap_type == "1":
                        swap_type = "InMoney"

                    if swap_type == "InPoints":
                        long_val  = round(float(lv), 4) if lv is not None else None
                        short_val = round(float(sv), 4) if sv is not None else None
                    elif swap_type == "InPips":
                        pip_to_pts = 10
                        long_val  = round(float(lv) * pip_to_pts, 4) if lv is not None else None
                        short_val = round(float(sv) * pip_to_pts, 4) if sv is not None else None
                    elif swap_type == "InMoney":
                        long_val  = round(float(lv), 4) if lv is not None else None
                        short_val = round(float(sv), 4) if sv is not None else None
                    else:
                        print(f"      Unknown swapType={swap_type} for {raw_name}, skipping")
                        continue

                    broker_data[canon] = {
                        "long":         long_val,
                        "short":        short_val,
                        "contractSize": int(cs) if cs is not None else None,
                        "swapType":     swap_type,
                    }
                time.sleep(0.8)

        # Log swapTypes found for debugging
        swap_types_found = {}
        for sym_data in broker_data.values():
            t = sym_data.get("swapType", "unknown")
            swap_types_found[t] = swap_types_found.get(t, 0) + 1
        print(f"    Got {len(broker_data)} symbols | swapTypes: {swap_types_found}")
        for symbol, rates in broker_data.items():
            output.setdefault(symbol, {})[broker_key] = rates

    brokers = set(b for sym in output.values() for b in sym.keys())
    print(f"  Done. {len(output)} symbols, {len(brokers)} brokers: {sorted(brokers)}")
    return output


# ─────────────────────────────────────────────────────
# EXNESS — GraphQL (CBF missing majors for Exness)
# ─────────────────────────────────────────────────────
EXNESS_METAL_MAP = {"GOLD": "XAUUSDm", "SILVER": "XAGUSDm"}
EXNESS_SYMBOLS = [
    "EURUSD", "GBPUSD", "USDJPY", "GBPJPY", "USDCAD", "EURAUD", "EURJPY",
    "AUDCAD", "AUDJPY", "AUDNZD", "AUDUSD", "CADJPY", "EURCAD", "EURCHF",
    "EURGBP", "EURNZD", "GBPCAD", "GBPCHF", "NZDCAD", "NZDJPY", "NZDUSD",
    "USDCHF", "CHFJPY", "AUDCHF", "GBPNZD", "NZDCHF", "SILVER", "GOLD",
    "CADCHF", "GBPAUD",
]
EXNESS_CONTRACT_SIZES = {"GOLD": 100, "SILVER": 5000}  # FX default 100000

GRAPHQL_QUERY = (
    "query getTradingInstruments($account_type: String!, $instruments: [String]) {\n"
    "  tradingInstruments: allExnessAccountTypeInstruments(\n"
    "    sort: {fields: \"instrument\"}\n"
    "    account_type: $account_type\n"
    "    filter: {can_trade: true, instrument: {in: $instruments}}\n"
    "  ) {\n"
    "    instrument swap_long swap_short commission_per_lot median_spread __typename\n"
    "  }\n"
    "}"
)

def run_exness():
    print("── Exness (std, GraphQL) ──")
    instruments = [EXNESS_METAL_MAP.get(s, s + "m") for s in EXNESS_SYMBOLS]
    reverse = {EXNESS_METAL_MAP.get(s, s + "m"): s for s in EXNESS_SYMBOLS}
    payload = json.dumps({
        "operationName": "getTradingInstruments",
        "query": GRAPHQL_QUERY,
        "variables": {"account_type": "mt5_mini_real_vc", "instruments": instruments}
    }).encode()
    output = {}
    try:
        req = urllib.request.Request(
            "https://www.exness.com/pwapi/",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0",
                "Origin": "https://www.exness.com",
                "Referer": "https://www.exness.com/trading/swap-rates/",
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        for item in (data.get("data", {}).get("tradingInstruments") or []):
            canon = reverse.get(item["instrument"])
            if canon:
                output.setdefault(canon, {})["exness-std"] = {
                    "long":         round(item["swap_long"] * 10, 4),
                    "short":        round(item["swap_short"] * 10, 4),
                    "contractSize": EXNESS_CONTRACT_SIZES.get(canon, 100000),
                }
        print(f"  Got {len(output)} symbols")
    except Exception as e:
        print(f"  Error: {e}")
    return output


# ─────────────────────────────────────────────────────
# HF MARKETS — Playwright (3 account types separately)
# col mapping confirmed: sym=col[1], short=col[5], long=col[6]
# ─────────────────────────────────────────────────────
HF_URLS = [
    "https://hfeu.com/en/trading-instruments/forex",
]
HF_SYMBOL_MAP = {"XAUUSD": "GOLD", "XAGUSD": "SILVER", "USOIL.S": "USOIL", "usoil.s": "USOIL"}

# HF Markets only offers these symbols — filter out anything else
# No AUD crosses (except AUDUSD), no metals, no energies
# HF Markets confirmed symbols - exclude everything not on their platform
HF_EXCLUDED_SYMBOLS = {
    # Metals HF doesn't have
    "SILVER",
    # Energies HF doesn't have
    "UKOIL", "NATGAS",
    # AUD crosses (only AUDUSD exists)
    "AUDCAD", "AUDJPY", "AUDNZD", "AUDCHF",
    # NZD pairs not offered
    "NZDUSD", "NZDCAD", "NZDCHF",
    # EUR crosses not offered
    "EURAUD", "EURNZD",
    # GBP crosses not offered
    "GBPAUD",
    # NZD crosses
    "GBPNZD", "NZDJPY",
}

# HF contract sizes confirmed from CBF:
# FX: 100000, GOLD: 100, SILVER: 1000, UKOIL/USOIL: 100
HF_CONTRACT_SIZES = {
    "GOLD": 100, "SILVER": 1000,
    "UKOIL": 100, "USOIL": 100, "NATGAS": 10000,
}

HF_CONTAINERS = {
    "premium-table-container": "hf-premium",
    "pro-table-container":     "hf-pro",
    "zero-table-container":    "hf-zero",
}

def parse_rate(val):
    if not val or str(val).strip() in ("-", "—", "N/A", ""):
        return None
    try:
        return round(float(str(val).strip().replace(",", ".")), 4)
    except Exception:
        return None

def run_hfmarkets():
    print("── HF Markets (Playwright) ──")
    results = {}
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            )
            page = browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            for hf_url in HF_URLS:
                print(f"  Scraping {hf_url}")
                page.goto(hf_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(4)
                page.evaluate("""
                    ['cookiescript_injected_wrapper','cookiescript_injected'].forEach(id => {
                        const el = document.getElementById(id); if (el) el.remove();
                    });
                """)
                time.sleep(1)

                # Debug: dump ALL symbol names found in ALL tables on this page
                all_syms = page.evaluate("""
                    () => {
                        const rows = document.querySelectorAll('table tbody tr');
                        const syms = [];
                        rows.forEach(r => {
                            const cells = r.querySelectorAll('td');
                            if(cells.length >= 2) syms.push(cells[1].innerText.trim());
                        });
                        return [...new Set(syms)].filter(s => s.length > 0);
                    }
                """)
                print(f"    All symbols on page: {all_syms}")

                table_data = page.evaluate("""
                    (containers) => {
                        const result = {};
                        for (const [containerId, brokerKey] of Object.entries(containers)) {
                            const container = document.getElementById(containerId);
                            if (!container) { result[brokerKey] = []; continue; }
                            const rows = container.querySelectorAll('table tbody tr');
                            const rowData = [];
                            rows.forEach(row => {
                                const cells = row.querySelectorAll('td');
                                if (cells.length >= 7) {
                                    rowData.push([
                                        cells[1].innerText.trim(),
                                        cells[5].innerText.trim(),
                                        cells[6].innerText.trim()
                                    ]);
                                }
                            });
                            result[brokerKey] = rowData;
                        }
                        return result;
                    }
                """, HF_CONTAINERS)

                for broker_key, rows in table_data.items():
                    matched = 0
                    for row in rows:
                        raw_sym = row[0].upper()
                        sym = HF_SYMBOL_MAP.get(raw_sym, raw_sym)
                        if sym not in OUR_SYMBOLS_SET:
                            continue
                        if sym in HF_EXCLUDED_SYMBOLS:
                            continue
                        short = parse_rate(row[1])
                        long  = parse_rate(row[2])
                        cs = HF_CONTRACT_SIZES.get(sym, 100000)
                        results.setdefault(sym, {})[broker_key] = {
                            "long": long, "short": short, "contractSize": cs
                        }
                        matched += 1
                    print(f"  {broker_key}: {len(rows)} rows → {matched} matched")

            browser.close()
    except Exception as e:
        print(f"  Exception: {e}")

    brokers = set(b for s in results.values() for b in s.keys())
    print(f"  Done. {len(results)} symbols, brokers: {brokers}")
    return results



# ─────────────────────────────────────────────────────
# Returns USD per lot directly — stored with swapType="InMoney"
# so frontend skips swapToUSD conversion
# ─────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────
def run():
    print(f"\nSwapHunter scraper v5 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")

    cbf_data = run_cbf()
    exn_data = run_exness()
    hfm_data = run_hfmarkets()

    # Count how many CBF brokers returned 0 data
    cbf_brokers_with_data = set(b for sym in cbf_data.values() for b in sym.keys())
    cbf_failed = len(CBF_BROKERS) - len(cbf_brokers_with_data)

    # Exness SF = Exness Swap-Free account — all swaps are 0 by definition
    # Mirrors exness-std symbols/contractSizes but with long=0, short=0
    exn_sf_data = {}
    for sym, brokers in exn_data.items():
        if "exness-std" in brokers:
            exn_sf_data[sym] = {
                "exness-sf": {
                    "long": 0.0,
                    "short": 0.0,
                    "contractSize": brokers["exness-std"]["contractSize"],
                }
            }

    all_symbols = set(list(cbf_data.keys()) + list(exn_data.keys()) + list(hfm_data.keys()) + list(exn_sf_data.keys()))
    merged = {}
    for sym in all_symbols:
        merged[sym] = {}
        merged[sym].update(cbf_data.get(sym, {}))
        merged[sym].update(exn_data.get(sym, {}))
        merged[sym].update(exn_sf_data.get(sym, {}))
        merged[sym].update(hfm_data.get(sym, {}))

    brokers = set(b for sym in merged.values() for b in sym.keys())
    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "sources": sorted(brokers),
        "swaps": merged,
    }

    with open("swaps.json", "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(v) for v in merged.values())
    print(f"\n✓ Done — {len(merged)} symbols, {len(brokers)} brokers, {total} entries")
    print(f"  Brokers: {sorted(brokers)}")

    if cbf_failed > 4:
        send_alert_email(cbf_failed, cbf_brokers_with_data)


def send_alert_email(cbf_failed, cbf_brokers_with_data):
    import os
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        print("  [alert] RESEND_API_KEY not set, skipping email alert")
        return

    working = ", ".join(sorted(cbf_brokers_with_data)) if cbf_brokers_with_data else "none"
    body = (
        f"SwapVenator scraper alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        f"{cbf_failed} out of {9} CBF brokers returned no data.\n"
        f"CBF brokers with data: {working}\n\n"
        f"CashbackForex API may be down or blocking the scraper.\n"
        f"Check the GitHub Actions logs for details."
    )
    payload = json.dumps({
        "from": "SwapVenator Alert <alert@swapvenator.io>",
        "to": ["info@swapvenator.io"],
        "subject": f"[SwapVenator] Scraper alert — {cbf_failed}/9 CBF brokers failed",
        "text": body,
    }).encode()

    try:
        req = urllib.request.Request(
            "https://api.resend.com/emails",
            data=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"  [alert] Email sent (status {resp.status})")
    except urllib.error.HTTPError as e:
        body_bytes = e.read()
        print(f"  [alert] Failed to send email: {e} — {body_bytes.decode()}")


if __name__ == "__main__":
    run()
