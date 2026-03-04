import logging
import os
import time

print("SCRAPER PID:", os.getpid())

import re
import random
import asyncio
import json
from pathlib import Path
from typing import Any, Optional

import httpx
from dotenv import load_dotenv

from src.db.connect import get_conn
from src.db.queries import upsert_asins_ingest, upsert_asins_ingest_with_meta

load_dotenv()

log = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


# ----------------- helpers -----------------
def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


# ----------------- Proxy (ENV Controlled) -----------------
USE_PROXY = _env_bool("SCRAPER_USE_PROXY", False)
PROXY_URL = (os.getenv("SCRAPER_PROXY_URL", "") or os.getenv("OXylabs_PROXY_URL", "") or "").strip()

if USE_PROXY and not PROXY_URL:
    raise RuntimeError("SCRAPER_USE_PROXY=true but SCRAPER_PROXY_URL is empty/missing in .env")


# ---------------- CONFIG (env first, json second) ----------------
def _load_node_ids_from_json() -> list[str]:
    project_root = Path(__file__).resolve().parents[2]  # .../src/workers -> repo root
    nodes_path = project_root / "data" / "amazon_nodes.json"
    if not nodes_path.exists():
        return []

    try:
        data = json.loads(nodes_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    if not isinstance(data, dict):
        return []

    wanted_groups = [g.strip() for g in os.getenv("AMAZON_NODE_GROUPS", "").split(",") if g.strip()]

    node_ids: list[str] = []

    if wanted_groups:
        lower_map = {str(k).lower(): k for k in data.keys()}
        for g in wanted_groups:
            key = lower_map.get(g.lower())
            if key is None:
                continue
            vals = data.get(key)
            if isinstance(vals, list):
                node_ids.extend([str(x).strip() for x in vals])
    else:
        for vals in data.values():
            if isinstance(vals, list):
                node_ids.extend([str(x).strip() for x in vals])

    seen: set[str] = set()
    cleaned: list[str] = []
    for n in node_ids:
        if n.isdigit() and n not in seen:
            seen.add(n)
            cleaned.append(n)

    return cleaned


def _get_node_ids() -> list[str]:
    env_nodes = [x.strip() for x in os.getenv("AMAZON_NODE_IDS", "").split(",") if x.strip().isdigit()]
    if env_nodes:
        return env_nodes
    return _load_node_ids_from_json()


NODE_IDS = _get_node_ids()
if not NODE_IDS:
    raise RuntimeError("No node IDs loaded. Ensure data/amazon_nodes.json exists OR set AMAZON_NODE_IDS in .env")

BASE_URL = os.getenv("AMAZON_BASE_URL", "https://www.amazon.co.uk/s")

UA = os.getenv(
    "AMAZON_UA",
    "Mozilla/5.0 (Linux; Android 16; sdk_gphone64_x86_64 Build/BE4B.251210.005; wv) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/134.0.6998.135 Mobile Safari/537.36",
)

COOKIES = os.getenv("AMAZON_COOKIES", "")

# Concurrency / stopping rules
CONCURRENCY = int(os.getenv("SCRAPER_CONCURRENCY", "3"))
MAX_EMPTY_PAGES = int(os.getenv("SCRAPER_MAX_EMPTY_PAGES", "5"))
MAX_PAGES_PER_NODE = int(os.getenv("SCRAPER_MAX_PAGES_PER_NODE", "100"))

# Sleep jitter
MIN_SLEEP = float(os.getenv("SCRAPER_MIN_SLEEP", "0.6"))
MAX_SLEEP = float(os.getenv("SCRAPER_MAX_SLEEP", "1.2"))

# DB inserts
DB_BATCH_COMMIT = int(os.getenv("SCRAPER_DB_COMMIT_BATCH", "500"))

# Price cap (pence). £250 = 25000
PRICE_CAP_PENCE = int(os.getenv("AMAZON_PRICE_CAP_PENCE", "25000"))
PRICE_MIN_PENCE = int(os.getenv("AMAZON_PRICE_MIN_PENCE", "0"))

# Seller filter (Amazon UK seller ID)
AMAZON_SELLER_ID = os.getenv("AMAZON_SELLER_ID", "A3P5ROKL5A1OLE")

# Deals-only filter
DEALS_ONLY = _env_bool("SCRAPER_DEALS_ONLY", False)
DEAL_TYPE_RNID = os.getenv("SCRAPER_DEAL_TYPE_RNID", "26901100031").strip()  # matches your URL example

# Parse toggles
PARSE_VOUCHERS = _env_bool("SCRAPER_PARSE_VOUCHERS", True)
PARSE_SPM = _env_bool("SCRAPER_PARSE_SPM", True)
PARSE_QTY_DISCOUNT = _env_bool("SCRAPER_PARSE_QTY_DISCOUNT", True)
PARSE_PRICE = _env_bool("SCRAPER_PARSE_PRICE", True)
PARSE_RRP = _env_bool("SCRAPER_PARSE_RRP", True)


# -------------------------------------------------------------------
ASIN_RE = re.compile(r'data-asin="([A-Z0-9]{10})"')
SPLIT_CARD_RE = re.compile(r'data-component-type="s-search-result"')

# ---------- Voucher tile patterns (search faceout) ----------
# Currency symbol can appear as literal £, &pound;, or unicode \u00a3.
_CCY = r"(?:£|&pound;|\u00a3)"

# These are intentionally resilient: they look for "Voucher/Coupon price" or "Saving/Save"
# and then the *next* a-offscreen £amount within that coupon region.
VOUCHER_PRICE_RE = re.compile(
    rf"(?:Voucher|Coupon)\s*price.*?a-offscreen\">\s*{_CCCY if False else _CCY}\s*([0-9]+(?:\.[0-9]{{1,2}})?)\s*<",
    re.IGNORECASE | re.DOTALL,
)
VOUCHER_SAVING_RE = re.compile(
    rf"(?:Saving|Save).*?a-offscreen\">\s*{_CCY}\s*([0-9]+(?:\.[0-9]{{1,2}})?)\s*<",
    re.IGNORECASE | re.DOTALL,
)

# Bought in past month patterns: "30K+ bought in past month", "500+ bought in past month"
BOUGHT_PAST_MONTH_RE = re.compile(
    r'([0-9]{1,3}(?:[.,][0-9]{1,3})?)(\s*[Kk])?\s*\+\s*bought in past month',
    re.IGNORECASE,
)

# Quantity discount patterns (best-effort)
QTY_DISC_RE_1 = re.compile(
    r"Save\s*([0-9]{1,2})%\s*when you buy\s*([0-9]{1,3})",
    re.IGNORECASE,
)
QTY_DISC_RE_2 = re.compile(
    r"Buy\s*([0-9]{1,3})\s*for\s*£\s*([0-9]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE,
)

# Price / RRP patterns (scoped to price-recipe region)
# We parse the FIRST a-price a-offscreen inside price-recipe as the displayed buy price.
PRICE_RECIPE_REGION_RE = re.compile(r'data-cy="price-recipe"', re.IGNORECASE)
PRICE_FROM_PRICE_RECIPE_RE = re.compile(
    rf'class="a-price"[^>]*>.*?<span class="a-offscreen">\s*{_CCY}\s*([0-9]+(?:\.[0-9]{{1,2}})?)\s*</span>',
    re.IGNORECASE | re.DOTALL,
)

# RRP can appear as "RRP: £9.99" in offscreen text and/or visible strike price.
RRP_RE_1 = re.compile(
    rf'a-offscreen">\s*RRP:\s*{_CCY}\s*([0-9]+(?:\.[0-9]{{1,2}})?)\s*<',
    re.IGNORECASE | re.DOTALL,
)
RRP_RE_2 = re.compile(
    rf'RRP:\s*</span>\s*<span class="a-price[^"]*"[^>]*>\s*<span class="a-offscreen">\s*{_CCY}\s*([0-9]+(?:\.[0-9]{{1,2}})?)\s*</span>',
    re.IGNORECASE | re.DOTALL,
)


def _is_captcha_page(text: str) -> bool:
    if not text:
        return False
    return (
        "Enter the characters you see below" in text
        or "api-services-support@amazon.com" in text
        or "/errors/validateCaptcha" in text
    )


def _build_rh(node_id: str) -> str:
    """
    Builds rh like:
      n:117332031,p_36:0-25000,p_6:A3P5ROKL5A1OLE[,p_n_deal_type:26901100031]
    """
    parts = [
        f"n:{node_id}",
        f"p_36:{PRICE_MIN_PENCE}-{PRICE_CAP_PENCE}",
        f"p_6:{AMAZON_SELLER_ID}",
    ]
    if DEALS_ONLY and DEAL_TYPE_RNID.isdigit():
        parts.append(f"p_n_deal_type:{DEAL_TYPE_RNID}")
    return ",".join(parts)


def extract_asins(html: str) -> list[str]:
    asins = ASIN_RE.findall(html or "")
    return [a for a in asins if a != "0000000000"]


def _parse_int_from_bought_text(num_str: str, is_k: bool) -> int:
    # "30" + K => 30000; "74.3" + K => 74300
    s = num_str.replace(",", ".")
    try:
        val = float(s)
    except Exception:
        return 0
    return int(val * 1000) if is_k else int(val)


def _split_cards(html: str) -> list[str]:
    if not html:
        return []
    starts = [m.start() for m in SPLIT_CARD_RE.finditer(html)]
    if not starts:
        return []
    chunks: list[str] = []
    for i, st in enumerate(starts):
        en = starts[i + 1] if i + 1 < len(starts) else len(html)
        chunks.append(html[st:en])
    return chunks


def _coupon_region(chunk: str) -> str:
    """
    Scope voucher parsing to the coupon tile only.
    Amazon often repeats the same words elsewhere; this prevents false matches.
    """
    i = chunk.lower().find("s-coupon-tile-container")
    if i == -1:
        # fallback to coupon component if tile container class naming changes
        i = chunk.lower().find("s-coupon-component")
        if i == -1:
            return ""
    return chunk[i : i + 8000]  # big enough to include voucher + saving blocks


def _price_region(chunk: str) -> str:
    """
    Scope price parsing to the price recipe section to avoid matching unrelated prices
    (e.g. unit price, ATC payloads, etc.)
    """
    m = PRICE_RECIPE_REGION_RE.search(chunk)
    if not m:
        return ""
    i = m.start()
    return chunk[i : i + 12000]


def _parse_float_price(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s.replace(",", ""))
    except Exception:
        return None


def _extract_price_and_rrp_from_chunk(chunk: str) -> dict[str, float]:
    out: dict[str, float] = {}
    region = _price_region(chunk)
    if not region:
        return out

    # Current buy price (first a-price offscreen inside price-recipe)
    if PARSE_PRICE:
        m_price = PRICE_FROM_PRICE_RECIPE_RE.search(region)
        if m_price:
            v = _parse_float_price(m_price.group(1))
            if v is not None:
                out["price_gbp"] = v

    # RRP (explicitly labelled)
    if PARSE_RRP:
        m_rrp = RRP_RE_1.search(region) or RRP_RE_2.search(region)
        if m_rrp:
            v = _parse_float_price(m_rrp.group(1))
            if v is not None:
                out["rrp_gbp"] = v

    return out


def extract_faceout_metadata(html: str) -> dict[str, dict[str, Any]]:
    """
    Returns:
      { asin: {
          asin,
          price_gbp?,
          rrp_gbp?,
          voucher_price_gbp?,
          voucher_saving_gbp?,
          bought_past_month_min?,
          qty_discount_percent?,
          qty_discount_min_qty?,
          qty_discount_bundle_price_gbp?
        }
      }

    Notes:
    - ASIN is both the dict key and explicitly stored in meta["asin"].
    - "bought_past_month_min" is a minimum estimate (e.g. "30K+" => 30000).
    """
    out: dict[str, dict[str, Any]] = {}

    for chunk in _split_cards(html):
        m_asin = ASIN_RE.search(chunk)
        if not m_asin:
            continue
        asin = m_asin.group(1)
        if asin == "0000000000":
            continue

        meta: dict[str, Any] = {"asin": asin}

        # ---- price / rrp ----
        price_meta = _extract_price_and_rrp_from_chunk(chunk)
        if price_meta:
            meta.update(price_meta)

        # ---- vouchers ----
        if PARSE_VOUCHERS:
            coupon = _coupon_region(chunk)
            if coupon:
                m_vp = VOUCHER_PRICE_RE.search(coupon)
                if m_vp:
                    v = _parse_float_price(m_vp.group(1))
                    if v is not None:
                        meta["voucher_price_gbp"] = v

                m_vs = VOUCHER_SAVING_RE.search(coupon)
                if m_vs:
                    v = _parse_float_price(m_vs.group(1))
                    if v is not None:
                        meta["voucher_saving_gbp"] = v

        # ---- bought past month ----
        if PARSE_SPM:
            m_spm = BOUGHT_PAST_MONTH_RE.search(chunk)
            if m_spm:
                num = m_spm.group(1)
                is_k = bool(m_spm.group(2))
                spm_val = _parse_int_from_bought_text(num, is_k)
                if spm_val > 0:
                    meta["bought_past_month_min"] = spm_val

        # ---- qty discount ----
        if PARSE_QTY_DISCOUNT:
            m1 = QTY_DISC_RE_1.search(chunk)
            if m1:
                meta["qty_discount_percent"] = int(m1.group(1))
                meta["qty_discount_min_qty"] = int(m1.group(2))
            else:
                m2 = QTY_DISC_RE_2.search(chunk)
                if m2:
                    meta["qty_discount_min_qty"] = int(m2.group(1))
                    v = _parse_float_price(m2.group(2))
                    if v is not None:
                        meta["qty_discount_bundle_price_gbp"] = v

        # Keep entries if we captured anything meaningful beyond just asin
        if len(meta) > 1:
            out[asin] = meta

    return out


def upsert_amazon_faceout(conn, rows: list[dict[str, Any]]) -> None:
    """Upsert scraper faceout data into amazon_faceout, tolerating schema variants.

    Supports both old columns (price_gbp, voucher_* etc.) and newer columns
    (buy_price_gbp, shown_price_gbp, pct_drop, voucher_text).
    """
    if not rows:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'amazon_faceout'
            """
        )
        cols = {r[0] for r in (cur.fetchall() or [])}

        if not cols or 'asin' not in cols:
            return

        payloads: list[dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict) or not r.get('asin'):
                continue
            d = dict(r)
            price = d.get('price_gbp')
            if price is None:
                price = d.get('buy_price_gbp')
            rrp = d.get('rrp_gbp')
            pct_drop = None
            try:
                if price is not None and rrp is not None and float(rrp) > 0:
                    pct_drop = max(0.0, ((float(rrp) - float(price)) / float(rrp)) * 100.0)
            except Exception:
                pct_drop = None

            voucher_bits = []
            if d.get('voucher_price_gbp') is not None:
                voucher_bits.append(f"voucher_price=£{float(d['voucher_price_gbp']):.2f}")
            if d.get('voucher_saving_gbp') is not None:
                voucher_bits.append(f"voucher_save=£{float(d['voucher_saving_gbp']):.2f}")

            out = {
                'asin': str(d['asin']).upper(),
                'source': d.get('source'),
                'node_id': d.get('node_id'),
                'page': d.get('page'),
                'bought_past_month_min': d.get('bought_past_month_min'),
                'price_gbp': price,
                'buy_price_gbp': price,
                'shown_price_gbp': price,
                'rrp_gbp': rrp,
                'pct_drop': pct_drop,
                'voucher_text': ('; '.join(voucher_bits) if voucher_bits else None),
                'voucher_price_gbp': d.get('voucher_price_gbp'),
                'voucher_saving_gbp': d.get('voucher_saving_gbp'),
                'qty_discount_percent': d.get('qty_discount_percent'),
                'qty_discount_min_qty': d.get('qty_discount_min_qty'),
                'qty_discount_bundle_price_gbp': d.get('qty_discount_bundle_price_gbp'),
                'raw': d.get('raw'),
            }
            payloads.append(out)

        if not payloads:
            return

        # Build INSERT dynamically from actual table columns.
        ordered_cols = [c for c in [
            'asin','last_scraped_at','source','node_id','page','buy_price_gbp','shown_price_gbp','price_gbp','rrp_gbp','pct_drop',
            'bought_past_month_min','voucher_text','voucher_price_gbp','voucher_saving_gbp',
            'qty_discount_percent','qty_discount_min_qty','qty_discount_bundle_price_gbp','raw'
        ] if c in cols]

        if 'last_scraped_at' in ordered_cols:
            pass
        elif 'last_scraped_at' in cols:
            ordered_cols.insert(1,'last_scraped_at')

        val_exprs = []
        for c in ordered_cols:
            if c == 'last_scraped_at':
                val_exprs.append('NOW()')
            elif c == 'raw':
                val_exprs.append('%(raw)s::jsonb')
            else:
                val_exprs.append(f'%({c})s')

        update_cols = [c for c in ordered_cols if c not in {'asin'}]
        set_parts = []
        for c in update_cols:
            if c == 'last_scraped_at':
                set_parts.append('last_scraped_at = EXCLUDED.last_scraped_at')
            elif c in {'source','node_id','page'}:
                set_parts.append(f"{c} = COALESCE(EXCLUDED.{c}, amazon_faceout.{c})")
            elif c == 'raw':
                set_parts.append('raw = COALESCE(EXCLUDED.raw, amazon_faceout.raw)')
            else:
                set_parts.append(f"{c} = COALESCE(EXCLUDED.{c}, amazon_faceout.{c})")

        sql = f"""
        INSERT INTO amazon_faceout ({', '.join(ordered_cols)})
        VALUES ({', '.join(val_exprs)})
        ON CONFLICT (asin) DO UPDATE SET
          {', '.join(set_parts)};
        """
        cur.executemany(sql, payloads)


async def scrape_page(
    client: httpx.AsyncClient,
    page: int,
    sem: asyncio.Semaphore,
    node_id: str,
) -> tuple[int, list[str], dict[str, dict[str, Any]], Optional[str]]:
    async with sem:
        params = {
            "rh": _build_rh(node_id),
            "page": str(page),
            "ni": "1",
            "language": "en_GB",
            "s": "exact-aware-popularity-rank",
            "dc": "1",
        }

        headers = {
            "user-agent": UA,
            "x-requested-with": "com.amazon.mShop.android.shopping",
        }
        if COOKIES:
            headers["cookie"] = COOKIES

        try:
            r = await client.get(BASE_URL, params=params, headers=headers)

            if r.status_code != 200:
                return page, [], {}, f"HTTP {r.status_code}"

            if _is_captcha_page(r.text):
                return page, [], {}, "CAPTCHA"

            asins = extract_asins(r.text)
            faceout = extract_faceout_metadata(r.text)

            # Fallback: if a card had ASIN but faceout metadata missed it entirely, create minimal entry
            # with bought_past_month if possible (best effort by whole-page not done here to avoid false matches).
            return page, asins, faceout, None

        except Exception:
            return page, [], {}, "ERR"


async def _db_upsert_async(asins: list[str], faceout_rows: list[dict[str, Any]], source_name: str) -> None:
    def _do():
        with get_conn() as conn:
            if asins:
                # Prefer meta-aware ingest so "bought in past month" can flow into the pipeline (asin_ingest -> asins.spm_scraped).
                spm_map = {
                    r.get("asin"): r.get("bought_past_month_min")
                    for r in (faceout_rows or [])
                    if isinstance(r, dict)
                }
                rows = [
                    (a, None, (int(spm_map.get(a)) if spm_map.get(a) is not None else None))
                    for a in asins
                ]
                upsert_asins_ingest_with_meta(conn, rows, source=source_name)

            if faceout_rows:
                upsert_amazon_faceout(conn, faceout_rows)

            conn.commit()

    await asyncio.to_thread(_do)


async def run() -> None:
    found_set: set[str] = set()
    total_inserted = 0

    start_time = time.time()
    sem = asyncio.Semaphore(CONCURRENCY)

    proxy_status = "YES" if USE_PROXY else "NO"
    print("\n✅ Starting Amazon ASIN seed scrape")
    print(f"✅ Categories (node IDs): {len(NODE_IDS)}")
    print(f"✅ Price filter: £{PRICE_MIN_PENCE/100:.2f} - £{PRICE_CAP_PENCE/100:.2f}")
    print(f"✅ Amazon-only seller filter: p_6:{AMAZON_SELLER_ID}")
    print(f"✅ Deals-only filter: {'YES' if DEALS_ONLY else 'NO'} (p_n_deal_type:{DEAL_TYPE_RNID})")
    print(
        f"✅ Parse: price={PARSE_PRICE} rrp={PARSE_RRP} "
        f"vouchers={PARSE_VOUCHERS} spm={PARSE_SPM} qty_discount={PARSE_QTY_DISCOUNT}"
    )
    print(f"✅ Max pages per category: {MAX_PAGES_PER_NODE}")
    print(f"✅ Concurrency: {CONCURRENCY}")
    print(f"✅ Empty stop streak: {MAX_EMPTY_PAGES}")
    print(f"✅ DB commit batch: {DB_BATCH_COMMIT}")
    print(f"✅ Proxy enabled: {proxy_status}\n")

    captcha_hit = False
    proxy_arg = PROXY_URL if USE_PROXY else None

    async with httpx.AsyncClient(
        http2=True,
        timeout=20,
        follow_redirects=True,
        proxy=proxy_arg,
    ) as client:
        for node_id in NODE_IDS:
            page = 1
            empty_streak = 0
            source_name = f"cat_{node_id}_cap_{PRICE_CAP_PENCE}_amazononly" + ("_dealsonly" if DEALS_ONLY else "")
            pending_to_db: list[str] = []
            pending_faceout_rows: list[dict[str, Any]] = []

            print(f"\n--- 🚀 AMAZON CATEGORY ASIN SCRAPE START | node={node_id} ---")
            print(f"    rh={_build_rh(node_id)}")

            while empty_streak < MAX_EMPTY_PAGES and page <= MAX_PAGES_PER_NODE:
                tasks = [
                    scrape_page(client, page + i, sem, node_id)
                    for i in range(CONCURRENCY)
                    if (page + i) <= MAX_PAGES_PER_NODE
                ]

                results = await asyncio.gather(*tasks)

                batch_new = 0

                for p, asins, faceout_map, err in results:
                    if err:
                        print(f"[Node {node_id} | Page {p}] ⚠️ {err}")
                        if err == "CAPTCHA":
                            print("🛑 CAPTCHA hit — STOPPING ENTIRE SCRAPER to protect session/IP.")
                            captcha_hit = True
                            break
                        continue

                    new_asins: list[str] = []
                    for a in set(asins):
                        if a not in found_set:
                            found_set.add(a)
                            new_asins.append(a)

                    if new_asins:
                        pending_to_db.extend(new_asins)
                        batch_new += len(new_asins)

                    if faceout_map:
                        for asin, meta in faceout_map.items():
                            # Ensure bought_past_month exists if parser found it; if not, leave None.
                            pending_faceout_rows.append(
                                {
                                    "asin": asin,
                                    "source": source_name,
                                    "node_id": node_id,
                                    "page": p,
                                    "bought_past_month_min": meta.get("bought_past_month_min"),
                                    "price_gbp": meta.get("price_gbp"),
                                    "rrp_gbp": meta.get("rrp_gbp"),
                                    "voucher_price_gbp": meta.get("voucher_price_gbp"),
                                    "voucher_saving_gbp": meta.get("voucher_saving_gbp"),
                                    "qty_discount_percent": meta.get("qty_discount_percent"),
                                    "qty_discount_min_qty": meta.get("qty_discount_min_qty"),
                                    "qty_discount_bundle_price_gbp": meta.get("qty_discount_bundle_price_gbp"),
                                    "raw": json.dumps(meta),
                                }
                            )

                    print(
                        f"[Node {node_id} | Page {p}] +{len(new_asins)} new (total {len(found_set)}) "
                        f"| faceout={len(faceout_map)}"
                    )

                if captcha_hit:
                    break

                if batch_new == 0:
                    empty_streak += 1
                    print(f"⚠️ No new ASINs this batch. Empty streak: {empty_streak}/{MAX_EMPTY_PAGES}")
                else:
                    empty_streak = 0

                if len(pending_to_db) >= DB_BATCH_COMMIT or len(pending_faceout_rows) >= (DB_BATCH_COMMIT * 2):
                    chunk_asins = pending_to_db[:]
                    pending_to_db.clear()

                    chunk_faceout = pending_faceout_rows[:]
                    pending_faceout_rows.clear()

                    await _db_upsert_async(chunk_asins, chunk_faceout, source_name)
                    total_inserted += len(chunk_asins)
                    print(
                        f"✅ Inserted {len(chunk_asins)} ASINs into asin_ingest "
                        f"+ upserted {len(chunk_faceout)} faceout rows (source={source_name})"
                    )

                page += CONCURRENCY
                await asyncio.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

            if pending_to_db or pending_faceout_rows:
                await _db_upsert_async(pending_to_db, pending_faceout_rows, source_name)
                total_inserted += len(pending_to_db)
                print(
                    f"✅ Final insert: {len(pending_to_db)} ASINs into asin_ingest "
                    f"+ upserted {len(pending_faceout_rows)} faceout rows (source={source_name})"
                )

            if captcha_hit:
                break

            print(f"✅ Finished node={node_id} | Total ASINs so far: {len(found_set)}")

    print("\n======================")
    print("✅ AMAZON SCRAPER DONE")
    print("======================")
    print(f"✅ Unique ASINs captured: {len(found_set)}")
    print(f"✅ Approx inserted to DB: {total_inserted}")
    print(f"⏱️ Runtime: {round(time.time() - start_time, 2)}s")

    if captcha_hit:
        print("⚠️ Stopped early because CAPTCHA was detected.")


if __name__ == "__main__":
    asyncio.run(run())