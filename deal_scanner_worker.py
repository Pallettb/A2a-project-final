import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sp_api.api import Products
from sp_api.base import Marketplaces

load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("deal_scanner")

AMAZON_SELLER_IDS = {
    "A3P5ROKL5A1OLE",  # Amazon UK
    "ATVPDKIKX0DER",  # Amazon US
    "A1PA6795UKMFR9",  # Amazon DE
    "A13V1IB3VIYZZH",  # Amazon FR
    "APJ6JRA9NG5V4",  # Amazon IT
    "A1AT7YVPFBWXBL",  # Amazon ES
}


@dataclass
class SpApiApp:
    name: str
    credentials: dict[str, Optional[str]]
    min_interval_sec: float
    last_call_ts: float = 0.0

    async def throttle(self) -> None:
        now = time.time()
        wait_for = self.min_interval_sec - (now - self.last_call_ts)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        self.last_call_ts = time.time()


class DealScanner:
    def __init__(self) -> None:
        self.database_url = os.getenv("DATABASE_URL")
        if not self.database_url:
            raise RuntimeError("DATABASE_URL is required")

        self.marketplace = getattr(Marketplaces, os.getenv("SPAPI_MARKETPLACE", "UK").upper(), Marketplaces.UK)
        self.min_roi = float(os.getenv("MIN_ROI", "0.20"))
        self.third_party_min_sellers = int(os.getenv("THIRD_PARTY_MIN_SELLERS", "3"))
        self.a2a_min_sellers = int(os.getenv("A2A_MIN_SELLERS", "1"))
        self.keepa_ttl_days = int(os.getenv("KEEPA_CACHE_TTL_DAYS", "30"))
        self.batch_size = int(os.getenv("SCANNER_BATCH_SIZE", "20"))
        self.poll_limit = int(os.getenv("SCANNER_POLL_LIMIT", "200"))
        self.spapi_apps = self._load_spapi_apps()

        self.keepa_key = os.getenv("KEEPA_API_KEY", "")
        self.keepa_domain = int(os.getenv("KEEPA_DOMAIN", "2"))  # UK

        self.a2a_webhook = os.getenv("DISCORD_WEBHOOK_A2A", "")
        self.third_party_webhook = os.getenv("DISCORD_WEBHOOK_3P", "")
        self.business_webhook = os.getenv("DISCORD_WEBHOOK_BUSINESS", "")

    def _load_spapi_apps(self) -> list[SpApiApp]:
        apps: list[SpApiApp] = []
        min_interval_sec = float(os.getenv("SPAPI_MIN_INTERVAL_SEC", "0.1"))
        app_count = int(os.getenv("SPAPI_APP_COUNT", "10"))

        for i in range(1, app_count + 1):
            prefix = f"SPAPI_APP_{i}_"
            creds = {
                "refresh_token": os.getenv(prefix + "REFRESH_TOKEN"),
                "lwa_app_id": os.getenv(prefix + "LWA_APP_ID"),
                "lwa_client_secret": os.getenv(prefix + "LWA_CLIENT_SECRET"),
                "aws_access_key": os.getenv(prefix + "AWS_ACCESS_KEY"),
                "aws_secret_key": os.getenv(prefix + "AWS_SECRET_KEY"),
                "role_arn": os.getenv(prefix + "ROLE_ARN"),
            }
            if all(creds.values()):
                apps.append(SpApiApp(name=f"app_{i}", credentials=creds, min_interval_sec=min_interval_sec))

        if not apps:
            # fallback to legacy single-app env naming
            creds = {
                "refresh_token": os.getenv("refresh_token"),
                "lwa_app_id": os.getenv("lwa_app_id"),
                "lwa_client_secret": os.getenv("lwa_client_secret"),
                "aws_access_key": os.getenv("aws_access_key"),
                "aws_secret_key": os.getenv("aws_secret_key"),
                "role_arn": os.getenv("role_arn"),
            }
            if all(v for k, v in creds.items() if k != "role_arn"):
                apps.append(SpApiApp(name="app_fallback", credentials=creds, min_interval_sec=min_interval_sec))

        if not apps:
            raise RuntimeError("No SP-API credentials found. Configure SPAPI_APP_1_*..SPAPI_APP_10_* or legacy vars.")

        return apps

    def _conn(self):
        return psycopg2.connect(self.database_url)

    def ensure_tables(self) -> None:
        sql = """
        CREATE TABLE IF NOT EXISTS asin_spapi_scan (
          asin TEXT PRIMARY KEY,
          seller_count INTEGER,
          seller_ids JSONB,
          offers JSONB,
          referral_fee_gbp NUMERIC,
          fba_fee_gbp NUMERIC,
          total_fee_gbp NUMERIC,
          quantity_discounts JSONB,
          scanned_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS asin_keepa_cache (
          asin TEXT PRIMARY KEY,
          avg90_price_gbp NUMERIC,
          sales_rank INTEGER,
          sales_per_month INTEGER,
          fetched_at TIMESTAMPTZ NOT NULL,
          valid_until TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS asin_deal_state (
          asin TEXT PRIMARY KEY,
          buy_price_gbp NUMERIC,
          effective_buy_price_gbp NUMERIC,
          avg90_price_gbp NUMERIC,
          sales_rank INTEGER,
          sales_per_month INTEGER,
          roi NUMERIC,
          profitable BOOLEAN NOT NULL DEFAULT FALSE,
          channel TEXT,
          reasons JSONB,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    def load_candidates(self) -> list[dict[str, Any]]:
        sql = """
        SELECT af.asin,
               af.buy_price_gbp,
               af.price_gbp,
               af.bought_past_month_min,
               af.last_scraped_at
        FROM amazon_faceout af
        WHERE COALESCE(af.buy_price_gbp, af.price_gbp) IS NOT NULL
        ORDER BY af.last_scraped_at DESC NULLS LAST
        LIMIT %s
        """
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (self.poll_limit,))
            return list(cur.fetchall())

    def load_keepa_cache(self, asin: str) -> Optional[dict[str, Any]]:
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM asin_keepa_cache WHERE asin=%s AND valid_until > NOW()",
                (asin,),
            )
            return cur.fetchone()

    def upsert_keepa_cache(self, asin: str, avg90: Optional[float], rank: Optional[int], spm: Optional[int]) -> None:
        now = datetime.now(timezone.utc)
        valid_until = now + timedelta(days=self.keepa_ttl_days)
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO asin_keepa_cache (asin, avg90_price_gbp, sales_rank, sales_per_month, fetched_at, valid_until)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (asin) DO UPDATE SET
                  avg90_price_gbp=EXCLUDED.avg90_price_gbp,
                  sales_rank=EXCLUDED.sales_rank,
                  sales_per_month=EXCLUDED.sales_per_month,
                  fetched_at=EXCLUDED.fetched_at,
                  valid_until=EXCLUDED.valid_until
                """,
                (asin, avg90, rank, spm, now, valid_until),
            )
            conn.commit()

    def upsert_spapi_scan(self, asin: str, scan: dict[str, Any]) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO asin_spapi_scan (
                  asin, seller_count, seller_ids, offers,
                  referral_fee_gbp, fba_fee_gbp, total_fee_gbp,
                  quantity_discounts, scanned_at
                ) VALUES (%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s::jsonb,NOW())
                ON CONFLICT (asin) DO UPDATE SET
                  seller_count=EXCLUDED.seller_count,
                  seller_ids=EXCLUDED.seller_ids,
                  offers=EXCLUDED.offers,
                  referral_fee_gbp=EXCLUDED.referral_fee_gbp,
                  fba_fee_gbp=EXCLUDED.fba_fee_gbp,
                  total_fee_gbp=EXCLUDED.total_fee_gbp,
                  quantity_discounts=EXCLUDED.quantity_discounts,
                  scanned_at=NOW()
                """,
                (
                    asin,
                    scan.get("seller_count"),
                    json.dumps(scan.get("seller_ids", [])),
                    json.dumps(scan.get("offers", [])),
                    scan.get("referral_fee_gbp"),
                    scan.get("fba_fee_gbp"),
                    scan.get("total_fee_gbp"),
                    json.dumps(scan.get("quantity_discounts", [])),
                ),
            )
            conn.commit()

    def upsert_deal_state(self, asin: str, payload: dict[str, Any]) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO asin_deal_state (
                  asin, buy_price_gbp, effective_buy_price_gbp, avg90_price_gbp,
                  sales_rank, sales_per_month, roi, profitable, channel, reasons, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                ON CONFLICT (asin) DO UPDATE SET
                  buy_price_gbp=EXCLUDED.buy_price_gbp,
                  effective_buy_price_gbp=EXCLUDED.effective_buy_price_gbp,
                  avg90_price_gbp=EXCLUDED.avg90_price_gbp,
                  sales_rank=EXCLUDED.sales_rank,
                  sales_per_month=EXCLUDED.sales_per_month,
                  roi=EXCLUDED.roi,
                  profitable=EXCLUDED.profitable,
                  channel=EXCLUDED.channel,
                  reasons=EXCLUDED.reasons,
                  updated_at=NOW()
                """,
                (
                    asin,
                    payload.get("buy_price_gbp"),
                    payload.get("effective_buy_price_gbp"),
                    payload.get("avg90_price_gbp"),
                    payload.get("sales_rank"),
                    payload.get("sales_per_month"),
                    payload.get("roi"),
                    payload.get("profitable", False),
                    payload.get("channel"),
                    json.dumps(payload.get("reasons", [])),
                ),
            )
            conn.commit()

    async def fetch_keepa(self, asin: str) -> dict[str, Optional[float]]:
        if not self.keepa_key:
            return {"avg90_price_gbp": None, "sales_rank": None, "sales_per_month": None}

        params = {
            "key": self.keepa_key,
            "domain": self.keepa_domain,
            "asin": asin,
            "stats": "90",
            "buybox": 1,
        }
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get("https://api.keepa.com/product", params=params)
            r.raise_for_status()
            payload = r.json()

        products = payload.get("products", [])
        if not products:
            return {"avg90_price_gbp": None, "sales_rank": None, "sales_per_month": None}

        p = products[0]
        stats = p.get("stats", {}) or {}

        avg90 = None
        avg90_arr = stats.get("avg90")
        if isinstance(avg90_arr, list) and avg90_arr:
            v = avg90_arr[0]
            if isinstance(v, (int, float)) and v > 0:
                avg90 = round(v / 100.0, 2)

        sales_rank = None
        rank = p.get("salesRankReference") or p.get("salesRank")
        if isinstance(rank, int) and rank > 0:
            sales_rank = rank

        spm = p.get("monthlySold")
        if isinstance(spm, int) and spm < 0:
            spm = None

        return {"avg90_price_gbp": avg90, "sales_rank": sales_rank, "sales_per_month": spm}

    async def fetch_spapi_offers(self, app: SpApiApp, asin: str) -> dict[str, Any]:
        await app.throttle()
        products = Products(credentials=app.credentials, marketplace=self.marketplace)
        response = products.get_item_offers(
            asin=asin,
            item_condition="New",
            marketplace_id=self.marketplace.marketplace_id,
            CustomerType="Business",
        )
        payload = response.payload or {}
        offers = payload.get("Offers", []) or []

        parsed_offers = []
        seller_ids = set()
        quantity_discounts = []

        for offer in offers:
            listing = self._amount(offer.get("ListingPrice")) or 0.0
            shipping = self._amount(offer.get("Shipping")) or 0.0
            business = self._amount(offer.get("BusinessPrice"))
            business = listing if business is None else business
            seller_id = offer.get("SellerId")
            if seller_id:
                seller_ids.add(seller_id)

            tiers = []
            for t in offer.get("QuantityDiscountPrices", []) or []:
                qty = t.get("QuantityTier")
                tier_price = self._amount(t.get("ListingPrice") or t.get("Price"))
                if isinstance(qty, int) and tier_price is not None and qty > 0:
                    unit_price = round(float(tier_price) / float(qty), 2)
                    tiers.append({"quantity": qty, "total_price": float(tier_price), "unit_price": unit_price})
            if tiers:
                quantity_discounts.extend([{"seller_id": seller_id, **x} for x in tiers])

            parsed_offers.append(
                {
                    "seller_id": seller_id,
                    "is_fba": bool(offer.get("IsFulfilledByAmazon", False)),
                    "is_buybox": bool(offer.get("IsBuyBoxWinner", False)),
                    "consumer_total": round(listing + shipping, 2),
                    "business_total": round(business + shipping, 2),
                    "quantity_discounts": tiers,
                }
            )

        lowest = min((o["business_total"] for o in parsed_offers), default=0.0)
        referral_fee = round(lowest * float(os.getenv("DEFAULT_REFERRAL_FEE_RATE", "0.15")), 2) if lowest else None
        fba_fee = float(os.getenv("DEFAULT_FBA_FEE_GBP", "0")) if lowest else None
        total_fee = (referral_fee or 0.0) + (fba_fee or 0.0) if lowest else None

        return {
            "seller_count": len(seller_ids),
            "seller_ids": sorted(seller_ids),
            "offers": parsed_offers,
            "quantity_discounts": quantity_discounts,
            "referral_fee_gbp": referral_fee,
            "fba_fee_gbp": fba_fee,
            "total_fee_gbp": total_fee,
        }

    @staticmethod
    def _amount(p: Any) -> Optional[float]:
        if isinstance(p, (int, float)):
            return float(p)
        if isinstance(p, dict):
            v = p.get("Amount")
            if isinstance(v, (int, float)):
                return float(v)
        return None

    async def post_discord(self, webhook: str, content: str) -> None:
        if not webhook:
            return
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(webhook, json={"content": content})

    def pick_channel(self, spapi: dict[str, Any]) -> Optional[str]:
        offers = spapi.get("offers", [])
        if not offers:
            return None
        best_offer = min(offers, key=lambda x: x["business_total"])
        seller_count = int(spapi.get("seller_count", 0))

        if best_offer.get("seller_id") in AMAZON_SELLER_IDS and seller_count >= self.a2a_min_sellers:
            return "a2a"
        if best_offer.get("seller_id") not in AMAZON_SELLER_IDS and seller_count >= self.third_party_min_sellers:
            return "third_party"
        return None

    def compute_roi(self, effective_buy_price: float, keepa_avg90: Optional[float], total_fee: Optional[float]) -> Optional[float]:
        if not keepa_avg90 or keepa_avg90 <= 0:
            return None
        costs = effective_buy_price + (total_fee or 0.0)
        if costs <= 0:
            return None
        return round((keepa_avg90 - costs) / costs, 4)

    async def process_one(self, app: SpApiApp, row: dict[str, Any]) -> None:
        asin = row["asin"]
        base_price = float(row.get("buy_price_gbp") or row.get("price_gbp"))

        spapi = await self.fetch_spapi_offers(app, asin)
        self.upsert_spapi_scan(asin, spapi)

        keepa_cache = self.load_keepa_cache(asin)
        if keepa_cache:
            keepa = {
                "avg90_price_gbp": float(keepa_cache.get("avg90_price_gbp")) if keepa_cache.get("avg90_price_gbp") is not None else None,
                "sales_rank": keepa_cache.get("sales_rank"),
                "sales_per_month": keepa_cache.get("sales_per_month"),
            }
        else:
            keepa = await self.fetch_keepa(asin)
            self.upsert_keepa_cache(asin, keepa["avg90_price_gbp"], keepa["sales_rank"], keepa["sales_per_month"])

        channel = self.pick_channel(spapi)
        if not channel:
            self.upsert_deal_state(
                asin,
                {
                    "buy_price_gbp": base_price,
                    "effective_buy_price_gbp": base_price,
                    "avg90_price_gbp": keepa.get("avg90_price_gbp"),
                    "sales_rank": keepa.get("sales_rank"),
                    "sales_per_month": keepa.get("sales_per_month"),
                    "roi": None,
                    "profitable": False,
                    "channel": None,
                    "reasons": ["channel_criteria_not_met"],
                },
            )
            return

        qty_tiers = spapi.get("quantity_discounts", [])
        amazon_qty_tiers = [q for q in qty_tiers if q.get("seller_id") in AMAZON_SELLER_IDS]
        effective_price = base_price
        business_channel = False
        if amazon_qty_tiers:
            cheapest = min(amazon_qty_tiers, key=lambda x: x["unit_price"])
            effective_price = min(base_price, float(cheapest["unit_price"]))
            business_channel = True

        roi = self.compute_roi(effective_price, keepa.get("avg90_price_gbp"), spapi.get("total_fee_gbp"))
        profitable = roi is not None and roi >= self.min_roi

        reasons = []
        if roi is None:
            reasons.append("missing_keepa_or_cost_data")
        elif roi < self.min_roi:
            reasons.append(f"roi_below_threshold:{roi}")

        self.upsert_deal_state(
            asin,
            {
                "buy_price_gbp": base_price,
                "effective_buy_price_gbp": effective_price,
                "avg90_price_gbp": keepa.get("avg90_price_gbp"),
                "sales_rank": keepa.get("sales_rank"),
                "sales_per_month": keepa.get("sales_per_month"),
                "roi": roi,
                "profitable": profitable,
                "channel": channel,
                "reasons": reasons,
            },
        )

        if not profitable:
            return

        msg = (
            f"ASIN {asin} | channel={channel} | buy=£{base_price:.2f} | effective=£{effective_price:.2f} "
            f"| avg90=£{(keepa.get('avg90_price_gbp') or 0):.2f} | roi={roi:.2%} | sellers={spapi.get('seller_count')}"
        )

        if business_channel:
            await self.post_discord(self.business_webhook, "[business leads] " + msg)
        elif channel == "a2a":
            await self.post_discord(self.a2a_webhook, "[a2a] " + msg)
        else:
            await self.post_discord(self.third_party_webhook, "[3rd party] " + msg)

    async def run(self) -> None:
        self.ensure_tables()
        rows = self.load_candidates()
        if not rows:
            log.info("No candidate ASINs found in amazon_faceout.")
            return

        log.info("Loaded %s candidates", len(rows))

        # split work by app index so each app has deterministic shard.
        shards: list[list[dict[str, Any]]] = [[] for _ in self.spapi_apps]
        for idx, row in enumerate(rows):
            shards[idx % len(self.spapi_apps)].append(row)

        tasks = []
        for app, shard in zip(self.spapi_apps, shards):
            tasks.append(asyncio.create_task(self._run_shard(app, shard)))

        await asyncio.gather(*tasks)

    async def _run_shard(self, app: SpApiApp, rows: list[dict[str, Any]]) -> None:
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i : i + self.batch_size]
            await asyncio.gather(*(self.process_one(app, row) for row in batch))


def main() -> None:
    scanner = DealScanner()
    asyncio.run(scanner.run())


if __name__ == "__main__":
    main()
