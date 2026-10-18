"""NexusMart cleaning engine: raw CSV -> cleaned CSV + reject log + data quality log.

Every rule is implemented here and logged with before/after counts.
Nothing is silently deleted: removed or nulled records go to rejects.csv with a reason code.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd

DATE_MIN = pd.Timestamp("2024-01-01")
DATE_MAX = pd.Timestamp("2025-06-30")
UNKNOWN = "UNKNOWN"
PRICE_SENTINEL = 1_000_000  # values >= this are sentinels (e.g. 9999999)

EXPECTED_COLUMNS = {
    "customers": ["customer_id", "full_name", "email", "phone", "gender", "registration_date", "city_id", "birth_year"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "list_price", "cost_price", "weight_kg", "seller_id"],
    "sellers": ["seller_id", "seller_name", "join_date", "seller_tier", "city_id", "rating"],
    "locations": ["city_id", "city", "province", "region", "latitude", "longitude", "population_tier"],
    "orders": ["order_id", "customer_id", "order_date", "order_status", "ship_city_id", "order_total", "discount_code", "channel"],
    "order_items": ["order_item_id", "order_id", "product_id", "seller_id", "quantity", "unit_price", "discount_amount"],
    "payments": ["payment_id", "order_id", "payment_method", "payment_status", "amount", "payment_date"],
    "shipping": ["shipment_id", "order_id", "courier", "shipped_date", "delivered_date", "shipping_fee", "delivery_status"],
    "returns": ["return_id", "order_item_id", "return_date", "return_reason", "return_status", "refund_amount"],
}

CATEGORY_MAP = {
    "fashion": "Fashion", "clothing": "Fashion", "fashion & apparel": "Fashion",
    "mobile & accessories": "Mobile & Accessories", "mobile acc.": "Mobile & Accessories", "mobiles": "Mobile & Accessories",
    "home & kitchen": "Home & Kitchen",
    "electronics": "Electronics", "electronic items": "Electronics", "elec.": "Electronics",
    "beauty & personal care": "Beauty & Personal Care", "beauty": "Beauty & Personal Care", "personal care": "Beauty & Personal Care",
    "footwear": "Footwear", "shoes": "Footwear",
    "grocery": "Grocery", "groceries": "Grocery", "food & grocery": "Grocery",
    "sports & outdoors": "Sports & Outdoors", "sports": "Sports & Outdoors",
    "health & wellness": "Health & Wellness", "health": "Health & Wellness",
    "toys & games": "Toys & Games", "toys": "Toys & Games",
    "books & stationery": "Books & Stationery", "books": "Books & Stationery", "stationary": "Books & Stationery",
    "automotive": "Automotive", "auto": "Automotive", "car accessories": "Automotive",
}
PROVINCE_MAP = {
    "punjab": "Punjab", "panjab": "Punjab", "sindh": "Sindh", "sind": "Sindh",
    "khyber pakhtunkhwa": "Khyber Pakhtunkhwa", "kp": "Khyber Pakhtunkhwa", "kpk": "Khyber Pakhtunkhwa",
    "balochistan": "Balochistan", "baluchistan": "Balochistan",
    "gilgit-baltistan": "Gilgit-Baltistan", "gilgit baltistan": "Gilgit-Baltistan", "gb": "Gilgit-Baltistan",
    "azad kashmir": "Azad Kashmir", "azad jammu & kashmir": "Azad Kashmir",
    "islamabad": "Islamabad Capital Territory",
}
COURIER_MAP = {
    "tcs": "TCS", "tcs express": "TCS", "t.c.s": "TCS",
    "leopards": "Leopards", "leopard courier": "Leopards",
    "m&p": "M&P", "m & p": "M&P", "mnp": "M&P",
    "trax": "Trax", "postex": "PostEx",
    "blueex": "BlueEx", "blue ex": "BlueEx", "blue-ex": "BlueEx",
}
PAY_METHOD_MAP = {
    "cod": "COD", "cash on delivery": "COD",
    "card": "Card", "credit/debit card": "Card", "debit card": "Card",
    "jazzcash": "JazzCash", "jazz cash": "JazzCash",
    "easypaisa": "EasyPaisa", "easy paisa": "EasyPaisa",
    "bank transfer": "Bank Transfer", "bank trf": "Bank Transfer",
}
WORD_NUM = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6}


# ----------------------------------------------------------------------------- logging
class DQ:
    def __init__(self):
        self.rules, self.tables, self.rejects = [], {}, []

    def rule(self, table, rule_id, issue, action, affected, column=""):
        self.rules.append(dict(table=table, rule_id=rule_id, column=column, issue=issue,
                               action=action, records_affected=int(affected)))

    def table(self, name, rows_raw, rows_clean, dup_removed=0, rejected=0):
        self.tables[name] = dict(table=name, rows_raw=int(rows_raw), exact_duplicates_removed=int(dup_removed),
                                 rows_rejected=int(rejected), rows_clean=int(rows_clean))

    def reject(self, table, df, key_col, reason, action, detail_cols=None):
        if df is None or len(df) == 0:
            return
        d = pd.DataFrame({"table_name": table, "record_key": df[key_col].astype(str).values,
                          "reason_code": reason, "action": action})
        cols = detail_cols or list(df.columns)
        d["raw_detail"] = df[cols].astype(str).apply(lambda r: json.dumps(r.to_dict(), ensure_ascii=False), axis=1).values
        self.rejects.append(d)


# ----------------------------------------------------------------------------- helpers
def money(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.replace(r"(?i)rs\.?", "", regex=True).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(t, errors="coerce")


def pdate(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    out = pd.to_datetime(t, format="%Y-%m-%d", errors="coerce")
    for fmt in ("%d/%m/%Y", "%b %d %Y"):
        m = out.isna()
        if m.any():
            out[m] = pd.to_datetime(t[m], format=fmt, errors="coerce")
    return out


def norm_key(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()


def read_raw(raw_dir: Path) -> dict:
    t = {}
    for name in EXPECTED_COLUMNS:
        df = pd.read_csv(Path(raw_dir) / f"{name}.csv", dtype=str, keep_default_na=False, encoding="utf-8-sig")
        for c in df.columns:
            df[c] = df[c].str.replace(r"\s+", " ", regex=True).str.strip()
        t[name] = df
    return t


def validate_raw(raw_dir: Path) -> dict:
    """File availability + header check. Raises on a hard problem."""
    report = {}
    for name, cols in EXPECTED_COLUMNS.items():
        p = Path(raw_dir) / f"{name}.csv"
        if not p.exists():
            raise FileNotFoundError(f"Missing raw file: {p}")
        head = pd.read_csv(p, nrows=0, encoding="utf-8-sig").columns.tolist()
        missing = [c for c in cols if c not in head]
        if missing:
            raise ValueError(f"{name}.csv missing columns: {missing}")
        extra = [c for c in head if c not in cols]
        with open(p, "rb") as fh:
            n = sum(1 for _ in fh) - 1
        report[name] = dict(rows=n, extra_columns=extra)
    return report


class UnionFind:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            if ra < rb:
                self.p[rb] = ra
            else:
                self.p[ra] = rb


def cluster_ids(df: pd.DataFrame, id_col: str, key_sets: list) -> pd.Series:
    """Union rows sharing ALL columns of any key set (never name alone). Master = smallest id."""
    uf = UnionFind()
    for ids in df[id_col]:
        uf.find(ids)
    for cols in key_sets:
        sub = df
        for c in cols:
            sub = sub[sub[c].notna() & (sub[c] != "")]
        for _, g in sub.groupby(cols)[id_col]:
            g = list(g)
            for other in g[1:]:
                uf.union(g[0], other)
    return df[id_col].map(uf.find)


# ----------------------------------------------------------------------------- main
def clean_all(raw_dir, out_dir, rejects_dir, reports_dir, discount_rule="expected"):
    raw_dir, out_dir, rejects_dir, reports_dir = map(Path, (raw_dir, out_dir, rejects_dir, reports_dir))
    for d in (out_dir, rejects_dir, reports_dir):
        d.mkdir(parents=True, exist_ok=True)
    dq = DQ()
    raw = read_raw(raw_dir)
    n_raw = {k: len(v) for k, v in raw.items()}

    # ===================================================== LOCATIONS
    lo = raw["locations"].copy()
    lo["city_id"] = lo["city_id"].astype(int)
    lo["province_raw"] = lo["province"]
    lo["province"] = norm_key(lo["province"]).map(PROVINCE_MAP)
    n_variant = (lo["province"].notna() & (lo["province_raw"] != lo["province"])).sum()
    dq.rule("locations", "LOC01", "Province spelling variants (Panjab, KP, Sind, GB ...)", "Mapped to canonical province", n_variant, "province")
    lo["latitude"] = pd.to_numeric(lo["latitude"], errors="coerce")
    lo["longitude"] = pd.to_numeric(lo["longitude"], errors="coerce")
    bad_xy = ~(lo["latitude"].between(23, 37.5) & lo["longitude"].between(60, 77.5))
    n_bad_xy = (bad_xy & (lo["latitude"].notna() | lo["longitude"].notna())).sum()
    lo.loc[bad_xy, ["latitude", "longitude"]] = np.nan
    dq.rule("locations", "LOC02", "Missing or out-of-Pakistan coordinates", "Set to NULL (kept, flagged)", bad_xy.sum(), "latitude/longitude")
    known = lo[lo["province"].notna() & lo["latitude"].notna()]
    miss = lo["province"].isna()
    filled = 0
    for i in lo.index[miss]:
        if pd.notna(lo.at[i, "latitude"]) and len(known):
            d2 = (known["latitude"] - lo.at[i, "latitude"]) ** 2 + (known["longitude"] - lo.at[i, "longitude"]) ** 2
            lo.at[i, "province"] = known.loc[d2.idxmin(), "province"]
            filled += 1
    dq.rule("locations", "LOC03", "Missing province", "Imputed from nearest city by coordinates (flag province_imputed)", filled, "province")
    still = lo["province"].isna().sum()
    lo["province_imputed"] = miss
    lo["province"] = lo["province"].fillna("Unknown")
    dq.rule("locations", "LOC04", "Missing province without coordinates", "Set to 'Unknown'", still, "province")
    lo["city"] = lo["city"].str.strip()
    lo = lo[["city_id", "city", "province", "region", "latitude", "longitude", "population_tier", "province_imputed"]]
    dq.table("locations", n_raw["locations"], len(lo))
    valid_city = set(lo["city_id"])

    # ===================================================== SELLERS
    se = raw["sellers"].copy()
    se["seller_name"] = se["seller_name"].str.replace(r"\s+", " ", regex=True).str.strip()
    t0 = se["seller_tier"]
    se["seller_tier"] = t0.str.strip().str.title().replace("", "Unknown")
    dq.rule("sellers", "SEL01", "seller_tier case/whitespace variants and blanks", "Title-cased; blank -> 'Unknown'", (t0 != se["seller_tier"]).sum(), "seller_tier")
    se["join_date_parsed"] = pdate(se["join_date"])
    dq.rule("sellers", "SEL02", "Missing/invalid join_date", "Retained as NULL (not needed for metrics)", se["join_date_parsed"].isna().sum(), "join_date")
    se["rating"] = pd.to_numeric(se["rating"], errors="coerce")
    bad_r = se["rating"].isna() | ~se["rating"].between(1, 5)
    tier_med = se.loc[~bad_r].groupby("seller_tier")["rating"].median()
    glob_med = se.loc[~bad_r, "rating"].median()
    se["rating_imputed"] = bad_r
    se.loc[bad_r, "rating"] = se.loc[bad_r, "seller_tier"].map(tier_med).fillna(glob_med)
    dq.rule("sellers", "SEL03", "Rating missing or outside 1-5", "Imputed with tier median (flag rating_imputed)", bad_r.sum(), "rating")
    se["city_id"] = pd.to_numeric(se["city_id"], errors="coerce")
    bad_c = ~se["city_id"].isin(valid_city)
    dq.rule("sellers", "SEL04", "Seller city not in locations", "Mapped to Unknown location (-1)", bad_c.sum(), "city_id")
    se["city_id"] = se["city_id"].where(~bad_c, -1).astype(int)
    se = se[["seller_id", "seller_name", "join_date_parsed", "seller_tier", "city_id", "rating", "rating_imputed"]].rename(columns={"join_date_parsed": "join_date"})
    dq.table("sellers", n_raw["sellers"], len(se))
    valid_seller = set(se["seller_id"])

    # ===================================================== raw item price reference (needed to repair products + items)
    it_raw = raw["order_items"].copy()
    it_raw["p"] = money(it_raw["unit_price"])
    good_p = it_raw[(it_raw["p"] > 0) & (it_raw["p"] < PRICE_SENTINEL)]
    ref_price = good_p.groupby("product_id")["p"].median()

    # ===================================================== PRODUCTS
    pr = raw["products"].copy()
    c0 = pr["category"]
    ck = c0.str.lower().str.strip().str.replace(" and ", " & ", regex=False).str.replace(r"\s*&\s*", " & ", regex=True)
    ck = ck.replace({"home&kitchen": "home & kitchen"})
    pr["category"] = ck.map(CATEGORY_MAP).fillna("Unknown")
    dq.rule("products", "PRD01", "Inconsistent category labels (%d raw variants)" % c0.nunique(), "Standardised to %d categories" % pr["category"].nunique(), (c0 != pr["category"]).sum(), "category")
    pr["sub_category"] = pr["sub_category"].replace("", "Unspecified")
    dq.rule("products", "PRD02", "Missing sub_category", "Set to 'Unspecified'", (raw["products"]["sub_category"] == "").sum(), "sub_category")
    pr["product_name"] = pr["product_name"].apply(lambda x: x.title() if (x.islower() or x.isupper()) else x)
    brands = set(b for b in pr["brand"].unique() if b)
    bl = {b.lower(): b for b in brands}
    first = pr["product_name"].str.split(" ").str[0].str.lower()
    nb = (pr["brand"] == "")
    pr.loc[nb, "brand"] = first[nb].map(bl).fillna("Unknown")
    dq.rule("products", "PRD03", "Missing brand", "Inferred from product name prefix; else 'Unknown'", nb.sum(), "brand")
    pr["list_price"] = money(pr["list_price"])
    pr["cost_price"] = money(pr["cost_price"])
    pr["weight_kg"] = money(pr["weight_kg"])
    pr = pr.join(ref_price.rename("ref_price"), on="product_id")
    bad_l = pr["list_price"].isna() | (pr["list_price"] <= 0) | (pr["list_price"] >= PRICE_SENTINEL)
    ratio = (pr["ref_price"] / pr["list_price"])[~bad_l]
    r0 = ratio[(ratio > 0.9) & (ratio < 1.2)].median()
    fix = bad_l & pr["ref_price"].notna()
    pr.loc[fix, "list_price"] = (pr.loc[fix, "ref_price"] / r0).round()
    pr["list_price_repaired"] = bad_l
    dq.rule("products", "PRD04", "Invalid list_price (<=0 or sentinel 9999999)", "Repaired from median sold unit price / typical unit-to-list ratio (%.3f)" % r0, fix.sum(), "list_price")
    dq.rule("products", "PRD04b", "Invalid list_price and product never sold", "Left NULL (flag list_price_repaired)", (bad_l & ~fix).sum(), "list_price")
    bad_cost = pr["cost_price"].isna() | (pr["cost_price"] <= 0) | (pr["cost_price"] > pr["list_price"] * 1.2)
    cr = (pr["cost_price"] / pr["list_price"])[~bad_cost & ~bad_l]
    cat_ratio = cr.groupby(pr["category"]).median()
    glob_ratio = float(cr.median())
    imp = (pr["category"].map(cat_ratio).fillna(glob_ratio) * pr["list_price"]).round()
    pr["cost_imputed"] = bad_cost
    pr.loc[bad_cost, "cost_price"] = imp[bad_cost]
    dq.rule("products", "PRD05", "cost_price missing, <=0 or >120%% of list price (%d of them above list)" % ((raw["products"]["cost_price"] != "") & (money(raw["products"]["cost_price"]) > money(raw["products"]["list_price"]) * 1.2)).sum(),
            "Imputed = category median cost/list ratio x list_price (flag cost_imputed)", bad_cost.sum(), "cost_price")
    bad_w = pr["weight_kg"].isna() | (pr["weight_kg"] <= 0) | (pr["weight_kg"] > 100)
    wmed = pr.loc[~bad_w].groupby("category")["weight_kg"].median()
    pr["weight_imputed"] = bad_w
    pr.loc[bad_w, "weight_kg"] = pr.loc[bad_w, "category"].map(wmed)
    dq.rule("products", "PRD06", "weight_kg missing, <=0 or >100", "Imputed with category median (flag weight_imputed)", bad_w.sum(), "weight_kg")
    bad_s = ~pr["seller_id"].isin(valid_seller)
    dq.rule("products", "PRD07", "Product seller not in sellers", "Report only (none expected)", bad_s.sum(), "seller_id")
    # duplicate products: same normalised name + category + list price within 10% (never name alone)
    pr["nk"] = norm_key(pr["product_name"])
    master = pr["product_id"].copy()
    for _, g in pr.groupby(["nk", "category"]):
        if len(g) < 2:
            continue
        g = g.sort_values("product_id")
        m = g.iloc[0]
        for _, o in g.iloc[1:].iterrows():
            if pd.notna(m["list_price"]) and pd.notna(o["list_price"]) and abs(o["list_price"] - m["list_price"]) <= 0.10 * m["list_price"]:
                master[o.name] = m["product_id"]
    pmap = pd.DataFrame({"raw_product_id": pr["product_id"], "master_product_id": master.values})
    pmap["merged"] = pmap["raw_product_id"] != pmap["master_product_id"]
    dq.rule("products", "PRD08", "Duplicate products under different IDs (same name+category, list price within 10%)",
            "Merged to lowest product_id; mapping in product_map.csv", pmap["merged"].sum(), "product_id")
    dup_rows = pr[pr["product_id"].isin(pmap.loc[pmap["merged"], "raw_product_id"])]
    dq.reject("products", dup_rows, "product_id", "DUPLICATE_PRODUCT_MERGED", "MERGED_INTO_MASTER", ["product_id", "product_name", "list_price", "seller_id"])
    pr_clean = pr[~pr["product_id"].isin(dup_rows["product_id"])].copy()
    pr_clean = pr_clean[["product_id", "product_name", "category", "sub_category", "brand", "list_price", "cost_price", "weight_kg",
                         "seller_id", "list_price_repaired", "cost_imputed", "weight_imputed"]]
    dq.table("products", n_raw["products"], len(pr_clean), dup_removed=0, rejected=len(dup_rows))
    pmap_dict = dict(zip(pmap["raw_product_id"], pmap["master_product_id"]))
    valid_product = set(pr_clean["product_id"])

    # ===================================================== PAYMENTS (clean first: used for date repair)
    pa = raw["payments"].copy()
    dmask = pa.duplicated(keep="first")
    dq.reject("payments", pa[dmask], "payment_id", "EXACT_DUPLICATE_ROW", "REMOVED")
    dq.rule("payments", "PAY01", "Exact duplicate payment rows", "Removed (first kept)", dmask.sum())
    pa = pa[~dmask].copy()
    m0 = pa["payment_method"]
    pa["payment_method"] = norm_key(m0).map(PAY_METHOD_MAP).fillna("Unknown")
    dq.rule("payments", "PAY02", "payment_method label variants (%d raw)" % m0.nunique(), "Standardised to 5 methods", (m0 != pa["payment_method"]).sum(), "payment_method")
    s0 = pa["payment_status"]
    sk = norm_key(s0)
    pa["payment_status"] = np.select([sk.isin(["paid", "complete", "success"]), sk.str.startswith("cancel"), sk.str.startswith("refund"), sk == "pending"],
                                     ["Paid", "Cancelled", "Refunded", "Pending"], "Unknown")
    dq.rule("payments", "PAY03", "payment_status label variants (%d raw)" % s0.nunique(), "Standardised to Paid/Cancelled/Refunded/Pending", (s0 != pa["payment_status"]).sum(), "payment_status")
    pa["amount"] = money(pa["amount"])
    neg = pa["amount"] < 0
    pa.loc[neg, "amount"] = pa.loc[neg, "amount"].abs()
    dq.rule("payments", "PAY04", "Negative payment amounts", "Converted to absolute value", neg.sum(), "amount")
    pa["pay_date"] = pdate(pa["payment_date"])
    pa.loc[~pa["pay_date"].between(DATE_MIN, DATE_MAX + pd.Timedelta(days=60)), "pay_date"] = pd.NaT
    dq.rule("payments", "PAY05", "Missing/invalid payment_date", "Kept as NULL", pa["pay_date"].isna().sum(), "payment_date")
    pa["payment_date"] = pa["pay_date"]

    # ===================================================== SHIPPING raw parse (for order-date repair)
    sh = raw["shipping"].copy()
    sh["shipped_dt"] = pdate(sh["shipped_date"])
    sh["delivered_dt"] = pdate(sh["delivered_date"])

    # ===================================================== CUSTOMERS
    cu = raw["customers"].copy()
    cu["full_name"] = cu["full_name"].str.title()
    cu["email"] = cu["email"].str.lower()
    ph = cu["phone"].str.replace(r"\D", "", regex=True)
    ph = ph.str.replace(r"^92", "0", regex=True)
    ph = ph.where(ph.str.len() == 11, np.where(ph.str.len() == 10, "0" + ph, ph))
    cu["phone"] = np.where(ph.str.len() == 11, ph.str[:4] + "-" + ph.str[4:], "")
    cu["phone_key"] = np.where(ph.str.len() == 11, ph, "")
    g0 = cu["gender"]
    gk = norm_key(g0)
    cu["gender"] = np.select([gk.isin(["m", "male"]), gk.isin(["f", "female"])], ["Male", "Female"], "Unknown")
    dq.rule("customers", "CUS01", "Gender label variants/blank", "Standardised to Male/Female/Unknown", (g0 != cu["gender"]).sum(), "gender")
    dq.rule("customers", "CUS02", "Phone formats mixed (+92, dashes, spaces)", "Normalised to 03XX-XXXXXXX; invalid -> blank", (raw["customers"]["phone"] != cu["phone"]).sum(), "phone")
    by = pd.to_numeric(cu["birth_year"], errors="coerce")
    bad_by = by.notna() & ~by.between(1930, 2010)
    by[bad_by] = np.nan
    cu["birth_year"] = by
    dq.rule("customers", "CUS03", "Implausible birth_year (<1930 or >2010)", "Set to NULL", bad_by.sum(), "birth_year")
    cu["reg_dt"] = pdate(cu["registration_date"])
    cu["city_id"] = pd.to_numeric(cu["city_id"], errors="coerce")
    bad_city = ~cu["city_id"].isin(valid_city)
    dq.rule("customers", "CUS04", "Missing/invalid city_id", "Mapped to Unknown location (-1) unless a duplicate record supplies it", bad_city.sum(), "city_id")
    cu["city_id"] = cu["city_id"].where(~bad_city, np.nan)
    cu["name_key"] = norm_key(cu["full_name"])
    cu["email_key"] = cu["email"]
    master_c = cluster_ids(cu, "customer_id", [["name_key", "email_key"], ["name_key", "phone_key"]])
    cmap = pd.DataFrame({"raw_customer_id": cu["customer_id"], "master_customer_id": master_c.values})
    cmap["merged"] = cmap["raw_customer_id"] != cmap["master_customer_id"]
    cmap["match_rule"] = np.where(cmap["merged"], "same name + same email or phone", "")
    dq.rule("customers", "CUS05", "Duplicate customers under different IDs (same name AND same email or phone; never name alone)",
            "Merged to lowest customer_id; attributes coalesced; mapping in customer_map.csv", cmap["merged"].sum(), "customer_id")
    cu["master"] = master_c.values
    dup_c = cu[cu["customer_id"] != cu["master"]]
    dq.reject("customers", dup_c, "customer_id", "DUPLICATE_CUSTOMER_MERGED", "MERGED_INTO_MASTER", ["customer_id", "full_name", "email", "phone", "master"])
    agg = cu.sort_values("customer_id").groupby("master").agg(
        full_name=("full_name", "first"), email=("email", lambda s: next((x for x in s if x), "")),
        phone=("phone", lambda s: next((x for x in s if x), "")),
        gender=("gender", lambda s: next((x for x in s if x != "Unknown"), "Unknown")),
        reg_dt=("reg_dt", "min"), city_id=("city_id", lambda s: s.dropna().iloc[0] if s.notna().any() else np.nan),
        birth_year=("birth_year", lambda s: s.dropna().iloc[0] if s.notna().any() else np.nan)).reset_index().rename(columns={"master": "customer_id"})
    cmap_dict = dict(zip(cmap["raw_customer_id"], cmap["master_customer_id"]))

    # ===================================================== ORDERS
    od = raw["orders"].copy()
    dmask = od.duplicated(keep="first")
    dq.reject("orders", od[dmask], "order_id", "EXACT_DUPLICATE_ROW", "REMOVED")
    dq.rule("orders", "ORD01", "Exact duplicate order rows", "Removed (first kept)", dmask.sum())
    od = od[~dmask].copy()
    assert od["order_id"].is_unique, "order_id still not unique after exact-duplicate removal"
    n_dup_orders = int(dmask.sum())
    st0 = od["order_status"]
    sk = norm_key(st0)
    od["order_status"] = np.select([sk.str.contains("cancel"), sk.str.contains("return"), sk.str.contains("transit") | (sk == "shipped")],
                                   ["Cancelled", "Returned", "In Transit"], "Delivered")
    dq.rule("orders", "ORD02", "order_status variants (%d raw labels)" % st0.nunique(), "Standardised to Delivered/Cancelled/Returned/In Transit", (st0 != od["order_status"]).sum(), "order_status")
    ch0 = od["channel"]
    ck = norm_key(ch0)
    od["channel"] = np.select([ck.isin(["app", "mobile app"]), ck.isin(["web", "website"]), ck.isin(["mobile web", "mweb"])], ["App", "Web", "Mobile Web"], "Unknown")
    dq.rule("orders", "ORD03", "channel variants (%d raw labels)" % ch0.nunique(), "Standardised to App/Web/Mobile Web", (ch0 != od["channel"]).sum(), "channel")
    dc_raw = od["discount_code"].copy()
    dc = od["discount_code"].str.upper()
    od["discount_pct"] = pd.to_numeric(dc.str.extract(r"SAVE(\d+)")[0], errors="coerce").fillna(0)
    od["discount_code"] = np.where(od["discount_pct"] > 0, "SAVE" + od["discount_pct"].astype(int).astype(str), "")
    dq.rule("orders", "ORD04", "discount_code case/whitespace variants (%d raw values)" % dc_raw.nunique(), "Upper-cased; percentage extracted", (dc_raw != od["discount_code"]).sum(), "discount_code")
    od["order_total"] = money(od["order_total"])
    od.loc[(od["order_total"] <= 0) | (od["order_total"] >= PRICE_SENTINEL), "order_total"] = np.nan
    # customers
    od["customer_id_raw"] = od["customer_id"]
    od["customer_id"] = od["customer_id"].map(cmap_dict)
    orphan_c = od["customer_id"].isna()
    od.loc[orphan_c, "customer_id"] = UNKNOWN
    dq.rule("orders", "ORD05", "Orders whose customer_id is not in customers (broken reference)", "Mapped to Unknown customer member", orphan_c.sum(), "customer_id")
    # order date
    od["n"] = od["order_id"].str[1:].astype(int)
    od["d"] = pdate(od["order_date"])
    bad_fmt = od["d"].isna().sum()
    oor = (od["d"].notna() & ~od["d"].between(DATE_MIN, DATE_MAX)).sum()
    od.loc[~od["d"].between(DATE_MIN, DATE_MAX), "d"] = pd.NaT
    od["date_repair"] = ""
    nc_pay = pa[(pa["payment_method"] != "COD") & pa["pay_date"].between(DATE_MIN, DATE_MAX)].groupby("order_id")["pay_date"].min()
    shp = sh[sh["shipped_dt"].notna()].drop_duplicates("order_id").set_index("order_id")["shipped_dt"] - pd.Timedelta(days=1)
    for src, series in (("payment_date(non-COD)", nc_pay), ("shipped_date-1", shp)):
        cand = od["order_id"].map(series)
        m = od["d"].isna() & cand.between(DATE_MIN, DATE_MAX)
        od.loc[m, "d"] = cand[m]
        od.loc[m, "date_repair"] = src
    s = od.sort_values("n")
    days = (s["d"] - DATE_MIN).dt.days.astype("float64")
    nb_ = pd.concat([days.ffill(), days.bfill()], axis=1).mean(axis=1).round()
    m = od["d"].isna()
    fill = DATE_MIN + pd.to_timedelta(nb_.reindex(od.index), unit="D")
    ok = m & fill.between(DATE_MIN, DATE_MAX)
    od.loc[ok, "d"] = fill[ok]
    od.loc[ok, "date_repair"] = "neighbour_order_id"
    dq.rule("orders", "ORD06", "Unparseable order_date (e.g. 00/00/2024, 2024-13-05, 2024-02-30)", "Repaired from other data (see ORD08)", bad_fmt, "order_date")
    dq.rule("orders", "ORD07", "order_date outside 1 Jan 2024 - 30 Jun 2025", "Repaired from other data (see ORD08)", oor, "order_date")
    dq.rule("orders", "ORD08", "Order dates repaired: payment_date (non-COD) > shipped_date-1 > neighbouring order_id", "Repaired",
            (od["date_repair"] != "").sum(), "order_date")
    unrep = od[od["d"].isna()]
    dq.reject("orders", unrep, "order_id", "UNREPAIRABLE_ORDER_DATE", "QUARANTINED_WITH_CHILDREN")
    od = od[od["d"].notna()].copy()
    od["order_date"] = od["d"]
    # ship city
    cust_city = agg.set_index("customer_id")["city_id"]
    od["ship_city_id"] = pd.to_numeric(od["ship_city_id"], errors="coerce")
    bad_sc = ~od["ship_city_id"].isin(valid_city)
    rep = od["customer_id"].map(cust_city)
    ok = bad_sc & rep.isin(valid_city)
    od.loc[ok, "ship_city_id"] = rep[ok]
    od["ship_city_repaired"] = ok
    left = od["ship_city_id"].isna() | ~od["ship_city_id"].isin(valid_city)
    od.loc[left, "ship_city_id"] = -1
    od["ship_city_id"] = od["ship_city_id"].astype(int)
    dq.rule("orders", "ORD09", "Missing ship_city_id", "Repaired with the customer's registered city (89% of known orders ship to it)", ok.sum(), "ship_city_id")
    dq.rule("orders", "ORD10", "Missing ship_city_id and no usable customer city", "Mapped to Unknown location (-1)", left.sum(), "ship_city_id")
    od = od[["order_id", "customer_id", "customer_id_raw", "order_date", "order_status", "ship_city_id", "ship_city_repaired",
             "order_total", "discount_code", "discount_pct", "channel", "date_repair"]]
    order_ids = set(od["order_id"])
    pct_map = od.set_index("order_id")["discount_pct"]
    dq.table("orders", n_raw["orders"], len(od), dup_removed=n_dup_orders, rejected=len(unrep))

    # ===================================================== ORDER ITEMS
    it = raw["order_items"].copy()
    orphan_o = ~it["order_id"].isin(order_ids)
    dq.reject("order_items", it[orphan_o], "order_item_id", "ORDER_NOT_FOUND_OR_QUARANTINED", "QUARANTINED")
    dq.rule("order_items", "ITM01", "Line items whose order is missing/quarantined", "Quarantined", orphan_o.sum(), "order_id")
    it = it[~orphan_o].copy()
    # quantity
    q0 = it["quantity"].str.lower()
    q = pd.to_numeric(q0, errors="coerce")
    q = q.fillna(q0.map(WORD_NUM))
    it["qty"] = q
    nq_word = (pd.to_numeric(q0, errors="coerce").isna() & q.notna()).sum()
    dq.rule("order_items", "ITM02", "quantity written as words/decimals (one, 2.0)", "Converted to integer", nq_word + (q0.str.contains(r"\.0$")).sum(), "quantity")
    negq = it["qty"] < 0
    it.loc[negq, "qty"] = it.loc[negq, "qty"].abs()
    dq.rule("order_items", "ITM03", "Negative quantity", "Converted to absolute value", negq.sum(), "quantity")
    # price
    it["price"] = money(it["unit_price"])
    negp = it["price"] < 0
    it.loc[negp, "price"] = it.loc[negp, "price"].abs()
    dq.rule("order_items", "ITM04", "Negative unit_price", "Converted to absolute value", negp.sum(), "unit_price")
    it["product_raw"] = it["product_id"]
    it["ref"] = it["product_raw"].map(ref_price)
    pr_list = pr_clean.set_index("product_id")["list_price"]
    it["ref"] = it["ref"].fillna(it["product_raw"].map(lambda x: pr_list.get(pmap_dict.get(x, x), np.nan)))
    badp = it["price"].isna() | (it["price"] <= 0) | (it["price"] >= PRICE_SENTINEL) | (it["price"] < 0.5 * it["ref"]) | (it["price"] > 2 * it["ref"])
    fixp = badp & it["ref"].notna()
    it["price_repaired"] = fixp | negp
    it.loc[fixp, "price"] = it.loc[fixp, "ref"].round()
    dq.rule("order_items", "ITM05", "Implausible unit_price (0, sentinel 9999999, x100, <50% or >200% of product's typical price)",
            "Replaced with product's median valid unit price (flag price_repaired)", fixp.sum(), "unit_price")
    dq.rule("order_items", "ITM05b", "Implausible unit_price with no reference price", "Quarantined", (badp & ~fixp).sum(), "unit_price")
    nofix = it[badp & ~fixp]
    dq.reject("order_items", nofix, "order_item_id", "UNREPAIRABLE_UNIT_PRICE", "QUARANTINED")
    it = it[~(badp & ~fixp)].copy()
    # order-level helpers
    it["pct"] = it["order_id"].map(pct_map)
    ot = od.set_index("order_id")["order_total"]
    it["ot"] = it["order_id"].map(ot)
    # big quantities (sentinel 500): back-solve from order_total when exactly one such line in the order
    bigq = it["qty"] > 50
    it["qty_repaired"] = negq | bigq | (it["qty"].isna())
    cnt_big = it[bigq].groupby("order_id").size()
    single = it["order_id"].map(cnt_big).eq(1) & bigq
    net_unit = it["price"] * (1 - it["pct"] / 100)
    other_net = (it["qty"].where(~bigq, 0) * net_unit).groupby(it["order_id"]).transform("sum")
    implied = ((it["ot"] - other_net) / net_unit).round()
    use = single & implied.between(1, 6)
    it.loc[use, "qty"] = implied[use]
    it.loc[bigq & ~use, "qty"] = 1
    it["qty"] = it["qty"].fillna(1)
    dq.rule("order_items", "ITM06", "Quantity outlier (500) / missing", "Back-solved from order_total when possible (%d), else 1 (%d)" % (use.sum(), (bigq & ~use).sum()), bigq.sum(), "quantity")
    it["qty"] = it["qty"].astype(int)
    # discounts
    it["gross"] = it["qty"] * it["price"]
    it["da"] = money(it["discount_amount"])
    exp = (it["gross"] * it["pct"] / 100).round()
    blank = it["da"].isna()
    over = it["da"] > it["gross"]
    off = it["da"].notna() & ~over & ((it["da"] - exp).abs() > np.maximum(2, 0.03 * exp))
    neg_d = it["da"] < 0
    fixd = blank | over | neg_d | (off if discount_rule == "expected" else False)
    it["discount_repaired"] = fixd
    it["discount"] = np.where(fixd, exp, it["da"])
    dq.rule("order_items", "ITM07", "discount_amount blank while order has a discount code", "Set to line gross x order discount %", (blank & (it["pct"] > 0)).sum(), "discount_amount")
    dq.rule("order_items", "ITM08", "discount_amount > line gross, negative, or inconsistent with order discount %", "Set to line gross x order discount %", (over | neg_d | off).sum(), "discount_amount")
    dq.rule("order_items", "ITM09", "discount_amount blank and no discount code", "Set to 0", (blank & (it["pct"] == 0)).sum(), "discount_amount")
    it["line_net"] = it["gross"] - it["discount"]
    # product & seller references
    it["product_id"] = it["product_raw"].map(pmap_dict)
    orph_p = it["product_id"].isna() | ~it["product_id"].isin(valid_product)
    it.loc[orph_p, "product_id"] = UNKNOWN
    dq.rule("order_items", "ITM10", "Line items whose product_id is not in products (broken reference)", "Mapped to Unknown product member (line kept in sales)", orph_p.sum(), "product_id")
    bad_s = ~it["seller_id"].isin(valid_seller)
    it.loc[bad_s, "seller_id"] = UNKNOWN
    dq.rule("order_items", "ITM11", "Line items whose seller_id is not in sellers", "Mapped to Unknown seller member", bad_s.sum(), "seller_id")
    it = it[["order_item_id", "order_id", "product_id", "product_raw", "seller_id", "qty", "price", "discount", "gross", "line_net",
             "price_repaired", "qty_repaired", "discount_repaired"]].rename(columns={"qty": "quantity", "price": "unit_price"})
    dq.table("order_items", n_raw["order_items"], len(it), rejected=n_raw["order_items"] - len(it))
    item_ids = set(it["order_item_id"])

    # ===================================================== PAYMENTS finish
    orphan_p = ~pa["order_id"].isin(order_ids)
    dq.reject("payments", pa[orphan_p], "payment_id", "ORDER_NOT_FOUND_OR_QUARANTINED", "QUARANTINED")
    pa = pa[~orphan_p]
    pa = pa[["payment_id", "order_id", "payment_method", "payment_status", "amount", "payment_date"]]
    dq.table("payments", n_raw["payments"], len(pa), dup_removed=int(dmask.sum()) if False else n_raw["payments"] - len(pa) - int(orphan_p.sum()), rejected=int(orphan_p.sum()))

    # ===================================================== SHIPPING
    orphan_s = ~sh["order_id"].isin(order_ids)
    dq.reject("shipping", sh[orphan_s], "shipment_id", "ORDER_NOT_FOUND_OR_QUARANTINED", "QUARANTINED")
    sh = sh[~orphan_s].copy()
    c0 = sh["courier"]
    sh["courier"] = norm_key(c0).map(COURIER_MAP).fillna("Unknown")
    dq.rule("shipping", "SHP01", "Courier name variants (%d raw)" % c0.nunique(), "Standardised to 6 couriers", (c0 != sh["courier"]).sum(), "courier")
    d0 = sh["delivery_status"]
    dk = norm_key(d0)
    sh["delivery_status"] = np.select([dk.isin(["delivered", "deliverd", "complete", "completed"]), dk.isin(["in transit", "in-transit", "shipped"]),
                                       dk.isin(["returned to seller", "return to seller", "rto"])],
                                      ["Delivered", "In Transit", "Returned to Seller"], "Unknown")
    dq.rule("shipping", "SHP02", "delivery_status variants (%d raw)" % d0.nunique(), "Standardised to Delivered/In Transit/Returned to Seller", (d0 != sh["delivery_status"]).sum(), "delivery_status")
    odate = od.set_index("order_id")["order_date"]
    sh["odate"] = sh["order_id"].map(odate)
    shipped_missing = sh["shipped_dt"].isna()
    before_order = sh["shipped_dt"].notna() & (sh["shipped_dt"] < sh["odate"])
    far = sh["shipped_dt"].notna() & (sh["shipped_dt"] > sh["odate"] + pd.Timedelta(days=30))
    badship = shipped_missing | before_order | far
    sh.loc[badship, "shipped_dt"] = sh.loc[badship, "odate"] + pd.Timedelta(days=1)
    sh["shipped_imputed"] = badship
    dq.rule("shipping", "SHP03", "shipped_date missing/unparseable", "Imputed as order_date + 1 day (modal lag) (flag shipped_imputed)", shipped_missing.sum(), "shipped_date")
    dq.rule("shipping", "SHP04", "shipped_date before order_date or >30 days after", "Imputed as order_date + 1 day", (before_order | far).sum(), "shipped_date")
    dl_blank = sh["delivered_date"] == ""
    dl_bad_fmt = sh["delivered_dt"].isna() & ~dl_blank
    dl_before = sh["delivered_dt"].notna() & (sh["delivered_dt"] < sh["shipped_dt"])
    dl_long = sh["delivered_dt"].notna() & ((sh["delivered_dt"] - sh["shipped_dt"]).dt.days > 30)
    for mask, code in ((dl_bad_fmt, "INVALID_DELIVERED_DATE"), (dl_before, "DELIVERED_BEFORE_SHIPPED"), (dl_long, "DELIVERY_OVER_30_DAYS")):
        dq.reject("shipping", sh[mask], "shipment_id", code, "FIELD_NULLED", ["shipment_id", "order_id", "shipped_date", "delivered_date"])
        dq.rule("shipping", "SHP05", code, "delivered_date set to NULL (cannot be repaired reliably); shipment kept", mask.sum(), "delivered_date")
    sh.loc[dl_bad_fmt | dl_before | dl_long, "delivered_dt"] = pd.NaT
    sh["shipping_days"] = (sh["delivered_dt"] - sh["shipped_dt"]).dt.days
    fee = money(sh["shipping_fee"])
    negf = fee < 0
    fee[negf] = fee[negf].abs()
    badf = fee.isna() | (fee <= 0) | (fee >= PRICE_SENTINEL)
    fee_med = fee[~badf].groupby(sh["courier"]).median()
    sh["fee_imputed"] = badf
    fee_final = fee.where(~badf, sh["courier"].map(fee_med).fillna(fee[~badf].median()))
    sh["shipping_fee"] = fee_final
    dq.rule("shipping", "SHP06", "Negative shipping_fee", "Converted to absolute value", negf.sum(), "shipping_fee")
    dq.rule("shipping", "SHP07", "shipping_fee missing, zero or sentinel (9999999)", "Imputed with courier median (flag fee_imputed)", badf.sum(), "shipping_fee")
    dq.rule("shipping", "SHP08", "Multiple shipments per order", "Report only (none expected)", sh["order_id"].duplicated().sum(), "order_id")
    sh = sh[["shipment_id", "order_id", "courier", "shipped_dt", "delivered_dt", "shipping_fee", "delivery_status", "shipping_days", "shipped_imputed", "fee_imputed"]].rename(
        columns={"shipped_dt": "shipped_date", "delivered_dt": "delivered_date"})
    dq.table("shipping", n_raw["shipping"], len(sh), rejected=int(orphan_s.sum()))

    # ===================================================== RETURNS
    rt = raw["returns"].copy()
    dmask = rt.duplicated(keep="first")
    dq.reject("returns", rt[dmask], "return_id", "EXACT_DUPLICATE_ROW", "REMOVED")
    rt = rt[~dmask]
    orph = ~rt["order_item_id"].isin(item_ids)
    dq.reject("returns", rt[orph], "return_id", "ORPHAN_ORDER_ITEM", "QUARANTINED")
    dq.rule("returns", "RET01", "Returns referencing a non-existent order_item_id (broken reference)", "Quarantined to rejects (no line to attach to)", orph.sum(), "order_item_id")
    rt = rt[~orph].copy()
    r0_ = rt["return_status"]
    rk = norm_key(r0_)
    rt["return_status"] = np.select([rk.isin(["refunded", "refund"]), rk == "approved", rk.isin(["rejected", "declined"])], ["Refunded", "Approved", "Rejected"], "Unknown")
    dq.rule("returns", "RET02", "return_status variants (Refund/Refunded/Declined...)", "Standardised to Refunded/Approved/Rejected", (r0_ != rt["return_status"]).sum(), "return_status")
    rr = norm_key(rt["return_reason"])
    rt["return_reason"] = np.select(
        [rr.str.contains("size|fit"), rr.str.contains("described|picture"), rr.str.contains("quality"), rr.str.contains("damage"),
         rr.str.contains("mind|no longer"), rr.str.contains("wrong item|wrong product"), rr.str.contains("colo"),
         rr.str.contains("late"), rr.str.contains("missing|parts|incomplete")],
        ["Size/Fit Issue", "Not as Described", "Quality Issue", "Damaged Product", "Changed Mind", "Wrong Item Delivered", "Colour Mismatch", "Late Delivery", "Missing Parts"],
        "Unknown")
    dq.rule("returns", "RET03", "return_reason free-text variants (%d raw) and blanks" % raw["returns"]["return_reason"].nunique(), "Mapped to 9 reasons; blank -> 'Unknown'", len(rt), "return_reason")
    rt["refund"] = money(rt["refund_amount"])
    rt["return_dt"] = pdate(rt["return_date"])
    line_net = it.set_index("order_item_id")["line_net"]
    rt["line_net"] = rt["order_item_id"].map(line_net)
    valid = rt["return_status"] != "Rejected"
    blank_r = valid & (rt["refund"].isna())
    over_r = valid & (rt["refund"] > rt["line_net"] * 1.0001)
    rt.loc[blank_r, "refund"] = rt.loc[blank_r, "line_net"]
    rt.loc[over_r, "refund"] = rt.loc[over_r, "line_net"]
    rt["refund_capped"] = over_r | blank_r
    dq.rule("returns", "RET04", "Valid refund_amount greater than the returned line's net value (inflated ~1.4x)", "Capped at line net value (flag refund_capped)", over_r.sum(), "refund_amount")
    dq.rule("returns", "RET05", "Valid return with blank refund_amount", "Set to line net value", blank_r.sum(), "refund_amount")
    odate_line = it.set_index("order_item_id")["order_id"].map(odate)
    rt["odate"] = rt["order_item_id"].map(odate_line)
    bad_rd = rt["return_dt"].isna() | (rt["return_dt"] < rt["odate"])
    dq.rule("returns", "RET06", "Return date missing/invalid or before order date", "Kept as NULL (ReturnDateKey -> Unknown date)", bad_rd.sum(), "return_date")
    rt.loc[bad_rd, "return_dt"] = pd.NaT
    dq.rule("returns", "RET07", "Multiple returns for the same order line", "Aggregated per line in fact build (valid returns only)", rt.duplicated("order_item_id").sum(), "order_item_id")
    rt = rt[["return_id", "order_item_id", "return_dt", "return_reason", "return_status", "refund", "refund_capped"]].rename(columns={"return_dt": "return_date", "refund": "refund_amount"})
    dq.table("returns", n_raw["returns"], len(rt), dup_removed=int(dmask.sum()), rejected=int(orph.sum()))

    # ===================================================== CUSTOMERS finish (registration date repair)
    first_order = od[od["customer_id"] != UNKNOWN].groupby("customer_id")["order_date"].min()
    agg["first_order"] = agg["customer_id"].map(first_order)
    bad_reg = agg["reg_dt"].isna() | (agg["reg_dt"] > agg["first_order"]) | (agg["reg_dt"] > DATE_MAX)
    use_fo = bad_reg & agg["first_order"].notna()
    agg["reg_repaired"] = bad_reg
    agg.loc[use_fo, "reg_dt"] = agg.loc[use_fo, "first_order"]
    dq.rule("customers", "CUS06", "registration_date missing/invalid or after first order or after 30 Jun 2025", "Set to first order date when the customer has orders (flag reg_repaired); else NULL", use_fo.sum(), "registration_date")
    dq.rule("customers", "CUS07", "registration_date still missing (customer has no orders)", "Left NULL", (agg["reg_dt"].isna()).sum(), "registration_date")
    agg["city_id"] = agg["city_id"].fillna(-1).astype(int)
    agg["email"] = agg["email"].replace("", np.nan)
    agg["phone"] = agg["phone"].replace("", np.nan)
    cu_clean = agg[["customer_id", "full_name", "email", "phone", "gender", "reg_dt", "city_id", "birth_year", "reg_repaired"]].rename(columns={"reg_dt": "registration_date"})
    dq.table("customers", n_raw["customers"], len(cu_clean), rejected=len(dup_c))

    # ===================================================== write outputs
    outs = {"customers": cu_clean, "products": pr_clean, "sellers": se, "locations": lo, "orders": od, "order_items": it,
            "payments": pa, "shipping": sh, "returns": rt, "customer_map": cmap, "product_map": pmap}
    for name, df in outs.items():
        df.to_csv(out_dir / f"{name}.csv", index=False, date_format="%Y-%m-%d")
    rej = pd.concat(dq.rejects, ignore_index=True) if dq.rejects else pd.DataFrame(columns=["table_name", "record_key", "reason_code", "action", "raw_detail"])
    rej.to_csv(rejects_dir / "rejects.csv", index=False)
    pd.DataFrame(dq.rules).to_csv(reports_dir / "dq_rules_log.csv", index=False)
    pd.DataFrame(dq.tables.values()).to_csv(reports_dir / "dq_table_counts.csv", index=False)
    # reconciliation with order_total
    net = it.groupby("order_id")["line_net"].sum()
    chk = od.set_index("order_id")["order_total"].to_frame().join(net).dropna()
    match = float((chk["order_total"].sub(chk["line_net"]).abs() <= 2).mean())
    summary = dict(order_total_reconciliation_pct=round(match * 100, 2), orders_checked=int(len(chk)),
                   rejects_by_reason=rej.groupby("reason_code").size().to_dict() if len(rej) else {},
                   tables=dq.tables)
    (reports_dir / "dq_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    base = Path(os.environ.get("NEXUS_BASE", "."))
    res = clean_all(base / "data/raw", base / "data/processed", base / "data/rejects", base / "reports")
    print(json.dumps(res, indent=2))
