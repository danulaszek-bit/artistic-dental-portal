"""
generate_sample_data.py
=======================
Creates realistic sample CSV files so you can test the pipeline and dashboard
without waiting for Magic Touch exports.

Run once:   python generate_sample_data.py
"""

import random
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import numpy as np

random.seed(42)
np.random.seed(42)

OUT = Path("sample_exports")
OUT.mkdir(exist_ok=True)

# ── Accounts (dental practices) ────────────────────────────────────────────
ACCOUNTS = [
    ("ACC001", "Lakefront Family Dental",    "Dr. Emily Chen"),
    ("ACC002", "North Shore Periodontics",   "Dr. Marcus Webb"),
    ("ACC003", "Downtown Smiles",            "Dr. Sara Patel"),
    ("ACC004", "Wicker Park Orthodontics",   "Dr. James Kowalski"),
    ("ACC005", "Lincoln Park Dental Arts",   "Dr. Rachel Kim"),
    ("ACC006", "Bucktown Prosthodontics",    "Dr. David Torres"),
    ("ACC007", "Lakeview Dental Group",      "Dr. Anna Novak"),
    ("ACC008", "Hyde Park Dental",           "Dr. Chris Adeyemi"),
    ("ACC009", "Gold Coast Implant Center",  "Dr. Mia Jensen"),
    ("ACC010", "Evanston Family Dentistry",  "Dr. Tom Bradley"),
    ("ACC011", "Oak Park Dental Studio",     "Dr. Lisa Okonkwo"),
    ("ACC012", "Naperville Smile Center",    "Dr. Brian Huang"),
    ("ACC013", "Schaumburg Dental Care",     "Dr. Julie Marchetti"),
    ("ACC014", "Arlington Heights Dental",   "Dr. Kevin Park"),
    ("ACC015", "Skokie Dental Associates",   "Dr. Maria Santos"),
]

accounts_df = pd.DataFrame(ACCOUNTS, columns=["account_id", "account_name", "doctor_name"])
accounts_df.to_csv(OUT / "accounts.csv", index=False)
print("✓  accounts.csv")

# ── Orders (2 years of production history) ─────────────────────────────────
PRODUCTS = {
    "Crown PFM":         {"base_rev": 145, "base_cost": 62},
    "Crown Zirconia":    {"base_rev": 165, "base_cost": 58},
    "Bridge 3-Unit":     {"base_rev": 420, "base_cost": 175},
    "Implant Crown":     {"base_rev": 285, "base_cost": 98},
    "Implant Abutment":  {"base_rev": 195, "base_cost": 72},
    "Full Denture":      {"base_rev": 680, "base_cost": 310},
    "Partial Denture":   {"base_rev": 395, "base_cost": 180},
    "Veneer":            {"base_rev": 120, "base_cost": 44},
    "Nightguard":        {"base_rev": 95,  "base_cost": 38},
    "Clear Aligner Set": {"base_rev": 550, "base_cost": 195},
}

# Accounts have different volume weights (Pareto: top accounts dominate)
VOLUME_WEIGHTS = [0.16, 0.13, 0.11, 0.09, 0.08, 0.07, 0.06, 0.06,
                  0.05, 0.05, 0.04, 0.04, 0.03, 0.02, 0.01]

start_date = date.today().replace(day=1) - timedelta(days=730)
end_date   = date.today()
total_days = (end_date - start_date).days

orders = []
order_id = 1000

for day_offset in range(total_days):
    current_date = start_date + timedelta(days=day_offset)
    if current_date.weekday() >= 5:      # no weekend orders
        continue

    # Seasonal volume: slight uptick Q4, dip in August
    base_volume = 22
    month = current_date.month
    if month in (11, 12): base_volume = 28
    if month == 8:        base_volume = 16

    # Year-over-year growth: ~8% (slightly above target)
    year_factor = 1.0 if current_date.year < date.today().year else 1.08
    n_orders = max(1, int(np.random.poisson(base_volume * year_factor)))

    for _ in range(n_orders):
        acc = random.choices(ACCOUNTS, weights=VOLUME_WEIGHTS)[0]
        prod_name = random.choice(list(PRODUCTS.keys()))
        prod = PRODUCTS[prod_name]

        rev  = prod["base_rev"]  * random.uniform(0.9, 1.15)
        cogs = prod["base_cost"] * random.uniform(0.88, 1.08)

        # Remake rate: ~3% baseline, certain products higher
        remake_prob = 0.03
        if prod_name in ("Crown PFM", "Full Denture", "Partial Denture"):
            remake_prob = 0.055
        is_remake = random.random() < remake_prob

        ship_days = random.randint(3, 12)
        ship_date = current_date + timedelta(days=ship_days)

        orders.append({
            "order_id":    f"ORD{order_id:06d}",
            "account_id":  acc[0],
            "product_type": prod_name,
            "revenue":     round(rev, 2),
            "cogs":        round(cogs, 2),
            "order_date":  current_date.isoformat(),
            "ship_date":   ship_date.isoformat(),
            "status":      "Remake" if is_remake else "Complete",
            "tech_id":     f"TECH{random.randint(1, 6):02d}",
        })
        order_id += 1

orders_df = pd.DataFrame(orders)
orders_df.to_csv(OUT / "orders.csv", index=False)
print(f"✓  orders.csv  ({len(orders_df):,} rows)")

# ── Implant Pipeline (open cases) ──────────────────────────────────────────
STAGES = ["Scan Received", "Design", "Milling", "Sintering", "Finishing", "QC", "Ship Ready"]

implants = []
for i in range(1, 38):
    acc = random.choice(ACCOUNTS)
    rx_days_ago = random.randint(1, 21)
    rx_date = date.today() - timedelta(days=rx_days_ago)
    due_date = rx_date + timedelta(days=14)
    stage_idx = min(int(rx_days_ago / 21 * len(STAGES)), len(STAGES) - 1)

    implants.append({
        "case_id":      f"IMP{i:04d}",
        "account_id":   acc[0],
        "rx_date":      rx_date.isoformat(),
        "due_date":     due_date.isoformat(),
        "stage":        STAGES[stage_idx],
        "product_type": random.choice(["Implant Crown", "Implant Abutment",
                                       "Implant Bridge", "All-on-4 Prosthesis"]),
        "tooth_number": random.randint(1, 32),
    })

implants_df = pd.DataFrame(implants)
implants_df.to_csv(OUT / "implants_pipeline.csv", index=False)
print(f"✓  implants_pipeline.csv  ({len(implants_df)} rows)")

# ── QuickBooks stub (monthly P&L) ──────────────────────────────────────────
months = pd.date_range(start=str(date.today().year - 1) + "-01-01",
                       end=date.today(), freq="MS")
qb_rows = []
for m in months:
    qb_rows.append({
        "period":      m,
        "revenue":     round(orders_df[
            pd.to_datetime(orders_df["order_date"]).dt.to_period("M") ==
            m.to_period("M")]["revenue"].sum(), 2),
        "lab_supplies": round(random.uniform(8000, 14000), 2),
        "labor":        round(random.uniform(22000, 30000), 2),
        "overhead":     round(random.uniform(5000, 8000), 2),
    })

qb_df = pd.DataFrame(qb_rows)
qb_df["net_income"] = qb_df["revenue"] - qb_df[["lab_supplies", "labor", "overhead"]].sum(axis=1)
with pd.ExcelWriter(OUT / "qb_financials.xlsx") as xw:
    qb_df.to_excel(xw, sheet_name="P&L", index=False)
print(f"✓  qb_financials.xlsx  ({len(qb_df)} months)")

print(f"\n✅ Sample data written to ./{OUT}/")
print("   Update config.yaml → data_source.csv.watch_folder to point here,")
print("   then run:  python pipeline.py")
