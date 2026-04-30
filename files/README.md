# Artistic Dental Studio — Automated Data Pipeline

Partner analytics pipeline: Magic Touch + QuickBooks → Google Drive → Streamlit Dashboard.

---

## File Overview

```
C:\ArtisticDentalPortal\
├── config.yaml              ← All settings (paths, schedule, targets, Drive folder)
├── pipeline.py              ← Data ingestion + KPI computation + Drive upload
├── dashboard.py             ← Streamlit web dashboard
├── requirements.txt         ← Python dependencies
├── generate_sample_data.py  ← Creates test CSVs to try before live data
└── cache/                   ← Auto-created; holds latest_kpis.xlsx
```

---

## Quick Start

### 1. Install dependencies
Open Command Prompt and navigate to your folder first:
```bash
cd C:\ArtisticDentalPortal
pip install -r requirements.txt
```

### 2. Generate test data (optional but recommended first)
```bash
python generate_sample_data.py
# Creates ./sample_exports/ with realistic CSV/Excel files
```

Update `config.yaml` → `data_source.csv.watch_folder` to:
```yaml
watch_folder: "C:/ArtisticDentalPortal/sample_exports"
```

### 3. Configure Google Drive

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project → enable **Google Drive API**
3. Create **OAuth 2.0 credentials** (Desktop app type) → download as `credentials.json`
4. Place `credentials.json` in `C:\ArtisticDentalPortal\`
5. In Google Drive, create a shared folder for both partners
6. Copy the folder ID from the URL: `drive.google.com/drive/folders/`**`THIS_PART`**
7. Paste it into `config.yaml` → `google_drive.folder_id`

First pipeline run will open a browser for Google auth — token is saved as `token.pickle` for subsequent runs.

### 4. Run the pipeline once
```bash
python pipeline.py
# Reads CSVs → computes KPIs → saves cache/latest_kpis.xlsx → uploads to Drive
```

### 5. Launch the dashboard
```bash
streamlit run dashboard.py
```

---

## Running on a Schedule (Nightly at 6 AM)

### Option A: Daemon mode (keep a terminal open)
```bash
python pipeline.py --daemon
```

### Option B: Windows Task Scheduler (recommended for production)
1. Open **Task Scheduler** → Create Basic Task
2. Trigger: Daily at 6:00 AM
3. Action: Start a program
   - Program: `C:\Python312\python.exe`
   - Arguments: `pipeline.py`
   - Start in: `C:\ArtisticDentalPortal\`
4. Set the task to **Run as your Windows domain user** (not SYSTEM) with "Run whether user is logged on or not" checked

> **Note:** If you ever move the folder to a network/server drive, replace the "Start in" path with the UNC path (e.g. `\\YourServer\ShareName\ArtisticDentalPortal\`) — mapped drive letters like S: are not visible to Task Scheduler at startup.

### Option C: Streamlit Cloud (dashboard only — partners access from anywhere)
1. Push this repo to GitHub (private)
2. Deploy at [share.streamlit.io](https://share.streamlit.io)
3. Set `cache/latest_kpis.xlsx` to pull from Google Drive (already handled in `dashboard.py`)
4. Share the app URL with both partners

---

## Upgrading to Direct Database (Phase 2)

When Magic Touch confirms ODBC access:

1. Set up a DSN in **Windows → ODBC Data Sources (64-bit)**
   - Name it `MagicTouchDSN`
   - Point it at the Magic Touch SQL Server instance

2. Edit **one line** in `config.yaml`:
   ```yaml
   data_source:
     mode: odbc   # ← change from "csv" to "odbc"
   ```

3. Optionally adjust the SQL queries in `pipeline.py` → `_load_from_odbc()`
   to match Magic Touch's actual table/column names.

4. Uncomment `pyodbc` in `requirements.txt` and run `pip install pyodbc`

No other changes needed.

---

## Dashboard Sections

| Section | What it shows |
|---|---|
| **KPI Row** | YTD Revenue · Growth vs Target · Remake Rate · Avg Margin · Implants Open · Top Accounts |
| **KPI Gauges** | Visual gauges for Growth %, Remake Rate, Avg Margin |
| **YTD vs Target** | Monthly bars vs prior year + 7% target line |
| **Profitability Rankings** | All accounts ranked by gross profit + margin % |
| **Top 20% Accounts** | Pareto chart showing revenue concentration |
| **Remake Trends** | Monthly remake rate over time with alert threshold |
| **Implant Pipeline** | Open cases by stage, days in progress, overdue flags |

---

## Adjusting Business Targets

All targets are in `config.yaml`:

```yaml
targets:
  annual_growth_pct: 7.0        # YTD growth goal
  pareto_threshold: 0.80        # top accounts = 80% of revenue
  remake_alert_pct: 5.0         # flag if monthly remake rate exceeds this
  implant_turnaround_days: 14   # expected case turnaround time
```

---

## Column Name Mapping

The pipeline expects these column names in your Magic Touch CSV exports.
If the actual column names differ, update the `_clean_orders()` function in `pipeline.py`.

| Expected | Description |
|---|---|
| `order_id` | Unique case/order identifier |
| `account_id` | Practice/doctor identifier |
| `product_type` | Type of restoration (Crown, Bridge, etc.) |
| `revenue` | Billable amount |
| `cogs` | Lab cost for the case |
| `order_date` | Date case was received |
| `ship_date` | Date case was shipped |
| `status` | "Complete" or "Remake" |
