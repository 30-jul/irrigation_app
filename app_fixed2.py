from flask import Flask, render_template, request, Response, redirect, url_for
from datetime import date, datetime, timedelta
from collections import defaultdict
import requests
from bs4 import BeautifulSoup  # harmless if not used

# ---------------------------
# NEW: MySQL connector import
# ---------------------------
try:
    import mysql.connector
except ImportError:
    mysql = None
    print("mysql-connector-python is not installed. Run: pip install mysql-connector-python")
else:
    mysql = mysql.connector

from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    login_required,
    logout_user,
    current_user,
)
from werkzeug.security import generate_password_hash, check_password_hash


# ---------------------------------------------------
# NEW: MySQL CONFIG + LOAD/SAVE HELPERS
# ---------------------------------------------------
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "Progre$$94"),
    "database": os.getenv("DB_NAME", "irrigation_app"),
    "port": int(os.getenv("DB_PORT", 3306))
}



from flask import Flask, render_template, request, redirect, url_for, Response

app = Flask(__name__)
app.secret_key = "greenfuel-agric-monitor"


# ---------------------------------------------------
# GLOBAL DATA STRUCTURES (in-memory, synced with MySQL)
# ---------------------------------------------------

# 30 blocks: 1–21, A1–A3, B1–B3, Mac 1–Mac 14
BLOCK_NAMES = (
    [f"Block {i}" for i in range(1, 22)]
    + [f"A{i}" for i in range(1, 4)]
    + [f"B{i}" for i in range(1, 4)]
    + [f"Mac {i}" for i in range(1, 15)]
)
NUM_BLOCKS = len(BLOCK_NAMES)

# Weekly irrigation data per block (52 rows for a full year)
blocks_data = {i: [] for i in range(1, NUM_BLOCKS + 1)}

# Agronomy weekly data (growth, fertigation, chemigation)
agronomy_data = {i: [] for i in range(1, NUM_BLOCKS + 1)}

# Block metadata (cut/plant date + Kc + variety)
block_meta = {
    i: {"cut_date": "", "kc": "", "variety": ""}
    for i in range(1, NUM_BLOCKS + 1)
}

# Daily weather data for the estate
weather_data = []

# Soil-moisture manual inputs per block
soil_manual = {i: {"start_balance": 120.0, "by_date": {}} for i in range(1, NUM_BLOCKS + 1)}

# NDVI / biomass records
ndvi_data = []

# Pest & disease records
pests_data = []

DEFAULT_ROWS = 52
MAX_DEFICIT_BALANCE = 120.0  # mm, cap for soil-moisture P&L

# ---------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------


def init_block_rows(block_id: int):
    """Create default weekly rows for a block if empty."""
    if not blocks_data[block_id]:
        rows = []
        for i in range(1, DEFAULT_ROWS + 1):
            rows.append(
                {
                    "week": f"Week {i}",
                    "scheduled": "",
                    "actual": "",
                    "eff_rain": "",
                    "percent": "",
                    "comment": "",
                }
            )
        blocks_data[block_id] = rows


def init_agronomy_rows(block_id: int):
    """Create default weekly agronomy rows for a block if empty."""
    if not agronomy_data[block_id]:
        rows = []
        for i in range(1, DEFAULT_ROWS + 1):
            rows.append(
                {
                    "week": f"Week {i}",
                    "gain": "",
                    "cumulative": "",
                    "fertigation": "",
                    "chemigation": "",
                }
            )
        agronomy_data[block_id] = rows


def safe_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def current_week_index(cut_str: str, today: date):
    """Return (week_index, cut_dt, first_monday) from cut_date; Monday-based weeks."""
    if not cut_str:
        return None
    try:
        cut_dt = datetime.strptime(cut_str, "%Y-%m-%d").date()
    except ValueError:
        return None
    if today < cut_dt:
        return None

    first_monday = cut_dt - timedelta(days=cut_dt.weekday())  # Monday = 0
    week_index = (today - first_monday).days // 7
    return week_index, cut_dt, first_monday


def current_week_percent(block_id: int, today: date):
    """Return % for the current week only, using Monday-based weeks from cut date."""
    meta = block_meta[block_id]
    res = current_week_index(meta.get("cut_date"), today)
    if res is None:
        return None
    week_index, _, _ = res

    rows = blocks_data[block_id]
    if week_index < 0 or week_index >= len(rows):
        return None

    val = safe_float(rows[week_index].get("percent"))
    return round(val, 1) if val is not None else None


def season_total_mm(block_id: int):
    """Total season water applied (Actual + Effective Rain) in mm."""
    rows = blocks_data[block_id]
    total = 0.0
    has_data = False
    for r in rows:
        a = safe_float(r.get("actual"))
        e = safe_float(r.get("eff_rain"))
        if a is not None:
            total += a
            has_data = True
        if e is not None:
            total += e
            has_data = True
    return round(total, 1) if has_data else None


def agronomy_weekly_and_cum(block_id: int, today: date):
    """Return (weekly_gain, cumulative_growth) for current week for a block."""
    init_agronomy_rows(block_id)
    meta = block_meta[block_id]
    res = current_week_index(meta.get("cut_date"), today)
    if res is None:
        return None, None
    week_index, _, _ = res

    rows = agronomy_data[block_id]
    if week_index < 0 or week_index >= len(rows):
        return None, None

    row = rows[week_index]
    g = safe_float(row.get("gain"))
    c = safe_float(row.get("cumulative"))
    return (
        round(g, 1) if g is not None else None,
        round(c, 1) if c is not None else None,
    )
def pct_color(pct):
    """Colour for percentage bar (current week view)."""
    if pct is None:
        return "#bdbdbd"  # grey for missing
    try:
        p = float(pct)
    except Exception:
        return "#bdbdbd"

    # Blue 90–110%, Green 70–90 & 110–130, Red <50 or >150, amber for mid
    if 90 <= p <= 110:
        return "#1565c0"  # blue
    if (70 <= p < 90) or (110 < p <= 130):
        return "#2e7d32"  # green
    if p < 50 or p > 150:
        return "#c62828"  # red
    return "#f9a825"     # amber for 50–70 and 130–150


# ---------------------------------------------------
# PREVIOUS WEEK CALCULATIONS (CALENDAR-BASED)
# ---------------------------------------------------

def get_current_week_window(today):
    """Return Monday–Sunday window for the current calendar week."""
    start = today - timedelta(days=today.weekday())   # Monday
    end = start + timedelta(days=6)                   # Sunday
    return start, end


def get_previous_week_window(today):
    """Return Monday–Sunday for the previous calendar week."""
    curr_mon, _ = get_current_week_window(today)
    prev_mon = curr_mon - timedelta(days=7)
    prev_sun = prev_mon + timedelta(days=6)
    return prev_mon, prev_sun


def extract_weather_range(start, end):
    """Extract daily weather rows inside a specific date window."""
    rows = []
    for r in weather_data:
        d = r["date"]
        if start <= d <= end:
            rows.append(r)
    rows = sorted(rows, key=lambda x: x["date"])
    return rows


def extract_irrigation_previous_week(today):
    """
    For each block, compute previous-week irrigation performance:
    % = (Actual + EffRain sum) / (Scheduled sum) × 100
    Only for calendar previous Monday–Sunday.
    """
    prev_mon, prev_sun = get_previous_week_window(today)

    results_labels = []
    results_values = []
    results_colors = []

    for block_id in range(1, NUM_BLOCKS + 1):
        block_name = BLOCK_NAMES[block_id - 1]
        rows = blocks_data[block_id]

        sched_sum = 0.0
        actual_sum = 0.0

        # Weekly rows like: "Week 3 (10 Feb–16 Feb)"
        for r in rows:
            week_label = r["week"]
            if "(" in week_label and "–" in week_label:
                try:
                    inside = week_label.split("(")[1].split(")")[0]
                    d1_str, d2_str = inside.split("–")
                    d1 = datetime.strptime(d1_str.strip() + f" {today.year}", "%d %b %Y").date()
                    d2 = datetime.strptime(d2_str.strip() + f" {today.year}", "%d %b %Y").date()
                except Exception:
                    continue
            else:
                continue

            # Skip weeks outside the previous-week window
            if d2 < prev_mon or d1 > prev_sun:
                continue

            s = safe_float(r.get("scheduled"))
            a = safe_float(r.get("actual"))
            e = safe_float(r.get("eff_rain"))

            if s is not None:
                sched_sum += s
            if a is not None:
                actual_sum += a
            if e is not None:
                actual_sum += e

        if sched_sum > 0:
            pct = round((actual_sum / sched_sum) * 100, 1)
        else:
            pct = None

        if pct is None:
            continue

        results_labels.append(block_name)
        results_values.append(pct)
        results_colors.append(pct_color(pct))

    return results_labels, results_values, results_colors


def extract_agronomy_previous_week(today):
    """
    Extract previous-week agronomy: gain + cumulative.
    Uses calendar previous week (Mon–Sun) to pick the row.
    """
    prev_mon, prev_sun = get_previous_week_window(today)

    labels = []
    weekly = []
    cum = []

    for block_id in range(1, NUM_BLOCKS + 1):
        block_name = BLOCK_NAMES[block_id - 1]
        init_agronomy_rows(block_id)
        rows = agronomy_data[block_id]

        selected_gain = None
        selected_cum = None

        for r in rows:
            week_label = r["week"]
            if "(" in week_label and "–" in week_label:
                try:
                    inside = week_label.split("(")[1].split(")")[0]
                    d1_str, d2_str = inside.split("–")
                    d1 = datetime.strptime(d1_str.strip() + f" {today.year}", "%d %b %Y").date()
                    d2 = datetime.strptime(d2_str.strip() + f" {today.year}", "%d %b %Y").date()
                except Exception:
                    continue
            else:
                continue

            if d2 < prev_mon or d1 > prev_sun:
                continue

            selected_gain = safe_float(r.get("gain"))
            selected_cum = safe_float(r.get("cumulative"))
            break

        labels.append(block_name)
        weekly.append(selected_gain if selected_gain is not None else 0)
        cum.append(selected_cum if selected_cum is not None else 0)

    return labels, weekly, cum


def fetch_forecast():
    """
    Advanced 7-day forecast from yr.no JSON API for Chisumbanje Business Centre.
    """
    url = "https://www.yr.no/api/v0/locations/2-893332/forecast"

    empty = {
        "headers": ["Date", "Symbol", "Temp (°C)", "Rain (mm)"],
        "rows": [],
        "days": [],
        "chart_labels": [],
        "chart_temp": [],
        "chart_rain": [],
    }

    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print("Forecast fetch failed:", e)
        return empty

    try:
        data = resp.json()
    except Exception as e:
        print("Forecast JSON parse failed:", e)
        return empty

    raw_days = data.get("days") or data.get("dayIntervals") or []
    if not isinstance(raw_days, list):
        return empty

    from datetime import datetime as _dt

    days = []
    table_rows = []
    chart_labels = []
    chart_temp = []
    chart_rain = []

    for d in raw_days[:7]:
        date_str = d.get("date") or d.get("time") or ""
        if len(date_str) >= 10:
            iso = date_str[:10]
        else:
            iso = date_str

        weekday = ""
        short_date = iso
        try:
            dt_obj = _dt.strptime(iso, "%Y-%m-%d").date()
            weekday = dt_obj.strftime("%a")
            short_date = dt_obj.strftime("%d %b")
        except Exception:
            pass

        temp_val = None
        temp_info = d.get("temperature") or {}
        if isinstance(temp_info, dict):
            temp_val = (
                temp_info.get("value")
                or temp_info.get("max")
                or temp_info.get("min")
            )
        if temp_val is not None:
            try:
                temp_val = round(float(temp_val), 1)
            except Exception:
                pass

        rain_val = None
        precip = d.get("precipitation") or {}
        if isinstance(precip, dict):
            rain_val = (
                precip.get("value")
                or precip.get("max")
                or precip.get("min")
            )
        if rain_val is not None:
            try:
                rain_val = round(float(rain_val), 1)
            except Exception:
                pass

        symbol_code = None
        symbol_info = d.get("symbol") or {}
        if isinstance(symbol_info, dict):
            symbol_code = symbol_info.get("code") or symbol_info.get("id")

        emoji = "🌤"
        text = symbol_code or ""
        if symbol_code:
            code_lower = symbol_code.lower()
            if "thunder" in code_lower:
                emoji = "⛈"
            elif "rain" in code_lower or "shower" in code_lower:
                emoji = "🌧"
            elif "snow" in code_lower:
                emoji = "❄️"
            elif "cloud" in code_lower or "fog" in code_lower:
                emoji = "☁️"
            elif "sun" in code_lower or "clear" in code_lower:
                emoji = "☀️"

        days.append(
            {
                "iso_date": iso,
                "weekday": weekday,
                "date_short": short_date,
                "temp": temp_val,
                "temp_str": f"{temp_val}°C" if temp_val is not None else "-",
                "rain": rain_val if rain_val is not None else 0,
                "rain_str": f"{rain_val} mm" if rain_val is not None else "0 mm",
                "symbol": symbol_code or "",
                "emoji": emoji,
                "symbol_text": text,
            }
        )

        table_rows.append(
            [
                short_date,
                text or "-",
                f"{temp_val}°C" if temp_val is not None else "-",
                rain_val if rain_val is not None else "0",
            ]
        )

              # Show both weekday and date on the chart, e.g. "Mon 25 Nov"
        label = ""
        if weekday and short_date:
            label = f"{weekday} {short_date}"
        else:
            label = short_date or iso
        chart_labels.append(label)

        chart_temp.append(temp_val if temp_val is not None else None)
        chart_rain.append(rain_val if rain_val is not None else 0)

    return {
        "headers": ["Date", "Symbol", "Temp (°C)", "Rain (mm)"],
        "rows": table_rows,
        "days": days,
        "chart_labels": chart_labels,
        "chart_temp": chart_temp,
        "chart_rain": chart_rain,
    }

def compute_soil_balance(block_id: int):
    """
    Compute current soil-moisture balance for a block using:
      Balance_today = Balance_start - ΣETc + Σ(Effective Rain + Irrigation)
    """
    manual = soil_manual[block_id]
    start_balance = manual.get("start_balance", 120.0)
    balance = start_balance
    kc_val = safe_float(block_meta[block_id].get("kc")) or 1.0

    for r in sorted(weather_data, key=lambda x: x["date"]):
        dstr = r["date_str"]
        et0 = safe_float(r.get("et0")) or 0.0
        etc = et0 * kc_val

        md = manual.get("by_date", {}).get(dstr, {})
        eff = safe_float(md.get("eff")) or 0.0
        irr = safe_float(md.get("irr")) or 0.0

        balance = balance - etc + eff + irr
        if balance > MAX_DEFICIT_BALANCE:
            balance = MAX_DEFICIT_BALANCE
        if balance < 0:
            balance = 0

    return round(balance, 1)
def soil_pct_color(balance, tam):
    """
    Return (pct, colour_hex) based on balance/TAM * 100.

    Colour bands:
      blue       95–100%
      light blue 90–94%
      green      70–89%
      orange     50–69%
      red        <50%
    """
    if balance is None or tam is None or tam <= 0:
        return 0.0, "#bdbdbd"  # grey for missing

    pct = (float(balance) / float(tam)) * 100.0

    if pct >= 95:
        colour = "#1565c0"   # blue
    elif pct >= 90:
        colour = "#42a5f5"   # light blue
    elif pct >= 70:
        colour = "#2e7d32"   # green
    elif pct >= 50:
        colour = "#fb8c00"   # orange
    else:
        colour = "#c62828"   # red

    return pct, colour



# ---------------------------------------------------
# NEW: MySQL CONFIG + LOAD/SAVE HELPERS
# ---------------------------------------------------
def get_db():
    """
    Return a MySQL connection using Aiven SSL settings.
    """
    return mysql.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        port=DB_CONFIG.get("port", 3306),
        ssl_ca=None,         # Aiven handles SSL internally
        ssl_disabled=False   # Force SSL on
    )

def init_db():
    """Create tables if they don't exist."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            tmax DOUBLE NULL,
            tmin DOUBLE NULL,
            rain DOUBLE NULL,
            et0 DOUBLE NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(50) NOT NULL UNIQUE,
            password_hash VARCHAR(255) NOT NULL,
            role VARCHAR(20) NOT NULL DEFAULT 'user'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS blocks_meta (
            block_id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            cut_date DATE NULL,
            kc DOUBLE NULL,
            variety VARCHAR(50) NULL,
            sm_start_balance DOUBLE NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS irrigation_weeks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            block_id INT NOT NULL,
            week_index INT NOT NULL,
            week_label VARCHAR(100),
            scheduled DOUBLE NULL,
            actual DOUBLE NULL,
            eff_rain DOUBLE NULL,
            percent DOUBLE NULL,
            comment TEXT,
            UNIQUE KEY uniq_block_week (block_id, week_index)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS soil_manual_entries (
            id INT AUTO_INCREMENT PRIMARY KEY,
            block_id INT NOT NULL,
            date DATE NOT NULL,
            eff DOUBLE NULL,
            irr DOUBLE NULL,
            UNIQUE KEY uniq_block_date (block_id, date)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agronomy_weeks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            block_id INT NOT NULL,
            week_index INT NOT NULL,
            week_label VARCHAR(100),
            gain DOUBLE NULL,
            cumulative DOUBLE NULL,
            fertigation VARCHAR(100),
            chemigation VARCHAR(100),
            UNIQUE KEY uniq_agro_block_week (block_id, week_index)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ndvi_records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            block_id INT NOT NULL,
            ndvi DOUBLE NOT NULL,
            biomass DOUBLE NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pests_records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            block_id INT NOT NULL,
            pest VARCHAR(100) NOT NULL,
            severity VARCHAR(20),
            area DOUBLE NULL,
            action TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


# ------------ LOAD FROM DB INTO MEMORY ------------

def load_weather_from_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT date, tmax, tmin, rain, et0 FROM weather ORDER BY date")
    weather_data.clear()
    for d, tmax, tmin, rain, et0 in cur.fetchall():
        d_str = d.strftime("%Y-%m-%d")
        weather_data.append(
            {
                "date": d,
                "date_str": d_str,
                "tmax": tmax if tmax is not None else "",
                "tmin": tmin if tmin is not None else "",
                "rain": rain if rain is not None else "",
                "et0": et0 if et0 is not None else "",
            }
        )
    cur.close()
    conn.close()


def load_blocks_from_db():
    # ensure base structure
    for bid in range(1, NUM_BLOCKS + 1):
        init_block_rows(bid)
        init_agronomy_rows(bid)

    conn = get_db()
    cur = conn.cursor()

    # blocks_meta
    cur.execute("SELECT block_id, name, cut_date, kc, variety, sm_start_balance FROM blocks_meta")
    for block_id, name, cut_date, kc, variety, sm_start_balance in cur.fetchall():
        if 1 <= block_id <= NUM_BLOCKS:
            block_meta[block_id]["cut_date"] = cut_date.strftime("%Y-%m-%d") if cut_date else ""
            block_meta[block_id]["kc"] = "" if kc is None else str(kc)
            block_meta[block_id]["variety"] = variety or ""
            if sm_start_balance is not None:
                soil_manual[block_id]["start_balance"] = float(sm_start_balance)

    # irrigation weeks
    cur.execute("""
        SELECT block_id, week_index, week_label, scheduled, actual, eff_rain, percent, comment
        FROM irrigation_weeks
    """)
    for block_id, week_index, week_label, scheduled, actual, eff_rain, percent, comment in cur.fetchall():
        if 1 <= block_id <= NUM_BLOCKS and 0 <= week_index < DEFAULT_ROWS:
            rows = blocks_data[block_id]
            rows[week_index]["week"] = week_label or rows[week_index]["week"]
            rows[week_index]["scheduled"] = "" if scheduled is None else str(scheduled)
            rows[week_index]["actual"] = "" if actual is None else str(actual)
            rows[week_index]["eff_rain"] = "" if eff_rain is None else str(eff_rain)
            rows[week_index]["percent"] = "" if percent is None else str(percent)
            rows[week_index]["comment"] = comment or ""

    # soil manual entries
    cur.execute("SELECT block_id, date, eff, irr FROM soil_manual_entries")
    for block_id, d, eff, irr in cur.fetchall():
        if 1 <= block_id <= NUM_BLOCKS:
            d_str = d.strftime("%Y-%m-%d")
            soil_manual[block_id]["by_date"][d_str] = {
                "eff": "" if eff is None else str(eff),
                "irr": "" if irr is None else str(irr),
            }

    # agronomy weeks
    cur.execute("""
        SELECT block_id, week_index, week_label, gain, cumulative, fertigation, chemigation
        FROM agronomy_weeks
    """)
    for block_id, week_index, week_label, gain, cumulative, fert, chem in cur.fetchall():
        if 1 <= block_id <= NUM_BLOCKS and 0 <= week_index < DEFAULT_ROWS:
            rows = agronomy_data[block_id]
            rows[week_index]["week"] = week_label or rows[week_index]["week"]
            rows[week_index]["gain"] = "" if gain is None else str(gain)
            rows[week_index]["cumulative"] = "" if cumulative is None else str(cumulative)
            rows[week_index]["fertigation"] = fert or ""
            rows[week_index]["chemigation"] = chem or ""

    cur.close()
    conn.close()


def load_ndvi_from_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT date, block_id, ndvi, biomass FROM ndvi_records ORDER BY date, block_id")
    ndvi_data.clear()
    for d, block_id, ndvi, biomass in cur.fetchall():
        ndvi_data.append(
            {
                "date": d,
                "date_str": d.strftime("%Y-%m-%d"),
                "block_id": block_id,
                "ndvi": ndvi,
                "biomass": biomass,
            }
        )
    cur.close()
    conn.close()


def load_pests_from_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT date, block_id, pest, severity, area, action FROM pests_records ORDER BY date, block_id"
    )
    pests_data.clear()
    for d, block_id, pest, severity, area, action in cur.fetchall():
        pests_data.append(
            {
                "date": d,
                "date_str": d.strftime("%Y-%m-%d"),
                "block_id": block_id,
                "pest": pest,
                "severity": severity,
                "area": area,
                "action": action,
            }
        )
    cur.close()
    conn.close()


# ------------ SAVE / UPSERT HELPERS ------------

def save_weather_to_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM weather")
    for r in weather_data:
        d = r["date"]
        tmax = safe_float(r.get("tmax"))
        tmin = safe_float(r.get("tmin"))
        rain = safe_float(r.get("rain"))
        et0 = safe_float(r.get("et0"))
        cur.execute(
            "INSERT INTO weather (date, tmax, tmin, rain, et0) VALUES (%s,%s,%s,%s,%s)",
            (d, tmax, tmin, rain, et0),
        )
    conn.commit()
    cur.close()
    conn.close()


def save_block_meta_to_db(block_id):
    conn = get_db()
    cur = conn.cursor()
    meta = block_meta[block_id]
    cut_date = meta["cut_date"] or None
    if cut_date:
        try:
            cut_date = datetime.strptime(cut_date, "%Y-%m-%d").date()
        except ValueError:
            cut_date = None
    kc_val = safe_float(meta["kc"])
    variety = meta["variety"] or None
    sm_start = soil_manual[block_id].get("start_balance", 120.0)
    cur.execute(
        """
        REPLACE INTO blocks_meta (block_id, name, cut_date, kc, variety, sm_start_balance)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (block_id, BLOCK_NAMES[block_id - 1], cut_date, kc_val, variety, sm_start),
    )
    conn.commit()
    cur.close()
    conn.close()


def save_block_irrigation_to_db(block_id):
    conn = get_db()
    cur = conn.cursor()
    rows = blocks_data[block_id]
    for i, r in enumerate(rows):
        week_label = r["week"]
        scheduled = safe_float(r.get("scheduled"))
        actual = safe_float(r.get("actual"))
        eff_rain = safe_float(r.get("eff_rain"))
        percent = safe_float(r.get("percent"))
        comment = r.get("comment") or None
        cur.execute(
            """
            REPLACE INTO irrigation_weeks
            (block_id, week_index, week_label, scheduled, actual, eff_rain, percent, comment)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (block_id, i, week_label, scheduled, actual, eff_rain, percent, comment),
        )
    conn.commit()
    cur.close()
    conn.close()


def save_soil_manual_block_to_db(block_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM soil_manual_entries WHERE block_id=%s", (block_id,))
    for d_str, vals in soil_manual[block_id]["by_date"].items():
        try:
            d = datetime.strptime(d_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        eff = safe_float(vals.get("eff"))
        irr = safe_float(vals.get("irr"))
        cur.execute(
            """
            INSERT INTO soil_manual_entries (block_id, date, eff, irr)
            VALUES (%s,%s,%s,%s)
            """,
            (block_id, d, eff, irr),
        )
    conn.commit()
    cur.close()
    conn.close()


def save_agronomy_block_to_db(block_id):
    conn = get_db()
    cur = conn.cursor()
    rows = agronomy_data[block_id]
    for i, r in enumerate(rows):
        week_label = r["week"]
        gain = safe_float(r.get("gain"))
        cumulative = safe_float(r.get("cumulative"))
        fert = r.get("fertigation") or None
        chem = r.get("chemigation") or None
        cur.execute(
            """
            REPLACE INTO agronomy_weeks
            (block_id, week_index, week_label, gain, cumulative, fertigation, chemigation)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (block_id, i, week_label, gain, cumulative, fert, chem),
        )
    conn.commit()
    cur.close()
    conn.close()


def insert_ndvi_record_to_db(rec):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ndvi_records (date, block_id, ndvi, biomass)
        VALUES (%s,%s,%s,%s)
        """,
        (rec["date"], rec["block_id"], rec["ndvi"], rec["biomass"]),
    )
    conn.commit()
    cur.close()
    conn.close()


def insert_pest_record_to_db(rec):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pests_records (date, block_id, pest, severity, area, action)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (rec["date"], rec["block_id"], rec["pest"], rec["severity"], rec["area"], rec["action"]),
    )
    conn.commit()
    cur.close()
    conn.close()


# ---------------------------------------------------
# LOAD DATA ONCE WHEN APP STARTS
# ---------------------------------------------------

# Flask 3.x compatibility: load DB once
db_loaded = False

@app.before_request
def startup_load():
    global db_loaded
    if not db_loaded:
        try:
            init_db()
            load_weather_from_db()
            load_blocks_from_db()
            load_ndvi_from_db()
            load_pests_from_db()
            print("MySQL data loaded into memory.")
            db_loaded = True
        except Exception as e:
            print("DB init/load failed:", e)

# ---------------------------------------------------
# ROUTES
# ---------------------------------------------------

# --------- OVERVIEW PAGE ---------
# --------- AUTH ROUTES ---------

@app.route("/login", methods=["GET", "POST"])
def login():
    return redirect(url_for("index"))

@app.route("/")
def index():
    today = date.today()
    view_mode = request.args.get("view", "week")  # "week" or "season"

    # Big-screen flag (?tv=1)
    tv_mode = request.args.get("tv", "0") == "1"

    # ---------------------------
    # 1. Latest 7 weather rows
    # ---------------------------
    latest_rows = sorted(weather_data, key=lambda x: x["date"], reverse=True)[:7]
    latest_rows = list(reversed(latest_rows))
    weather_row_count = len(latest_rows)

    # ---------------------------
    # 2. Current month summary
    # ---------------------------
    monthly = defaultdict(lambda: {"tmax": [], "tmin": [], "rain": [], "et0": []})
    for r in weather_data:
        d = r.get("date")
        if not isinstance(d, date):
            continue
        if d.year == today.year and d.month == today.month:
            for field in ["tmax", "tmin", "rain", "et0"]:
                v = safe_float(r.get(field))
                if v is not None:
                    monthly[(d.year, d.month)][field].append(v)

    monthly_stats = []
    vals = monthly.get((today.year, today.month))
    if vals:
        avg_tmax = round(sum(vals["tmax"]) / len(vals["tmax"]), 1) if vals["tmax"] else None
        avg_tmin = round(sum(vals["tmin"]) / len(vals["tmin"]), 1) if vals["tmin"] else None
        sum_rain = round(sum(vals["rain"]), 1) if vals["rain"] else None
        if vals["et0"]:
            avg_et0 = round(sum(vals["et0"]) / len(vals["et0"]), 2)
            cum_et0 = round(sum(vals["et0"]), 2)
        else:
            avg_et0 = None
            cum_et0 = None
        monthly_stats.append(
            {
                "label": f"{today.year}-{today.month:02d}",
                "avg_tmax": avg_tmax,
                "avg_tmin": avg_tmin,
                "sum_rain": sum_rain,
                "avg_et0": avg_et0,
                "cum_et0": cum_et0,
            }
        )

    # ---------------------------
    # 3. Block performance (weekly % or season total)
    # ---------------------------
    comparison_labels = []
    comparison_values = []
    comparison_colors = []

    for block_id in range(1, NUM_BLOCKS + 1):
        name = BLOCK_NAMES[block_id - 1]
        if view_mode == "week":
            pct = current_week_percent(block_id, today)
            if pct is None:
                continue
            comparison_labels.append(name)
            comparison_values.append(pct)
            comparison_colors.append(pct_color(pct))
        else:  # season totals
            total_mm = season_total_mm(block_id)
            if total_mm is None:
                continue
            comparison_labels.append(name)
            comparison_values.append(total_mm)
            comparison_colors.append("#2e7d32")  # green bars for totals
    if view_mode == "week":
        comparison_title = "Irrigation Block Performance – Weekly % of Schedule"
        comparison_y_label = "% of scheduled water"
    else:
        comparison_title = "Irrigation Block Performance – Season Total Applied Water (mm)"
        comparison_y_label = "Total depth (mm)"

    # ---------------------------
    # 3B. PREVIOUS WEEK IRRIGATION
    # ---------------------------
    prev_irrig_labels, prev_irrig_values, prev_irrig_colors = extract_irrigation_previous_week(today)

    # ---------------------------
    # 4. Filtered blocks chart (max 6)
    # ---------------------------
    selected_ids = []

  
    for val in request.args.getlist("filter_block"):
        try:
            bid = int(val)
        except ValueError:
            continue
        if 1 <= bid <= NUM_BLOCKS:
            selected_ids.append(bid)
    selected_ids = selected_ids[:6]

    filter_labels = []
    filter_values = []
    filter_colors = []

    if selected_ids:
        for bid in selected_ids:
            name = BLOCK_NAMES[bid - 1]
            if view_mode == "week":
                pct = current_week_percent(bid, today)
                if pct is None:
                    continue
                filter_labels.append(name)
                filter_values.append(pct)
                filter_colors.append(pct_color(pct))
            else:
                total_mm = season_total_mm(bid)
                if total_mm is None:
                    continue
                filter_labels.append(name)
                filter_values.append(total_mm)
                filter_colors.append("#2e7d32")
    else:
        # Default: show first 6 blocks if no filter chosen
        for bid in range(1, min(NUM_BLOCKS, 6) + 1):
            name = BLOCK_NAMES[bid - 1]
            if view_mode == "week":
                pct = current_week_percent(bid, today)
                if pct is None:
                    continue
                filter_labels.append(name)
                filter_values.append(pct)
                filter_colors.append(pct_color(pct))
            else:
                total_mm = season_total_mm(bid)
                if total_mm is None:
                    continue
                filter_labels.append(name)
                filter_values.append(total_mm)
                filter_colors.append("#2e7d32")

    # ---------------------------
    # 5. 7-day forecast (with dates)
    # ---------------------------
    fc = fetch_forecast()
    forecast_headers = fc["headers"]
    forecast_rows = fc["rows"]
    forecast_days = fc["days"]
    forecast_chart_labels = fc["chart_labels"]
    forecast_chart_temp = fc["chart_temp"]
    forecast_chart_rain = fc["chart_rain"]
    # ---------------------------
    # 5B. PREVIOUS WEEK WEATHER (CALENDAR)
    # ---------------------------
    prev_mon, prev_sun = get_previous_week_window(today)
    prev_weather_rows = extract_weather_range(prev_mon, prev_sun)

    prev_weather_chart_labels = [r["date_str"] for r in prev_weather_rows]
    prev_weather_chart_temp = [
        safe_float(r["tmax"]) if safe_float(r["tmax"]) is not None else None
        for r in prev_weather_rows
    ]
    prev_weather_chart_rain = [
        safe_float(r["rain"]) if safe_float(r["rain"]) is not None else 0
        for r in prev_weather_rows
    ]

    # ---------------------------
    # 6. Agronomy snapshot arrays
    # ---------------------------
    agro_labels = []
    agro_weekly = []
    agro_cum = []

    for block_id in range(1, NUM_BLOCKS + 1):
        name = BLOCK_NAMES[block_id - 1]
        weekly_gain, cum_height = agronomy_weekly_and_cum(block_id, today)
        agro_labels.append(name)
        agro_weekly.append(weekly_gain if weekly_gain is not None else 0)
        agro_cum.append(cum_height if cum_height is not None else 0)
    # ---------------------------
    # 6B. PREVIOUS WEEK AGRONOMY
    # ---------------------------
    prev_agro_labels, prev_agro_weekly, prev_agro_cum =  extract_agronomy_previous_week(today)

    # ---------------------------
    # 7. Latest soil moisture balance (per block)
    # ---------------------------
    latest_balances = {}

    for block_id in range(1, NUM_BLOCKS + 1):
        name = BLOCK_NAMES[block_id - 1]

        try:
            bal = compute_soil_balance(block_id)
        except Exception:
            bal = None

        bal_val = bal if bal is not None else 0.0
        tam = soil_manual[block_id].get("start_balance", MAX_DEFICIT_BALANCE)

        pct, colour = soil_pct_color(bal_val, tam)

        latest_balances[name] = {
            "balance": round(bal_val, 1),
            "tam": float(tam),
            "pct": round(pct, 1),
            "color": colour
        }

    # ---------------------------
    # 8. NDVI averages by block
    # ---------------------------
    ndvi_sum = defaultdict(float)
    ndvi_count = defaultdict(int)
    for rec in ndvi_data:
        name = BLOCK_NAMES[rec["block_id"] - 1]
        v = safe_float(rec.get("ndvi"))
        if v is not None:
            ndvi_sum[name] += v
            ndvi_count[name] += 1

    avg_ndvi_by_block = {}
    for name, total in ndvi_sum.items():
        c = ndvi_count[name]
        if c > 0:
            avg_ndvi_by_block[name] = round(total / c, 3)

    # ---------------------------
    # 9. Pest counts per block
    # ---------------------------
    pest_counts = defaultdict(int)
    for rec in pests_data:
        name = BLOCK_NAMES[rec["block_id"] - 1]
        pest_counts[name] += 1

    # ---------------------------
    # Growth snapshot (optional)
    # ---------------------------
    growth_by_block = {}
    for block_id in range(1, NUM_BLOCKS + 1):
        weekly_gain, cum_height = agronomy_weekly_and_cum(block_id, today)
        growth_by_block[BLOCK_NAMES[block_id - 1]] = {
            "weekly_gain": weekly_gain,
            "cumulative": cum_height,
        }

    # ----------------------------------------------------
    # RETURN TEMPLATE (correct indentation)
    # ----------------------------------------------------
    return render_template(
        "index.html",
        today=today,
        weather_rows=latest_rows,
        weather_row_count=weather_row_count,
        monthly_stats=monthly_stats,
        block_names=BLOCK_NAMES,
        comparison_labels=comparison_labels,
        comparison_values=comparison_values,
        comparison_title=comparison_title,
        comparison_y_label=comparison_y_label,
        comparison_colors=comparison_colors,
        view_mode=view_mode,
        forecast_headers=forecast_headers,
        forecast_rows=forecast_rows,
        forecast_days=forecast_days,
        forecast_chart_labels=forecast_chart_labels,
        forecast_chart_temp=forecast_chart_temp,
        forecast_chart_rain=forecast_chart_rain,
        filter_labels=filter_labels,
        filter_values=filter_values,
        filter_colors=filter_colors,
        filter_selected_ids=selected_ids,
        agro_labels=agro_labels,
        agro_weekly=agro_weekly,
        agro_cum=agro_cum,
        num_blocks=NUM_BLOCKS,
        latest_balances=latest_balances,
        avg_ndvi_by_block=avg_ndvi_by_block,
        pest_counts=pest_counts,
        growth_by_block=growth_by_block,
        # Previous week irrigation
        prev_irrig_labels=prev_irrig_labels,
        prev_irrig_values=prev_irrig_values,
        prev_irrig_colors=prev_irrig_colors,
        # Previous week weather
        prev_weather_rows=prev_weather_rows,
        prev_weather_chart_labels=prev_weather_chart_labels,
        prev_weather_chart_temp=prev_weather_chart_temp,
        prev_weather_chart_rain=prev_weather_chart_rain,
        # Previous week agronomy
        prev_agro_labels=prev_agro_labels,
        prev_agro_weekly=prev_agro_weekly,
        prev_agro_cum=prev_agro_cum,
        tv_mode=tv_mode,
    )
  


# --------- WEATHER DATA MANAGEMENT PAGE ---------

@app.route("/weather", methods=["GET", "POST"])
def weather_page():
    today = date.today()

    if request.method == "POST":
        action = request.form.get("action")

        if action == "add_weather":
            d_str = request.form.get("weather_date", "").strip()
            tmax = request.form.get("tmax", "").strip()
            tmin = request.form.get("tmin", "").strip()
            rain = request.form.get("rain", "").strip()
            et0 = request.form.get("et0", "").strip()

            if d_str:
                try:
                    d_obj = datetime.strptime(d_str, "%Y-%m-%d").date()
                except ValueError:
                    d_obj = None

                if d_obj is not None:
                    weather_data.append(
                        {
                            "date": d_obj,
                            "date_str": d_str,
                            "tmax": tmax,
                            "tmin": tmin,
                            "rain": rain,
                            "et0": et0,
                        }
                    )

        elif action == "edit_weather":
            new_list = []
            try:
                row_count = int(request.form.get("row_count", "0"))
            except ValueError:
                row_count = 0

            for i in range(row_count):
                d_str = request.form.get(f"date_{i}", "").strip()
                tmax = request.form.get(f"tmax_{i}", "").strip()
                tmin = request.form.get(f"tmin_{i}", "").strip()
                rain = request.form.get(f"rain_{i}", "").strip()
                et0 = request.form.get(f"et0_{i}", "").strip()
                delete_flag = request.form.get(f"delete_{i}")

                if not d_str or delete_flag == "on":
                    continue

                try:
                    d_obj = datetime.strptime(d_str, "%Y-%m-%d").date()
                except ValueError:
                    continue

                new_list.append(
                    {
                        "date": d_obj,
                        "date_str": d_str,
                        "tmax": tmax,
                        "tmin": tmin,
                        "rain": rain,
                        "et0": et0,
                    }
                )

            weather_data.clear()
            weather_data.extend(new_list)

        # NEW: persist to MySQL
        try:
            save_weather_to_db()
        except Exception as e:
            print("Failed to save weather to DB:", e)

    rows = sorted(weather_data, key=lambda x: x["date"])
    row_count = len(rows)

    monthly = defaultdict(lambda: {"tmax": [], "tmin": [], "rain": [], "et0": []})
    for r in rows:
        d = r["date"]
        y, m = d.year, d.month
        key = (y, m)
        for field in ["tmax", "tmin", "rain", "et0"]:
            v = safe_float(r.get(field))
            if v is not None:
                monthly[key][field].append(v)

    monthly_stats = []
    for (y, m), vals in monthly.items():
        label = f"{y}-{m:02d}"
        avg_tmax = round(sum(vals["tmax"]) / len(vals["tmax"]), 1) if vals["tmax"] else None
        avg_tmin = round(sum(vals["tmin"]) / len(vals["tmin"]), 1) if vals["tmin"] else None
        sum_rain = round(sum(vals["rain"]), 1) if vals["rain"] else None
        if vals["et0"]:
            avg_et0 = round(sum(vals["et0"]) / len(vals["et0"]), 2)
            cum_et0 = round(sum(vals["et0"]), 2)
        else:
            avg_et0 = None
            cum_et0 = None

        monthly_stats.append(
            {
                "label": label,
                "avg_tmax": avg_tmax,
                "avg_tmin": avg_tmin,
                "sum_rain": sum_rain,
                "avg_et0": avg_et0,
                "cum_et0": cum_et0,
            }
        )

    monthly_stats.sort(key=lambda x: x["label"])

    return render_template(
        "weather.html",
        today=today,
        weather_rows=rows,
        weather_row_count=row_count,
        monthly_stats=monthly_stats,
        num_blocks=NUM_BLOCKS,
        block_id=0,
        block_names=BLOCK_NAMES,
    )


# --------- DOWNLOAD WEATHER CSV ---------

@app.route("/download_weather")
def download_weather():
    import io
    import csv

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["date", "tmax", "tmin", "rain", "et0"])

    for r in sorted(weather_data, key=lambda x: x["date"]):
        writer.writerow([r["date_str"], r["tmax"], r["tmin"], r["rain"], r["et0"]])

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=weather_data.csv"},
    )


# --------- BLOCK PAGE (IRRIGATION + SOIL MOISTURE P&L) ---------

@app.route("/block/<int:block_id>", methods=["GET", "POST"])
def block_view(block_id):
    if block_id < 1 or block_id > NUM_BLOCKS:
        return redirect(url_for("index"))

    today = date.today()
    init_block_rows(block_id)

    rows = blocks_data[block_id]
    meta = block_meta[block_id]
    manual = soil_manual[block_id]
    block_name = BLOCK_NAMES[block_id - 1]

    if request.method == "POST":
        meta["cut_date"] = request.form.get("cut_date", "").strip()
        meta["kc"] = request.form.get("kc", "").strip()
        v = request.form.get("variety")
        if v is not None:
            meta["variety"] = v.strip()

        updated = []
        for i, row in enumerate(rows):
            week = request.form.get(f"week_{i}", "").strip()
            scheduled = request.form.get(f"scheduled_{i}", "").strip()
            actual = request.form.get(f"actual_{i}", "").strip()
            eff = request.form.get(f"effrain_{i}", "").strip()
            comment = request.form.get(f"comment_{i}", "").strip()

            s = safe_float(scheduled)
            a = safe_float(actual)
            e = safe_float(eff)
            combined = (a or 0) + (e or 0) if (a is not None or e is not None) else None

            if s and combined is not None and s != 0:
                pct = round((combined / s) * 100, 1)
            else:
                pct = ""

            updated.append(
                {
                    "week": week,
                    "scheduled": scheduled,
                    "actual": actual,
                    "eff_rain": eff,
                    "percent": pct,
                    "comment": comment,
                }
            )

        blocks_data[block_id] = updated
        rows = updated

        sb_str = request.form.get("sm_start_balance", "").strip()
        sb_val = safe_float(sb_str)
        if sb_val is not None:
            manual["start_balance"] = sb_val

        manual["by_date"] = {}
        try:
            sm_count = int(request.form.get("sm_row_count", "0"))
        except ValueError:
            sm_count = 0

        for i in range(sm_count):
            d_str = request.form.get(f"sm_date_{i}", "").strip()
            eff_str = request.form.get(f"sm_eff_{i}", "").strip()
            irr_str = request.form.get(f"sm_irr_{i}", "").strip()
            if not d_str:
                continue
            manual["by_date"][d_str] = {"eff": eff_str, "irr": irr_str}

        # NEW: save to MySQL
        try:
            save_block_meta_to_db(block_id)
            save_block_irrigation_to_db(block_id)
            save_soil_manual_block_to_db(block_id)
        except Exception as e:
            print(f"Failed to save block {block_id} to DB:", e)

    age_days = age_months = None
    cut_dt = None
    if meta["cut_date"]:
        try:
            cut_dt = datetime.strptime(meta["cut_date"], "%Y-%m-%d").date()
            age_days = (today - cut_dt).days
            age_months = round(age_days / 30.0, 1)
        except ValueError:
            cut_dt = None

    if cut_dt:
        first_monday = cut_dt - timedelta(days=cut_dt.weekday())
        for i in range(len(rows)):
            ws = first_monday + timedelta(days=7 * i)
            we = ws + timedelta(days=6)
            rows[i]["week"] = f"Week {i+1} ({ws.strftime('%d %b')}–{we.strftime('%d %b')})"

    labels = [r["week"] for r in rows]
    scheduled_vals = [safe_float(r["scheduled"]) for r in rows]
    actual_plus = []
    for r in rows:
        a = safe_float(r["actual"])
        e = safe_float(r["eff_rain"])
        actual_plus.append((a or 0) + (e or 0) if (a is not None or e is not None) else None)

    pcts = [safe_float(r["percent"]) for r in rows if safe_float(r["percent"]) is not None]
    avg_pct = round(sum(pcts) / len(pcts), 1) if pcts else None
    min_pct = round(min(pcts), 1) if pcts else None
    max_pct = round(max(pcts), 1) if pcts else None

    sm_rows = []
    start_balance = manual.get("start_balance", 120.0)
    balance = start_balance
    kc_val = safe_float(meta["kc"]) or 1.0

    weather_by_date = {r["date"]: r for r in weather_data}

    window_start = today - timedelta(days=6)
    if cut_dt and cut_dt > window_start:
        window_start = cut_dt

    current = window_start
    daily_list = []
    while current <= today:
        r = weather_by_date.get(current)
        if r:
            daily_list.append(r)
        current += timedelta(days=1)

    for r in daily_list:
        dstr = r["date_str"]
        et0_val = safe_float(r.get("et0")) or 0.0
        etc = round(et0_val * kc_val, 2)
        rain_val = safe_float(r.get("rain")) or 0.0

        manual_date = manual.get("by_date", {}).get(dstr, {})
        eff_str = manual_date.get("eff", "")
        irr_str = manual_date.get("irr", "")
        eff_val = safe_float(eff_str) or 0.0
        irr_val = safe_float(irr_str) or 0.0

        balance = round(balance - etc + eff_val + irr_val, 1)
        if balance > MAX_DEFICIT_BALANCE:
            balance = MAX_DEFICIT_BALANCE
        if balance < 0:
            balance = 0

        sm_rows.append(
            {
                "date_str": dstr,
                "et0": et0_val,
                "kc": kc_val,
                "etc": etc,
                "rain": rain_val,
                "eff_rain_str": eff_str,
                "irr_str": irr_str,
                "balance": balance,
            }
        )

    # 🔹 Only show the latest 7 days in the table,
    # but balances are still computed over full history.
    if len(sm_rows) > 7:
        sm_rows_display = sm_rows[-7:]
    else:
        sm_rows_display = sm_rows

    return render_template(
        "block.html",
        block_id=block_id,
        block_name=block_name,
        block_names=BLOCK_NAMES,
        rows=rows,
        avg_pct=avg_pct,
        min_pct=min_pct,
        max_pct=max_pct,
        chart_labels=labels,
        chart_scheduled=scheduled_vals,
        chart_actual=actual_plus,
        today=today,
        cut_date=meta["cut_date"],
        age_days=age_days,
        age_months=age_months,
        kc=meta["kc"],
        num_blocks=NUM_BLOCKS,
        sm_rows=sm_rows_display,
        sm_start_balance=start_balance,
        sm_row_count=len(sm_rows_display),
    )

# --------- AGRONOMY PAGE ---------

@app.route("/agronomy/<int:block_id>", methods=["GET", "POST"])
def agronomy_view(block_id):
    if block_id < 1 or block_id > NUM_BLOCKS:
        return redirect(url_for("index"))

    today = date.today()
    init_agronomy_rows(block_id)

    rows = agronomy_data[block_id]
    meta = block_meta[block_id]
    block_name = BLOCK_NAMES[block_id - 1]

    if request.method == "POST":
        meta["variety"] = request.form.get("variety", "").strip()
        cut = request.form.get("cut_date")
        if cut is not None:
            meta["cut_date"] = cut.strip()

        updated = []
        running_cum = 0.0
        for i, row in enumerate(rows):
            week = request.form.get(f"ag_week_{i}", "").strip()
            gain_str = request.form.get(f"gain_{i}", "").strip()
            fert_str = request.form.get(f"fert_{i}", "").strip()
            chem_str = request.form.get(f"chem_{i}", "").strip()

            gain_val = safe_float(gain_str)
            if gain_val is not None:
                running_cum += gain_val
                cum_str = f"{running_cum:.1f}"
            else:
                cum_str = ""

            updated.append(
                {
                    "week": week,
                    "gain": gain_str,
                    "cumulative": cum_str,
                    "fertigation": fert_str,
                    "chemigation": chem_str,
                }
            )

        agronomy_data[block_id] = updated
        rows = updated

        # NEW: save agronomy + meta to DB
        try:
            save_block_meta_to_db(block_id)
            save_agronomy_block_to_db(block_id)
        except Exception as e:
            print(f"Failed to save agronomy for block {block_id}:", e)

    age_days = age_months = None
    cut_dt = None
    if meta["cut_date"]:
        try:
            cut_dt = datetime.strptime(meta["cut_date"], "%Y-%m-%d").date()
            age_days = (today - cut_dt).days
            age_months = round(age_days / 30.0, 1)
        except ValueError:
            cut_dt = None

    if cut_dt:
        first_monday = cut_dt - timedelta(days=cut_dt.weekday())
        for i in range(len(rows)):
            ws = first_monday + timedelta(days=7 * i)
            we = ws + timedelta(days=6)
            rows[i]["week"] = f"Week {i+1} ({ws.strftime('%d %b')}–{we.strftime('%d %b')})"

    labels = [r["week"] for r in rows]
    gains = [safe_float(r["gain"]) for r in rows]
    cums = [safe_float(r["cumulative"]) for r in rows]

    return render_template(
        "agronomy.html",
        block_id=block_id,
        block_name=block_name,
        block_names=BLOCK_NAMES,
        num_blocks=NUM_BLOCKS,
        today=today,
        cut_date=meta["cut_date"],
        age_days=age_days,
        age_months=age_months,
        variety=meta["variety"],
        rows=rows,
        chart_labels=labels,
        chart_gains=gains,
        chart_cums=cums,
    )


# --------- NDVI & BIOMASS PAGE ---------

@app.route("/ndvi", methods=["GET", "POST"])
def ndvi_page():
    today = date.today()

    if request.method == "POST":
        d_str = request.form.get("date", "").strip()
        blk_id_str = request.form.get("block_id", "").strip()
        ndvi_str = request.form.get("ndvi", "").strip()

        if d_str and blk_id_str and ndvi_str:
            try:
                d_obj = datetime.strptime(d_str, "%Y-%m-%d").date()
                blk_id = int(blk_id_str)
            except ValueError:
                blk_id = None

            ndvi_val = safe_float(ndvi_str)

            if blk_id and 1 <= blk_id <= NUM_BLOCKS and ndvi_val is not None:
                biomass = 150.0 * ndvi_val
                rec = {
                    "date": d_obj,
                    "date_str": d_str,
                    "block_id": blk_id,
                    "ndvi": ndvi_val,
                    "biomass": biomass,
                }
                ndvi_data.append(rec)
                try:
                    insert_ndvi_record_to_db(rec)
                except Exception as e:
                    print("Failed to save NDVI to DB:", e)

    records = sorted(ndvi_data, key=lambda x: x["date"])
    ndvi_by_date = defaultdict(list)
    for r in records:
        ndvi_by_date[r["date_str"]].append(r["ndvi"])

    chart_dates = sorted(ndvi_by_date.keys())
    chart_ndvi = [
        round(sum(ndvi_by_date[d]) / len(ndvi_by_date[d]), 3)
        for d in chart_dates
    ]

    return render_template(
        "ndvi.html",
        today=today,
        block_names=BLOCK_NAMES,
        num_blocks=NUM_BLOCKS,
        records=records,
        chart_dates=chart_dates,
        chart_ndvi=chart_ndvi,
    )

# --------- PEST & DISEASE PAGE ---------

@app.route("/pests", methods=["GET", "POST"])
def pests_page():
    today = date.today()

    if request.method == "POST":
        action = request.form.get("action", "")

        # -------------------------
        # ADD NEW PEST RECORD
        # -------------------------
        if action == "add_pest":
            d_str = request.form.get("date", "").strip()
            blk_id_str = request.form.get("block_id", "").strip()
            pest = request.form.get("pest", "").strip()
            severity = request.form.get("severity", "").strip()
            area_str = request.form.get("area", "").strip()
            action_txt = request.form.get("action_text", "").strip()

            if d_str and blk_id_str and pest:
                try:
                    d_obj = datetime.strptime(d_str, "%Y-%m-%d").date()
                    blk_id = int(blk_id_str)
                except ValueError:
                    blk_id = None

                area_val = safe_float(area_str)

                if blk_id and 1 <= blk_id <= NUM_BLOCKS:
                    rec = {
                        "date": d_obj,
                        "date_str": d_str,
                        "block_id": blk_id,
                        "pest": pest,
                        "severity": severity or "Low",
                        "area": area_val,
                        "action": action_txt,
                    }
                    # Update in-memory
                    pests_data.append(rec)

                    # Save to DB
                    try:
                        insert_pest_record_to_db(rec)
                    except Exception as e:
                        print("Failed to save pest record to DB:", e)

        # -------------------------
        # EDIT / DELETE EXISTING PEST RECORDS
        # -------------------------
        elif action == "edit_pests":
            new_rows = []
            try:
                row_count = int(request.form.get("row_count", "0"))
            except ValueError:
                row_count = 0

            for i in range(row_count):
                d_str = request.form.get(f"date_{i}", "").strip()
                blk_id_str = request.form.get(f"block_id_{i}", "").strip()
                pest = request.form.get(f"pest_{i}", "").strip()
                severity = request.form.get(f"severity_{i}", "").strip()
                area_str = request.form.get(f"area_{i}", "").strip()
                action_txt = request.form.get(f"action_{i}", "").strip()
                delete_flag = request.form.get(f"delete_{i}")

                # Skip empty or deleted rows
                if not d_str or not blk_id_str or not pest or delete_flag == "on":
                    continue

                try:
                    d_obj = datetime.strptime(d_str, "%Y-%m-%d").date()
                    blk_id = int(blk_id_str)
                except ValueError:
                    continue

                if not (1 <= blk_id <= NUM_BLOCKS):
                    continue

                area_val = safe_float(area_str)

                new_rows.append(
                    {
                        "date": d_obj,
                        "date_str": d_str,
                        "block_id": blk_id,
                        "pest": pest,
                        "severity": severity,
                        "area": area_val,
                        "action": action_txt,
                    }
                )

            # Replace in-memory data
            pests_data.clear()
            pests_data.extend(new_rows)

            # Replace DB table content
            try:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("DELETE FROM pests_records")
                for r in new_rows:
                    cur.execute(
                        """
                        INSERT INTO pests_records (date, block_id, pest, severity, area, action)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        """,
                        (r["date"], r["block_id"], r["pest"], r["severity"], r["area"], r["action"]),
                    )
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                print("Failed to rewrite pests_records table:", e)

    # Always show sorted records
    records = sorted(pests_data, key=lambda x: x["date"])

    return render_template(
        "pests.html",
        today=today,
        block_names=BLOCK_NAMES,
        num_blocks=NUM_BLOCKS,
        records=records,
    )

# ---------------------------------------------------
# NO-CACHE HEADERS
# ---------------------------------------------------

@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ---------------------------------------------------
# RUN APP
# ---------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True)
