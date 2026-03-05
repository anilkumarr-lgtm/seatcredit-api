from flask import Flask, request, jsonify
from flask_cors import CORS
import pymysql
import pandas as pd
import json
import io
from datetime import datetime

app = Flask(__name__)
CORS(app)

# ─── DB CONFIG ────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "reportreplica-mumbai.seatseller.in",
    "port":     3306,
    "user":     "readonlyuser",
    "password": "ssprodreadAccess",
    "database": "ssprod",
    "connect_timeout": 10
}

def get_conn():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)

# ─── CREDIT MAPPING QUERIES ───────────────────────────────────────────────────
CREDIT_QUERIES = {
    "Cp": "SELECT agentaccount FROM users_cp_agentmapper",
    "DB": "SELECT agentaccount FROM users_distributor_agentmapper",
    "Rupifi": "SELECT ACCOUNT as agentaccount FROM rupifi_onboarding WHERE currentstatus='ACTIVE'",
    "Aspire": "SELECT ACCOUNT as agentaccount FROM aspire_onboarding WHERE Applicationstatus='PROCESS_COMPLETED'"
}

# ─── AGENT DETAIL QUERY ───────────────────────────────────────────────────────
AGENT_QUERY = """
SELECT
    a.account,
    a.name AS agentname,
    a.organizationname,
    u1.name AS RoName,
    u2.name AS RmName,
    a.email,
    a.mobile,
    c.city,
    e.name AS cityname,
    c.state,
    k.name AS StateName,
    b.region,
    d.name AS AgentRegion,
    a.agenttype,
    a.status,
    DATE(a.creationtime) AS onboarded
FROM users_user a
LEFT JOIN users_user_subscriptions b ON a.account = b.account
LEFT JOIN sslocation_mylocation c ON a.mylocation = c.id
LEFT JOIN sslocation_region e ON c.city = e.id
LEFT JOIN sslocation_region k ON c.state = k.id
LEFT JOIN sslocation_region d ON b.region = d.id
LEFT JOIN users_user u1 ON b.accountmanager = u1.account
LEFT JOIN users_user u2 ON b.l2accountmanager = u2.account
WHERE a.account IN (
    SELECT a.account FROM users_user a
    LEFT JOIN users_user_subscriptions b ON a.account = b.account
    WHERE DATE(a.creationtime) >= '2013-04-01'
    AND b.subscription = 'retail_agent'
    AND a.account = a.masteruser
    AND a.status IN ('Active')
    AND a.testuser <> 1
    GROUP BY a.account
)
GROUP BY a.account
LIMIT 1000000
"""

# ─── SCORING ENGINE ───────────────────────────────────────────────────────────
# Tightened: min GMV ₹10,000 | min active months 9
MIN_GMV       = 10000
MIN_MONTHS    = 9
CREDIT_LIMIT_PCT = 0.50   # 50% of avg monthly GMV

def compute_score(avg_gmv, active_months, gmv_growth, tenure_months):
    # GMV Score (0-40)
    if   avg_gmv >= 200000: gmv_score = 40
    elif avg_gmv >= 100000: gmv_score = 35
    elif avg_gmv >= 50000:  gmv_score = 30
    elif avg_gmv >= 25000:  gmv_score = 24
    elif avg_gmv >= 15000:  gmv_score = 18
    elif avg_gmv >= 10000:  gmv_score = 12
    else:                   gmv_score = 0   # below threshold → 0

    # Growth Score (0-20)
    if   gmv_growth >= 50:  growth_score = 20
    elif gmv_growth >= 20:  growth_score = 16
    elif gmv_growth >= 5:   growth_score = 12
    elif gmv_growth >= -5:  growth_score = 8
    elif gmv_growth >= -20: growth_score = 4
    else:                   growth_score = 0

    # Consistency Score (0-20) — out of 23 months
    consistency_pct = (active_months / 23) * 100
    consistency_score = round((consistency_pct / 100) * 20)

    # Tenure Score (0-20)
    if   tenure_months >= 48: tenure_score = 20
    elif tenure_months >= 36: tenure_score = 16
    elif tenure_months >= 24: tenure_score = 12
    elif tenure_months >= 12: tenure_score = 8
    elif tenure_months >= 6:  tenure_score = 4
    else:                     tenure_score = 1

    score = min(100, gmv_score + growth_score + consistency_score + tenure_score)

    # Hard eligibility gates (tightened)
    eligible = (
        score >= 38 and
        avg_gmv >= MIN_GMV and
        active_months >= MIN_MONTHS
    )

    recommended_limit = round(avg_gmv * CREDIT_LIMIT_PCT) if eligible else 0

    return {
        "score": score,
        "eligible": eligible,
        "recommended_limit": recommended_limit,
        "consistency_pct": round(consistency_pct, 1),
        "breakdown": {
            "gmv_score":         {"score": gmv_score,         "max": 40},
            "growth_score":      {"score": growth_score,       "max": 20},
            "consistency_score": {"score": consistency_score,  "max": 20},
            "tenure_score":      {"score": tenure_score,       "max": 20},
        }
    }

def get_tier(score, eligible):
    if not eligible:    return "INELIGIBLE"
    if score >= 75:     return "GOLD"
    if score >= 55:     return "SILVER"
    return                     "BRONZE"

def calc_gmv_metrics(monthly_gmv: dict):
    if not monthly_gmv:
        return 0, 0, 0
    values = list(monthly_gmv.values())
    avg_gmv = sum(values) / len(values)
    active_months = len(values)
    # Growth: first 3 vs last 3
    sorted_keys = sorted(monthly_gmv.keys())
    first3 = [monthly_gmv[k] for k in sorted_keys[:3]]
    last3  = [monthly_gmv[k] for k in sorted_keys[-3:]]
    avg_first = sum(first3) / len(first3) if first3 else 0
    avg_last  = sum(last3)  / len(last3)  if last3  else 0
    gmv_growth = ((avg_last - avg_first) / avg_first * 100) if avg_first > 0 else 0
    return round(avg_gmv, 2), active_months, round(gmv_growth, 1)

# ─── PARSE EXCEL ─────────────────────────────────────────────────────────────
def parse_gmv_excel(file_bytes):
    """
    Parses the pivot-style Excel:
    Row 1: year headers (2024, None, None... 2025, None... 2026, None)
    Row 2: Account, Mar, Apr, May... Jan, Feb, Apr... Jan, Feb
    Returns: dict { account_str -> { "YYYY-Mon": value, ... } }
    """
    df_raw = pd.read_excel(io.BytesIO(file_bytes), header=None)

    year_row  = df_raw.iloc[0].tolist()
    month_row = df_raw.iloc[1].tolist()

    # Build month labels
    month_labels = []
    current_year = None
    for i, (yr, mo) in enumerate(zip(year_row, month_row)):
        if i == 0:  # Account column
            month_labels.append(None)
            continue
        if yr is not None and str(yr).strip() not in ('', 'nan'):
            try: current_year = int(float(str(yr)))
            except: pass
        if mo and str(mo).strip() not in ('', 'nan') and current_year:
            month_labels.append(f"{current_year}-{str(mo).strip()[:3]}")
        else:
            month_labels.append(None)

    gmv_data = {}
    for _, row in df_raw.iloc[2:].iterrows():
        account = row.iloc[0]
        if pd.isna(account):
            continue
        account = str(int(float(str(account))))
        monthly = {}
        for i, label in enumerate(month_labels):
            if label is None or i >= len(row):
                continue
            val = row.iloc[i]
            if pd.notna(val) and isinstance(val, (int, float)) and val > 0:
                monthly[label] = round(float(val), 2)
        gmv_data[account] = monthly

    return gmv_data

# ─── IN-MEMORY STORE ─────────────────────────────────────────────────────────
_store = {
    "agents":       {},   # account -> agent info from DB
    "credit_map":   {},   # account -> credit type string
    "gmv_data":     {},   # account -> monthly gmv dict
    "last_db_sync": None,
    "last_gmv_upload": None,
}

def sync_db():
    """Fetch agents + credit mapping from MySQL"""
    conn = get_conn()
    try:
        # 1. Credit mapping
        credit_map = {}
        with conn.cursor() as cur:
            for credit_type, query in CREDIT_QUERIES.items():
                try:
                    cur.execute(query)
                    rows = cur.fetchall()
                    for row in rows:
                        acc = str(row.get("agentaccount", "")).strip()
                        if acc:
                            # If already mapped, concatenate
                            if acc in credit_map:
                                credit_map[acc] = credit_map[acc] + "+" + credit_type
                            else:
                                credit_map[acc] = credit_type
                except Exception as e:
                    print(f"Credit query error ({credit_type}): {e}")

        # 2. Agent details
        agents = {}
        with conn.cursor() as cur:
            cur.execute(AGENT_QUERY)
            rows = cur.fetchall()
            for row in rows:
                acc = str(row["account"])
                ob = row.get("onboarded")
                tenure = 0
                if ob:
                    try:
                        ob_date = ob if isinstance(ob, datetime) else datetime.strptime(str(ob), "%Y-%m-%d")
                        tenure = max(0, int((datetime(2026, 3, 1) - ob_date).days / 30))
                    except:
                        pass
                agents[acc] = {
                    "account":    acc,
                    "name":       row.get("agentname") or "",
                    "org":        row.get("organizationname") or "",
                    "city":       row.get("cityname") or "",
                    "state":      row.get("StateName") or "",
                    "region":     row.get("AgentRegion") or "",
                    "agent_type": row.get("agenttype") or "",
                    "ro":         row.get("RoName") or "",
                    "rm":         row.get("RmName") or "",
                    "email":      row.get("email") or "",
                    "mobile":     row.get("mobile") or "",
                    "onboarded":  str(ob)[:10] if ob else "",
                    "tenure_months": tenure,
                    "credit_mapped": credit_map.get(acc, "Not Mapped"),
                }

        _store["agents"]       = agents
        _store["credit_map"]   = credit_map
        _store["last_db_sync"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return True, f"Synced {len(agents)} agents, {len(credit_map)} credit mappings"
    finally:
        conn.close()

def build_scored_agent(acc):
    """Merge agent info + GMV + score"""
    agent = _store["agents"].get(acc)
    if not agent:
        return None
    gmv   = _store["gmv_data"].get(acc, {})
    avg_gmv, active_months, gmv_growth = calc_gmv_metrics(gmv)
    tenure = agent["tenure_months"]
    s      = compute_score(avg_gmv, active_months, gmv_growth, tenure)
    tier   = get_tier(s["score"], s["eligible"])

    # Sparkline: last 6 months
    sorted_keys = sorted(gmv.keys())
    sparkline   = [gmv[k] for k in sorted_keys[-6:]]

    return {
        **agent,
        "avg_gmv":            avg_gmv,
        "active_months":      active_months,
        "gmv_growth":         gmv_growth,
        "monthly_gmv":        gmv,
        "sparkline":          sparkline,
        "score":              s["score"],
        "eligible":           s["eligible"],
        "tier":               tier,
        "recommended_limit":  s["recommended_limit"],
        "consistency_pct":    s["consistency_pct"],
        "breakdown":          s["breakdown"],
    }

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":           "ok",
        "agents_loaded":    len(_store["agents"]),
        "gmv_loaded":       len(_store["gmv_data"]),
        "last_db_sync":     _store["last_db_sync"],
        "last_gmv_upload":  _store["last_gmv_upload"],
    })

@app.route("/sync", methods=["POST"])
def sync():
    """Trigger a fresh DB sync"""
    try:
        ok, msg = sync_db()
        return jsonify({"success": ok, "message": msg})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/upload-gmv", methods=["POST"])
def upload_gmv():
    """Upload Excel file to update GMV data"""
    if "file" not in request.files:
        return jsonify({"success": False, "message": "No file uploaded"}), 400
    f = request.files["file"]
    try:
        gmv_data = parse_gmv_excel(f.read())
        _store["gmv_data"]        = gmv_data
        _store["last_gmv_upload"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return jsonify({"success": True, "message": f"Loaded GMV for {len(gmv_data)} agents"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/agent/<account_id>", methods=["GET"])
def get_agent(account_id):
    """Get single agent with full score"""
    if not _store["agents"]:
        return jsonify({"error": "DB not synced yet. Call /sync first."}), 503
    result = build_scored_agent(str(account_id))
    if not result:
        return jsonify({"error": f"Agent {account_id} not found"}), 404
    return jsonify(result)

@app.route("/city/<city_name>", methods=["GET"])
def get_city(city_name):
    """Get all agents in a city with scores"""
    if not _store["agents"]:
        return jsonify({"error": "DB not synced yet. Call /sync first."}), 503

    city_lower = city_name.lower().strip()
    matched = [
        acc for acc, a in _store["agents"].items()
        if a.get("city", "").lower().strip() == city_lower
    ]

    scored = []
    for acc in matched:
        r = build_scored_agent(acc)
        if r:
            scored.append(r)

    scored.sort(key=lambda x: -x["score"])

    # Summary stats
    eligible    = [a for a in scored if a["eligible"]]
    mapped      = [a for a in scored if a["credit_mapped"] != "Not Mapped"]
    total_limit = sum(a["recommended_limit"] for a in eligible)
    avg_gmv     = sum(a["avg_gmv"] for a in scored) / len(scored) if scored else 0

    tier_counts = {"GOLD": 0, "SILVER": 0, "BRONZE": 0, "INELIGIBLE": 0}
    cm_counts   = {}
    for a in scored:
        tier_counts[a["tier"]] = tier_counts.get(a["tier"], 0) + 1
        cm = a["credit_mapped"]
        cm_counts[cm] = cm_counts.get(cm, 0) + 1

    return jsonify({
        "city":         city_name,
        "total":        len(scored),
        "eligible":     len(eligible),
        "mapped":       len(mapped),
        "avg_gmv":      round(avg_gmv, 2),
        "total_recommended_limit": total_limit,
        "tier_counts":  tier_counts,
        "cm_counts":    cm_counts,
        "agents":       scored,
    })

@app.route("/cities", methods=["GET"])
def list_cities():
    """Return all cities with agent counts"""
    from collections import Counter
    cities = Counter(
        a["city"] for a in _store["agents"].values() if a.get("city")
    )
    return jsonify([
        {"city": city, "count": count}
        for city, count in sorted(cities.items(), key=lambda x: -x[1])
        if city
    ])

@app.route("/search", methods=["GET"])
def search():
    """Search agents by account id, name, or city"""
    q = request.args.get("q", "").strip().lower()
    if not q or len(q) < 3:
        return jsonify([])
    results = []
    for acc, a in _store["agents"].items():
        if (q in acc.lower() or
            q in a.get("name","").lower() or
            q in a.get("city","").lower()):
            results.append({"account": acc, "name": a.get("name",""), "city": a.get("city","")})
        if len(results) >= 20:
            break
    return jsonify(results)

# ─── STARTUP ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Starting SeatCredit API...")
    print("Syncing DB on startup...")
    try:
        ok, msg = sync_db()
        print(f"DB sync: {msg}")
    except Exception as e:
        print(f"DB sync failed (will retry on /sync): {e}")
    app.run(host="0.0.0.0", port=5000, debug=False)
