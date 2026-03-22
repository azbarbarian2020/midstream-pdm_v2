"""
Midstream PDM Demo - Synthetic Data Generator
Generates stations, assets, telemetry, maintenance, parts, technicians, and manuals.
Loads all data into Snowflake tables created by setup.sql.

Usage:
  SNOWFLAKE_CONNECTION_NAME=myconn python seed_data.py
  # Or set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PAT in .env
"""
import os
import sys
import json
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError:
    print("Install: pip install snowflake-connector-python[pandas]")
    sys.exit(1)


random.seed(42)
np.random.seed(42)

DATABASE = "PDM_DEMO"
WAREHOUSE = "PDM_DEMO_WH"

NOW = datetime(2026, 3, 20, 12, 0, 0)
TELEMETRY_DAYS = 180
HISTORICAL_INTERVAL_MINUTES = 15
DEMO_INTERVAL_MINUTES = 5
DEMO_WINDOW_DAYS = 7

# ---------------------------------------------------------------------------
# 1) Stations - Real Permian Basin locations
# ---------------------------------------------------------------------------
STATIONS = [
    {"station_id": 1, "name": "Midland Junction", "lat": 31.9973, "lon": -102.0779, "region": "Permian Basin", "station_type": "Gathering"},
    {"station_id": 2, "name": "Odessa Compressor Hub", "lat": 31.8457, "lon": -102.3676, "region": "Permian Basin", "station_type": "Compression"},
    {"station_id": 3, "name": "Pecos River Station", "lat": 31.4229, "lon": -103.4932, "region": "Permian Basin", "station_type": "Pumping"},
    {"station_id": 4, "name": "Monahans Transfer", "lat": 31.5943, "lon": -102.8927, "region": "Permian Basin", "station_type": "Gathering"},
    {"station_id": 5, "name": "Big Spring Terminal", "lat": 32.2504, "lon": -101.4787, "region": "Permian Basin", "station_type": "Terminal"},
    {"station_id": 6, "name": "Andrews Booster", "lat": 32.3185, "lon": -102.5454, "region": "Permian Basin", "station_type": "Compression"},
    {"station_id": 7, "name": "Crane Valley Pump", "lat": 31.3972, "lon": -102.3502, "region": "Permian Basin", "station_type": "Pumping"},
    {"station_id": 8, "name": "Kermit Junction", "lat": 31.8582, "lon": -103.0921, "region": "Permian Basin", "station_type": "Gathering"},
    {"station_id": 9, "name": "Wink Compressor", "lat": 31.7502, "lon": -103.1598, "region": "Permian Basin", "station_type": "Compression"},
    {"station_id": 10, "name": "Stanton Hub", "lat": 32.1293, "lon": -101.7885, "region": "Permian Basin", "station_type": "Terminal"},
]

# ---------------------------------------------------------------------------
# 2) Assets - 50 total, mixed pumps and compressors
# ---------------------------------------------------------------------------
PUMP_MODELS = [
    ("Flowserve HPRT", "Flowserve"),
    ("Sulzer MSD", "Sulzer"),
    ("Sundyne LMV-311", "Sundyne"),
    ("Grundfos CRN", "Grundfos"),
]
COMPRESSOR_MODELS = [
    ("Ariel JGK/4", "Ariel"),
    ("Dresser-Rand DATUM", "Dresser-Rand"),
    ("Atlas Copco GA-90", "Atlas Copco"),
    ("Ingersoll Rand Centac", "Ingersoll Rand"),
]

FAILURE_MODES_PUMP = ["NORMAL", "BEARING_WEAR", "VALVE_FAILURE", "SEAL_LEAK", "OVERHEATING"]
FAILURE_MODES_COMPRESSOR = ["NORMAL", "BEARING_WEAR", "VALVE_FAILURE", "SURGE", "OVERHEATING"]

ASSETS_PER_STATION = [5, 5, 5, 5, 5, 5, 5, 5, 5, 5]

def generate_assets():
    assets = []
    asset_id = 1
    for st in STATIONS:
        n = ASSETS_PER_STATION[st["station_id"] - 1]
        for j in range(n):
            is_pump = j % 2 == 0
            if is_pump:
                model, mfg = random.choice(PUMP_MODELS)
                atype = "PUMP"
                attrs = {"max_flow_gpm": random.randint(500, 5000), "stages": random.randint(1, 5)}
            else:
                model, mfg = random.choice(COMPRESSOR_MODELS)
                atype = "COMPRESSOR"
                attrs = {"max_pressure_psi": random.randint(500, 3000), "stages": random.randint(1, 4)}
            install_years_ago = random.randint(1, 15)
            assets.append({
                "asset_id": asset_id,
                "station_id": st["station_id"],
                "asset_type": atype,
                "model_name": model,
                "manufacturer": mfg,
                "install_date": (NOW - timedelta(days=install_years_ago * 365)).strftime("%Y-%m-%d"),
                "rated_capacity": round(random.uniform(100, 2000), 1),
                "attributes": json.dumps(attrs),
            })
            asset_id += 1
    return assets

# ---------------------------------------------------------------------------
# 3) Degradation patterns for ~10 assets
# ---------------------------------------------------------------------------
DEGRADING_ASSETS = {
    5:  {"mode": "BEARING_WEAR",   "start_offset_days": 30, "severity": 0.85, "target_rul_now": 12},  # PUMP
    12: {"mode": "VALVE_FAILURE",  "start_offset_days": 25, "severity": 0.80, "target_rul_now": 20},  # COMPRESSOR
    18: {"mode": "SEAL_LEAK",      "start_offset_days": 20, "severity": 0.90, "target_rul_now": 14},  # PUMP
    22: {"mode": "OVERHEATING",    "start_offset_days": 25, "severity": 0.75, "target_rul_now": 18},  # COMPRESSOR
    27: {"mode": "BEARING_WEAR",   "start_offset_days": 15, "severity": 0.95, "target_rul_now": 0},   # COMPRESSOR — goes OFFLINE by end of demo
    34: {"mode": "SURGE",          "start_offset_days": 21, "severity": 1.0,  "target_rul_now": 7},   # COMPRESSOR — hero asset
    35: {"mode": "SEAL_LEAK",      "start_offset_days": 15, "severity": 0.85, "target_rul_now": 22},  # PUMP
    39: {"mode": "OVERHEATING",    "start_offset_days": 18, "severity": 0.85, "target_rul_now": 25},  # COMPRESSOR
    41: {"mode": "BEARING_WEAR",   "start_offset_days": 7,  "severity": 0.45, "target_rul_now": 15},  # PUMP — transitions green→yellow during demo window
    48: {"mode": "SEAL_LEAK",      "start_offset_days": 22, "severity": 0.75, "target_rul_now": 16},  # PUMP
}

def generate_telemetry(assets):
    """Generate telemetry: 15-min intervals for historical, 5-min for last 7 days (demo window)."""
    historical_days = TELEMETRY_DAYS - DEMO_WINDOW_DAYS
    hist_start = NOW - timedelta(days=TELEMETRY_DAYS)
    demo_start = NOW - timedelta(days=DEMO_WINDOW_DAYS)

    n_hist = historical_days * 24 * 60 // HISTORICAL_INTERVAL_MINUTES
    n_demo = DEMO_WINDOW_DAYS * 24 * 60 // DEMO_INTERVAL_MINUTES
    n_points = n_hist + n_demo

    hist_timestamps = [hist_start + timedelta(minutes=i * HISTORICAL_INTERVAL_MINUTES) for i in range(n_hist)]
    demo_timestamps = [demo_start + timedelta(minutes=i * DEMO_INTERVAL_MINUTES) for i in range(n_demo)]
    timestamps = hist_timestamps + demo_timestamps

    all_rows = []
    for asset in assets:
        aid = asset["asset_id"]
        atype = asset["asset_type"]
        is_pump = atype == "PUMP"

        base_vib = np.random.uniform(2, 5)
        base_temp = np.random.uniform(160, 200)
        base_pressure = np.random.uniform(200, 800)
        base_flow = np.random.uniform(300, 2000)
        base_rpm = np.random.uniform(1500, 3600)
        base_power = np.random.uniform(50, 500)

        vib = base_vib + np.random.normal(0, 0.3, n_points)
        temp = base_temp + np.random.normal(0, 3, n_points)
        pressure = base_pressure + np.random.normal(0, 10, n_points)
        flow = base_flow + np.random.normal(0, 20, n_points)
        rpm = base_rpm + np.random.normal(0, 30, n_points)
        power = base_power + np.random.normal(0, 5, n_points)

        diff_p = np.full(n_points, np.nan)
        suction_p = np.full(n_points, np.nan)
        seal_temp_arr = np.full(n_points, np.nan)
        cavitation = np.full(n_points, np.nan)
        discharge_t = np.full(n_points, np.nan)
        inlet_t = np.full(n_points, np.nan)
        comp_ratio = np.full(n_points, np.nan)
        oil_p = np.full(n_points, np.nan)

        if is_pump:
            diff_p = np.random.uniform(30, 100, n_points) + np.random.normal(0, 2, n_points)
            suction_p = np.random.uniform(50, 200, n_points) + np.random.normal(0, 3, n_points)
            seal_temp_arr = np.random.uniform(120, 180, n_points) + np.random.normal(0, 2, n_points)
            cavitation = np.random.uniform(0.01, 0.15, n_points) + np.random.normal(0, 0.01, n_points)
        else:
            discharge_t = np.random.uniform(200, 350, n_points) + np.random.normal(0, 5, n_points)
            inlet_t = np.random.uniform(60, 120, n_points) + np.random.normal(0, 2, n_points)
            comp_ratio = np.random.uniform(1.5, 4.0, n_points) + np.random.normal(0, 0.05, n_points)
            oil_p = np.random.uniform(40, 80, n_points) + np.random.normal(0, 1, n_points)

        if aid in DEGRADING_ASSETS:
            deg = DEGRADING_ASSETS[aid]
            mode = deg["mode"]
            days_before_end = deg["start_offset_days"]
            severity = deg["severity"]

            degrade_start_ts = NOW - timedelta(days=days_before_end)
            degrade_start_idx = 0
            for idx_t, t in enumerate(timestamps):
                if t >= degrade_start_ts:
                    degrade_start_idx = idx_t
                    break
            n_degrade = n_points - degrade_start_idx
            ramp = np.linspace(0, severity, n_degrade)

            if mode == "BEARING_WEAR":
                vib[degrade_start_idx:] += ramp * 20
                temp[degrade_start_idx:] += ramp * 50
                power[degrade_start_idx:] += ramp * 40
                rpm[degrade_start_idx:] += ramp * np.random.normal(0, 80, n_degrade)
                vib[degrade_start_idx:] += np.random.normal(0, ramp * 2, n_degrade)
                if not is_pump:
                    oil_p[degrade_start_idx:] -= ramp * 25
            elif mode == "VALVE_FAILURE":
                pressure[degrade_start_idx:] += np.sin(np.linspace(0, 30 * np.pi, n_degrade)) * ramp * 120
                flow[degrade_start_idx:] -= ramp * 350
                vib[degrade_start_idx:] += ramp * 3
                if is_pump:
                    diff_p[degrade_start_idx:] += ramp * 60
                else:
                    discharge_t[degrade_start_idx:] += ramp * 50
                    comp_ratio[degrade_start_idx:] += ramp * 0.8
            elif mode == "SEAL_LEAK":
                pressure[degrade_start_idx:] -= ramp * 100
                flow[degrade_start_idx:] -= ramp * 250
                if is_pump:
                    seal_temp_arr[degrade_start_idx:] += ramp * 70
                    suction_p[degrade_start_idx:] -= ramp * 50
                    cavitation[degrade_start_idx:] += ramp * 0.35
                else:
                    vib[degrade_start_idx:] += ramp * 5
            elif mode == "SURGE":
                vib[degrade_start_idx:] += ramp * 15
                flow[degrade_start_idx:] += np.sin(np.linspace(0, 50 * np.pi, n_degrade)) * ramp * 500
                pressure[degrade_start_idx:] += np.sin(np.linspace(0, 50 * np.pi, n_degrade)) * ramp * 150
                if not is_pump:
                    comp_ratio[degrade_start_idx:] += ramp * 3.0 + np.sin(np.linspace(0, 40 * np.pi, n_degrade)) * ramp * 2.0
                    discharge_t[degrade_start_idx:] += ramp * 120
                    oil_p[degrade_start_idx:] -= ramp * 30
                    inlet_t[degrade_start_idx:] += ramp * 40
            elif mode == "OVERHEATING":
                temp[degrade_start_idx:] += ramp * 100
                power[degrade_start_idx:] += ramp * 60
                if is_pump:
                    seal_temp_arr[degrade_start_idx:] += ramp * 80
                else:
                    discharge_t[degrade_start_idx:] += ramp * 80
                    oil_p[degrade_start_idx:] -= ramp * 20

        for i in range(n_points):
            row = {
                "ASSET_ID": aid,
                "TS": timestamps[i].strftime("%Y-%m-%d %H:%M:%S"),
                "VIBRATION": round(float(vib[i]), 2),
                "TEMPERATURE": round(float(temp[i]), 2),
                "PRESSURE": round(float(pressure[i]), 2),
                "FLOW_RATE": round(float(flow[i]), 2),
                "RPM": round(float(rpm[i]), 2),
                "POWER_DRAW": round(float(power[i]), 2),
            }
            row["DIFFERENTIAL_PRESSURE"] = None if np.isnan(diff_p[i]) else round(float(diff_p[i]), 2)
            row["SUCTION_PRESSURE"] = None if np.isnan(suction_p[i]) else round(float(suction_p[i]), 2)
            row["SEAL_TEMPERATURE"] = None if np.isnan(seal_temp_arr[i]) else round(float(seal_temp_arr[i]), 2)
            row["CAVITATION_INDEX"] = None if np.isnan(cavitation[i]) else round(float(cavitation[i]), 4)
            row["DISCHARGE_TEMP"] = None if np.isnan(discharge_t[i]) else round(float(discharge_t[i]), 2)
            row["INLET_TEMP"] = None if np.isnan(inlet_t[i]) else round(float(inlet_t[i]), 2)
            row["COMPRESSION_RATIO"] = None if np.isnan(comp_ratio[i]) else round(float(comp_ratio[i]), 4)
            row["OIL_PRESSURE"] = None if np.isnan(oil_p[i]) else round(float(oil_p[i]), 2)
            all_rows.append(row)

    return all_rows

# ---------------------------------------------------------------------------
# 4) Maintenance logs
# ---------------------------------------------------------------------------
MAINT_TYPES = ["INSPECTION", "PREVENTIVE", "CORRECTIVE", "EMERGENCY", "OVERHAUL"]

def generate_maintenance(assets):
    logs = []
    for asset in assets:
        aid = asset["asset_id"]
        n_events = random.randint(5, 15)
        for _ in range(n_events):
            days_ago = random.randint(1, TELEMETRY_DAYS)
            ts = NOW - timedelta(days=days_ago, hours=random.randint(6, 18))
            mtype = random.choice(MAINT_TYPES)
            parts = []
            if mtype in ("CORRECTIVE", "EMERGENCY", "OVERHAUL"):
                n_parts = random.randint(1, 4)
                part_names = ["bearing assembly", "mechanical seal", "valve kit", "gasket set",
                              "lubricant", "impeller", "piston ring", "O-ring kit", "coupling",
                              "filter element", "pressure gauge", "thermocouple"]
                parts = [{"name": random.choice(part_names), "qty": random.randint(1, 3)} for _ in range(n_parts)]

            desc_templates = {
                "INSPECTION": f"Routine inspection of {asset['asset_type'].lower()} {aid}. All parameters within normal range.",
                "PREVENTIVE": f"Scheduled preventive maintenance on {asset['model_name']}. Lubrication and filter replacement completed.",
                "CORRECTIVE": f"Corrective repair on {asset['asset_type'].lower()} {aid}. Replaced worn components per vibration analysis.",
                "EMERGENCY": f"Emergency shutdown and repair of {asset['asset_type'].lower()} {aid}. Abnormal readings detected.",
                "OVERHAUL": f"Major overhaul of {asset['model_name']} at station {asset['station_id']}. Full disassembly and rebuild.",
            }
            logs.append({
                "ASSET_ID": aid,
                "TS": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "MAINTENANCE_TYPE": mtype,
                "DESCRIPTION": desc_templates[mtype],
                "TECHNICIAN_ID": f"TECH-{random.randint(1, 8):03d}",
                "PARTS_USED": json.dumps(parts),
                "DURATION_HRS": round(random.uniform(0.5, 24), 1),
                "COST": round(random.uniform(200, 50000), 2),
            })
    return logs

# ---------------------------------------------------------------------------
# 5) Parts inventory
# ---------------------------------------------------------------------------
def generate_parts():
    parts = []
    part_id = 1
    categories = {
        "bearing": ["Radial bearing", "Thrust bearing", "Journal bearing", "Ball bearing assembly"],
        "seal": ["Mechanical seal", "Lip seal", "O-ring kit", "Gasket set", "Packing rings"],
        "valve": ["Suction valve kit", "Discharge valve kit", "Check valve", "Relief valve", "Control valve"],
        "filter": ["Oil filter element", "Inlet air filter", "Strainer basket", "Coalescing filter"],
        "general": ["Lubricant - synthetic", "Coupling insert", "Impeller", "Piston ring set",
                     "Thermocouple probe", "Pressure gauge", "Vibration sensor"],
    }
    for cat, items in categories.items():
        for item in items:
            for atype in ["PUMP", "COMPRESSOR"]:
                parts.append({
                    "part_id": part_id,
                    "part_name": f"{item} ({atype[0]})",
                    "asset_type": atype,
                    "category": cat,
                    "unit_cost": round(random.uniform(50, 5000), 2),
                    "qty_on_hand": random.randint(2, 50),
                    "lead_time_days": random.choice([1, 3, 5, 7, 14, 21]),
                })
                part_id += 1
    return parts

# ---------------------------------------------------------------------------
# 6) Technicians
# ---------------------------------------------------------------------------
TECHNICIANS = [
    {"tech_id": "TECH-001", "name": "Carlos Mendez", "home_base_lat": 31.9973, "home_base_lon": -102.0779, "home_base_city": "Midland",
     "certifications": json.dumps(["pump", "compressor"]), "availability": "AVAILABLE", "years_experience": 14,
     "specialty_notes": "Lead tech for critical compressor failures. API 618 and API 610 certified. Specialist in bearing diagnostics and vibration analysis.",
     "bio": "14-year veteran of Permian Basin midstream operations. Carlos started as a pump mechanic at Pioneer Natural Resources before specializing in reciprocating and centrifugal compressor maintenance. He holds API 618 and API 610 certifications and has completed over 300 compressor overhauls. Known for his vibration analysis expertise, Carlos often mentors junior technicians on predictive maintenance techniques.",
     "photo_url": "/photos/techs/TECH-001.jpeg", "hourly_rate": 95},
    {"tech_id": "TECH-002", "name": "Sarah Johnson", "home_base_lat": 31.8457, "home_base_lon": -102.3676, "home_base_city": "Odessa",
     "certifications": json.dumps(["pump"]), "availability": "AVAILABLE", "years_experience": 8,
     "specialty_notes": "Centrifugal and positive displacement pump specialist. Seal replacement and alignment expert.",
     "bio": "Sarah joined the team after 5 years at Targa Resources where she specialized in pipeline booster pump maintenance. She is certified in laser shaft alignment and has extensive experience with mechanical seal replacements on API 610 pumps. Her efficiency on seal leak repairs is the highest on the team.",
     "photo_url": "/photos/techs/TECH-002.jpeg", "hourly_rate": 85},
    {"tech_id": "TECH-003", "name": "Mike Torres", "home_base_lat": 31.4229, "home_base_lon": -103.4932, "home_base_city": "Pecos",
     "certifications": json.dumps(["compressor"]), "availability": "AVAILABLE", "years_experience": 18,
     "specialty_notes": "Senior compressor tech. Expert in surge control systems, anti-surge valve calibration, and gas turbine-driven compressors.",
     "bio": "With 18 years in the field, Mike is the most experienced compressor technician on the team. He previously worked at DCP Midstream as a lead compressor mechanic. Mike specializes in surge diagnostics and anti-surge valve calibration for centrifugal compressors. He holds a Gas Turbine Maintenance certification and is the go-to for complex compressor overhauls.",
     "photo_url": "/photos/techs/TECH-003.jpeg", "hourly_rate": 100},
    {"tech_id": "TECH-004", "name": "Jessica Chen", "home_base_lat": 32.2504, "home_base_lon": -101.4787, "home_base_city": "Big Spring",
     "certifications": json.dumps(["pump", "compressor"]), "availability": "AVAILABLE", "years_experience": 11,
     "specialty_notes": "Dual-certified pump and compressor tech. Thermal imaging and overheating diagnostics specialist.",
     "bio": "Jessica holds dual certifications in pump and compressor maintenance, making her one of the most versatile technicians on the team. She earned her Mechanical Engineering degree from Texas Tech before entering field service. Her thermal imaging certification allows her to quickly diagnose overheating issues. Jessica has been instrumental in reducing unplanned downtime at the Big Spring area stations by 30%.",
     "photo_url": "/photos/techs/TECH-004.jpeg", "hourly_rate": 90},
    {"tech_id": "TECH-005", "name": "Robert Davis", "home_base_lat": 32.3185, "home_base_lon": -102.5454, "home_base_city": "Andrews",
     "certifications": json.dumps(["pump"]), "availability": "ON_CALL", "years_experience": 6,
     "specialty_notes": "Pump maintenance with focus on valve systems. Currently on-call rotation for Andrews and Midland areas.",
     "bio": "Robert is a reliable pump technician who covers the Andrews corridor. He previously worked in upstream operations maintaining artificial lift systems before transitioning to midstream. His valve system expertise makes him effective on valve failure repairs. Currently on the on-call rotation for evening and weekend emergencies.",
     "photo_url": "/photos/techs/TECH-005.jpeg", "hourly_rate": 80},
    {"tech_id": "TECH-006", "name": "Maria Garcia", "home_base_lat": 31.3972, "home_base_lon": -102.3502, "home_base_city": "Crane",
     "certifications": json.dumps(["compressor"]), "availability": "AVAILABLE", "years_experience": 10,
     "specialty_notes": "Compressor overhaul specialist. Experienced with reciprocating compressor valve and packing replacements.",
     "bio": "Maria has a decade of experience focused on reciprocating compressor maintenance in the southern Permian Basin. She started at ONEOK as a compressor operator before moving into field maintenance. Maria is known for her thorough approach to compressor valve and packing replacements, consistently achieving first-time fix rates above 95%. She covers the Crane and McCamey corridor stations.",
     "photo_url": "/photos/techs/TECH-006.jpeg", "hourly_rate": 88},
    {"tech_id": "TECH-007", "name": "James Wilson", "home_base_lat": 31.8582, "home_base_lon": -103.0921, "home_base_city": "Kermit",
     "certifications": json.dumps(["pump", "compressor"]), "availability": "AVAILABLE", "years_experience": 15,
     "specialty_notes": "Senior dual-certified tech. Bearing replacement specialist with expertise in both pump and compressor bearings.",
     "bio": "James is a 15-year veteran who covers the western Permian stations from his base in Kermit. He is dual-certified on pumps and compressors with a particular expertise in bearing diagnostics and replacement. James apprenticed under a master mechanic at Williams Companies and has since become the team's bearing specialist. His systematic approach to root cause analysis has prevented numerous repeat failures.",
     "photo_url": "/photos/techs/TECH-007.jpeg", "hourly_rate": 95},
    {"tech_id": "TECH-008", "name": "Lisa Thompson", "home_base_lat": 31.7502, "home_base_lon": -103.1598, "home_base_city": "Wink",
     "certifications": json.dumps(["pump"]), "availability": "ON_CALL", "years_experience": 4,
     "specialty_notes": "Junior pump technician. Strong on routine maintenance and seal inspections. On-call for Wink and Kermit areas.",
     "bio": "Lisa is the newest member of the team, having joined 4 years ago after completing her Industrial Maintenance certification at Odessa College. She handles routine pump inspections and maintenance across the Wink-Kermit area. While still building her experience base, Lisa has shown strong diagnostic instincts and is currently studying for her compressor certification. On-call rotation covers weekends.",
     "photo_url": "/photos/techs/TECH-008.jpeg", "hourly_rate": 75},
]

# ---------------------------------------------------------------------------
# 7) Manuals (realistic content for pumps and compressors)
# ---------------------------------------------------------------------------
def generate_manuals():
    docs = []
    doc_id = 1

    pump_sections = [
        ("troubleshooting", "Centrifugal Pump - High Vibration Diagnosis",
         "When vibration levels exceed 7.0 mm/s RMS on centrifugal pumps, immediately check: (1) Bearing condition - inspect for wear, pitting, or discoloration. Replace bearings if radial clearance exceeds manufacturer specifications. (2) Shaft alignment - verify coupling alignment using laser or dial indicators. Misalignment greater than 0.002 inches requires correction. (3) Impeller balance - check for erosion, buildup, or damage. An out-of-balance impeller creates 1x running speed vibration. (4) Foundation bolts - verify all anchor bolts are torqued to specification. Loose mounting creates structural resonance. CAUTION: Lock out/tag out all energy sources before inspection. Refer to API 610 Section 6.3 for vibration acceptance criteria."),
        ("troubleshooting", "Centrifugal Pump - Seal Leak Diagnosis",
         "Mechanical seal leakage on centrifugal pumps requires immediate attention. Acceptable leakage per API 682: less than 0.5 ml/hr for light hydrocarbons. Diagnosis steps: (1) Check seal flush pressure differential - should be 15-25 psi above seal chamber pressure. (2) Inspect seal faces for scoring, heat checking, or carbon deposits. (3) Verify seal spring pressure within specification (typically 30-40 psi contact pressure). (4) Check shaft runout at seal location - maximum 0.002 inches TIR. (5) Review process temperature history - thermal shock can cause face distortion. WARNING: Hydrocarbon seal leaks create fire/explosion hazard. Ensure LEL monitors are operational. Isolate and depressurize before seal replacement."),
        ("troubleshooting", "Pump Cavitation Diagnosis and Prevention",
         "Cavitation in centrifugal pumps causes characteristic crackling noise, pitting on impeller vanes, and performance degradation. Key indicators: (1) Cavitation index > 0.15 indicates active cavitation. (2) Suction pressure drops below NPSH required value. (3) Erratic discharge pressure fluctuations. Prevention: Increase suction head by raising tank level or lowering pump position. Reduce suction line losses - verify strainer is clean, minimize elbows. Reduce pump speed if VFD equipped. Lower fluid temperature to reduce vapor pressure. API 610 requires NPSH available exceed NPSH required by minimum 3 feet (1 meter). CRITICAL: Prolonged cavitation causes impeller erosion and seal failure within weeks."),
        ("maintenance_procedure", "Pump Bearing Replacement Procedure",
         "Bearing replacement on centrifugal pumps (API 610 compliant): PREPARATION: (1) Lock out/tag out all energy sources. (2) Drain process fluid and flush system. (3) Disconnect coupling. (4) Remove bearing housing cover bolts. REMOVAL: (5) Use bearing puller to remove old bearing - never use hammer directly on bearing. (6) Inspect shaft journal for scoring or wear. Maximum allowable shaft runout: 0.001 inches. (7) Clean bearing housing bore with solvent. INSTALLATION: (8) Heat new bearing uniformly to 200F (93C) in oil bath or induction heater. (9) Slide bearing onto shaft - ensure proper shoulder contact. (10) Verify bearing endplay within specification (typically 0.002-0.004 inches). (11) Pack bearing housing 1/3 full with specified grease. (12) Reassemble in reverse order. POST-INSTALLATION: Run pump at no load for 30 minutes. Monitor bearing temperature - should stabilize below 180F (82C). Verify vibration below 3.0 mm/s. Expected time: 4-6 hours for experienced technician."),
        ("maintenance_procedure", "Pump Mechanical Seal Replacement",
         "Mechanical seal replacement procedure for API 682 seals: PREPARATION: (1) LOTO all energy sources. (2) Depressurize and drain pump casing. (3) Disconnect seal flush piping. (4) Remove gland plate bolts. REMOVAL: (5) Slide gland plate off shaft carefully. (6) Remove rotating seal assembly - note orientation. (7) Inspect stationary seat for damage. (8) Clean seal chamber thoroughly. INSTALLATION: (9) Install new stationary seat - verify O-ring is properly seated. (10) Lubricate shaft sleeve with compatible lubricant. (11) Slide new rotating assembly onto shaft. (12) Set seal face gap per manufacturer specification (typically 0.125 inches). (13) Torque gland bolts evenly in star pattern. (14) Reconnect flush piping. POST-INSTALLATION: Pressurize seal flush system first. Start pump and verify zero visible leakage. Monitor seal face temperature - should not exceed 200F. Expected time: 2-4 hours."),
        ("safety", "Pump Hot Work Safety Procedures",
         "Hot work on or near centrifugal pumps in hydrocarbon service requires: (1) Obtain hot work permit from area supervisor. (2) Gas test the work area - LEL must be below 10% before commencing. Continuous monitoring required. (3) Ensure fire watch is posted with extinguisher for 30 minutes after hot work completion. (4) Remove all flammable materials within 35-foot radius. (5) Verify all process isolation valves are LOTO tagged. (6) If pump handled H2S service - additional H2S monitor required, readings must be below 10 ppm. (7) Welding on pressure-containing parts requires ASME-qualified welder and post-weld NDE per ASME B31.3."),
        ("safety", "Pump Lockout/Tagout Procedure",
         "LOTO procedure for centrifugal pump maintenance: (1) Notify control room of pump shutdown. (2) Shut down pump via normal procedure. (3) Close and lock suction isolation valve. (4) Close and lock discharge isolation valve. (5) Close and lock all auxiliary connections (flush, drain, vent). (6) Verify zero energy: open drain valve to confirm depressurized. (7) Apply personal lock and tag to motor starter disconnect. (8) Attempt start to verify LOTO is effective."),
        ("parts_list", "Centrifugal Pump Recommended Spare Parts",
         "API 610 recommended spare parts list for centrifugal pump: COMMISSIONING SPARES (keep on-site): 1x complete mechanical seal assembly, 2x sets of bearings (radial + thrust), 1x coupling insert, 2x sets of gaskets (all flanges), 1x set of O-rings. CAPITAL SPARES (warehouse): 1x complete rotating assembly (shaft + impeller), 1x bearing housing, 1x seal chamber, 1x set of wear rings."),
        ("maintenance_procedure", "Pump Alignment Procedure - Laser Method",
         "Precision alignment procedure for centrifugal pumps: PREPARATION: (1) Ensure foundation and baseplate are level (within 0.001 in/ft). (2) Clean coupling faces and shaft ends. (3) Verify thermal growth calculations for operating temperature offset. SOFT FOOT CHECK: (4) Loosen each foot individually - if gap exceeds 0.002 inches, shim to correct. LASER ALIGNMENT: (5) Mount laser heads on pump and driver shafts. (6) Measure at 0, 90, 180, 270 degree positions. (7) Target values: Offset < 0.002 inches, Angularity < 0.0005 in/inch."),
        ("troubleshooting", "Pump Low Flow / Head Diagnosis",
         "When pump delivers less flow or head than rated: (1) Check suction strainer differential pressure - plugged strainer reduces NPSH available. (2) Verify impeller diameter matches design - check for erosion reducing effective diameter. (3) Inspect wear rings - excessive clearance causes internal recirculation. Replace if clearance exceeds 2x design. (4) Check for air ingestion at suction - inspect piping joints, verify liquid level above vortex breaker."),
    ]

    compressor_sections = [
        ("troubleshooting", "Reciprocating Compressor - High Vibration Diagnosis",
         "Elevated vibration on reciprocating compressors requires systematic diagnosis. Normal vibration for recips: 10-15 mm/s RMS at 1x running speed. Causes of excessive vibration: (1) Loose crosshead pin or bearing - creates 2x RPM component. (2) Broken or leaking valves - irregular pressure pulses. Perform valve temperature survey to identify. (3) Piston rod runout - maximum 0.002 inches TIR per API 618. (4) Foundation degradation - check grout condition and anchor bolt torque."),
        ("troubleshooting", "Compressor Surge Detection and Prevention",
         "Surge is the most dangerous operating condition for centrifugal compressors. Indicators: (1) Rapid oscillation of discharge pressure (> 5% of design). (2) Reversal of flow through the compressor. (3) Violent vibration and axial thrust excursions. (4) Audible rumbling or banging noise. (5) Rapid temperature rise on discharge. IMMEDIATE ACTION: Open anti-surge valve (blowoff) to increase flow. Do not attempt to increase speed during surge. If surge persists > 10 seconds, trip the compressor. PREVENTION: Maintain operating point to the right of surge line with minimum 10% margin."),
        ("troubleshooting", "Compressor Valve Failure Diagnosis",
         "Compressor valve failures account for 40% of unplanned reciprocating compressor shutdowns. Diagnosis: (1) Compare discharge temperatures across cylinders - a failed valve shows 15-30F higher discharge temperature. (2) Loss of capacity - if suction valve fails, cylinder cannot fill completely. (3) Increased power consumption for same throughput. (4) Valve cover temperature survey - infrared gun comparison between valves on same cylinder."),
        ("maintenance_procedure", "Compressor Piston Ring Replacement",
         "Piston ring replacement procedure for reciprocating compressors: PREPARATION: (1) LOTO all energy sources including gas isolation. (2) Depressurize and purge cylinder with nitrogen. (3) Gas test before entry - verify O2 > 19.5% and LEL < 10%. REMOVAL: (4) Remove cylinder head bolts in reverse star pattern. (5) Lift head carefully - watch for gasket adhesion. INSTALLATION: (9) Clean piston ring grooves thoroughly. (10) Install new rings with gap positions staggered 120 degrees."),
        ("maintenance_procedure", "Compressor Oil System Maintenance",
         "Lubrication system maintenance for reciprocating compressors: DAILY CHECKS: (1) Verify oil level in frame sump. (2) Check oil pressure at pump discharge (typically 40-60 psi). (3) Monitor oil temperature (should be 140-180F operating). WEEKLY: (5) Sample oil for water content (< 0.1%) and particle count. QUARTERLY: (7) Send oil sample for laboratory analysis - viscosity, TAN, wear metals."),
        ("safety", "Compressor Hot Work and Gas Hazards",
         "Compressors in natural gas service present significant ignition and toxic gas hazards. BEFORE ANY WORK: (1) Verify gas isolation is confirmed by control room. (2) Depressurize through blowdown system - do not vent to atmosphere if H2S present. (3) Purge system with nitrogen to < 1% hydrocarbon concentration. (4) Continuous gas monitoring with calibrated 4-gas detector."),
        ("safety", "Compressor Emergency Shutdown Procedures",
         "Emergency shutdown (ESD) of reciprocating and centrifugal compressors: AUTOMATIC TRIPS (verify functionality monthly): (1) High discharge temperature. (2) High discharge pressure. (3) Low suction pressure. (4) Low oil pressure. (5) High vibration. (6) High bearing temperature. MANUAL ESD: (1) Press ESD button at compressor or control room. (2) Verify unit has stopped."),
        ("parts_list", "Reciprocating Compressor Recommended Spare Parts",
         "API 618 recommended spare parts for reciprocating gas compressor: COMMISSIONING SPARES: 2x complete valve assemblies per cylinder (suction and discharge), 1x set piston rings per cylinder, 1x set packing rings per cylinder, 2x sets rod packing, crosshead pin bushings, all gaskets."),
        ("maintenance_procedure", "Compressor Performance Testing",
         "Compressor performance testing per ASME PTC-9: PREPARATION: (1) Install calibrated test instruments: suction/discharge pressure transmitters (accuracy +/- 0.1%), temperature RTDs (accuracy +/- 0.5F), flow measurement orifice plate. (2) Operate at steady state for minimum 1 hour before data collection. TEST EXECUTION: (4) Collect 5 data sets at 10-minute intervals at each test point."),
        ("troubleshooting", "Compressor High Discharge Temperature",
         "High discharge temperature on compressors indicates inefficiency or cooling failure. Diagnosis: (1) Check intercooler/aftercooler performance - fouled tubes cause poor heat transfer. (2) Leaking valves cause re-compression, raising discharge temp. (3) Broken piston rings allow blow-by, reducing efficiency. Maximum allowable discharge temperature per API 618: typically 300F for natural gas."),
    ]

    model_specific_docs = [
        ("Grundfos CRN", "PUMP", "troubleshooting", "Grundfos CRN - Bearing Wear Diagnosis",
         "Grundfos CRN vertical multistage centrifugal pump bearing wear diagnosis: The CRN series uses permanently lubricated deep-groove ball bearings with a design life of 20,000 hours. Signs of bearing degradation: (1) Vibration increases above 4.0 mm/s at pump head — use Grundfos GO Remote app for continuous monitoring if available. (2) Unusual noise from motor/pump coupling area. (3) Shaft runout exceeding 0.05mm at coupling end. (4) Bearing temperature above 80C measured at housing. Specific to CRN: The cartridge seal design means you must remove the complete pump head assembly for bearing access. Order Grundfos kit 96525490 (bearing set + shaft seal). Do NOT attempt to re-grease these bearings — they are sealed for life. Replacement interval: every 16,000 operating hours or when vibration exceeds alarm setpoint. Use Grundfos Product Center for exact part cross-reference by serial number."),
        ("Grundfos CRN", "PUMP", "maintenance_procedure", "Grundfos CRN - Seal Replacement Procedure",
         "Grundfos CRN mechanical seal replacement (cartridge type HQQE): This procedure applies to CRN 10-17 through CRN 150-6 models. PREPARATION: (1) Disconnect power and LOTO per site procedures. (2) Close isolation valves and drain pump. (3) Note impeller stack orientation before disassembly. DISASSEMBLY: (4) Remove coupling guard and motor bolts. (5) Lift motor and top bearing bracket as assembly. (6) Slide shaft with impeller stack from chamber housing. (7) The cartridge seal is retained by a circlip on the shaft — remove circlip and slide seal assembly off. INSTALLATION: (8) Clean shaft and seal chamber bore. (9) Lubricate new seal O-rings with Grundfos-approved lubricant (silicon-free). (10) Slide new cartridge seal onto shaft until circlip groove is accessible. (11) Re-install circlip. (12) Reassemble impeller stack — CRITICAL: maintain original impeller count and orientation, stage washers must align. Grundfos seal kit part numbers: HQQE = 96511844, HQQV = 96511845. Estimated time: 2 hours."),
        ("Grundfos CRN", "PUMP", "parts_list", "Grundfos CRN - Model-Specific Spare Parts",
         "Grundfos CRN recommended spares by model size: CRN 10-17: Seal kit (96511844), Bearing set (96525490), Wear ring set (96455090), Shaft (96511722). CRN 32-12: Seal kit (96511847), Bearing set (96525493), Wear ring set (96455096), Shaft (96511728). CRN 64-4: Seal kit (96511850), Bearing set (96525496), Wear ring set (96455102). All CRN models: O-ring kit (96455115), Coupling insert (CR flex 96416602). Always reference Grundfos Product Center with pump serial number for exact cross-reference. Standard lead times: seals 2-3 weeks, bearings 1 week, shafts 4-6 weeks. Recommend stocking 2x seal kits and 1x bearing set per installed unit for critical service."),
        ("Flowserve HPRT", "PUMP", "troubleshooting", "Flowserve HPRT - Seal Leak Diagnosis",
         "Flowserve HPRT (High Pressure Ring-section Turbine) pump seal leak troubleshooting: The HPRT uses a between-bearings multistage design with API 682 Plan 53B seal system. Seal leak indicators: (1) Barrier fluid reservoir level dropping — check level transmitter calibration first. (2) Visible weepage at atmospheric side. (3) Seal support system pressure excursions. HPRT-SPECIFIC DIAGNOSIS: (4) Check differential pressure across seal faces — HPRT operates at pressures up to 6000 psi so seal chamber pressure must be verified against design. (5) The inter-stage bushing wear can redirect flow to seal chamber — check bushing clearances if seal leaks recur after replacement. (6) Thermal transients during startup/shutdown stress the stationary seat — verify warmup rate follows Flowserve procedure FIS-126. Seal replacement requires specialized Flowserve tooling (ring puller set FT-8820). Contact Flowserve QuickResponse Center for emergency seal supply: 1-800-FSG-PUMP."),
        ("Flowserve HPRT", "PUMP", "maintenance_procedure", "Flowserve HPRT - Bearing Inspection",
         "Flowserve HPRT bearing inspection and replacement: The HPRT uses tilting-pad journal bearings (radial) and Kingsbury-type thrust bearings. INSPECTION INTERVAL: Every 8,000 hours or annually, whichever comes first. PROCEDURE: (1) Remove bearing housing cover. (2) Inspect pad surfaces — babbitt should be smooth, no wiping or scoring. (3) Measure pad thickness at center and edges — variation > 0.001 inches indicates uneven loading. (4) Inspect thrust collar for scoring — 16 RMS or better required. (5) Check bearing clearance with feeler gauge: radial 0.0015-0.003 in/inch of journal diameter. HPRT-SPECIFIC: The high-speed rotor (up to 12,000 RPM) makes vibration monitoring critical. Install proximity probes per API 670. Alert at 25 microns, trip at 50 microns shaft displacement."),
        ("Sundyne LMV-311", "PUMP", "troubleshooting", "Sundyne LMV-311 - Cavitation Prevention",
         "Sundyne LMV-311 integrally geared centrifugal pump cavitation prevention: The LMV-311 operates at speeds up to 25,000 RPM via an integral speed increaser, making it particularly sensitive to NPSH conditions. LMV-311 SPECIFIC REQUIREMENTS: (1) NPSH available must exceed NPSH required by minimum 5 feet (higher than standard API 610 margin of 3 feet). (2) The inducer option (Sundyne P/N IND-311) reduces NPSH required by up to 50% — verify if installed. (3) Suction piping must be straight for minimum 10 pipe diameters before pump inlet — the high-speed impeller amplifies flow disturbances. (4) Suction strainer mesh must not exceed 40 mesh — finer mesh creates excessive pressure drop. MONITORING: Cavitation on LMV-311 shows as vibration increase at vane-pass frequency (RPM x number of vanes). Use accelerometer on pump casing near suction. Sundyne SmartGuard controller alarm setpoint: 0.5 g at vane-pass frequency."),
        ("Sundyne LMV-311", "PUMP", "maintenance_procedure", "Sundyne LMV-311 - Speed Increaser Service",
         "Sundyne LMV-311 integral speed increaser (gearbox) service procedure: The LMV-311 gearbox multiplies motor speed 4:1 to 10:1 depending on configuration. SERVICE INTERVAL: Oil change every 4,000 hours. Full inspection every 16,000 hours. OIL CHANGE: (1) Use only Sundyne-approved synthetic oil (Mobil SHC 626 or equivalent, ISO VG 68 PAO). (2) Drain volume approximately 1.5 gallons. (3) Fill to center of sight glass. FULL INSPECTION: (4) Remove gearbox cover. (5) Inspect gear teeth for pitting, scoring, or wear pattern. (6) Check pinion bearing condition — these are angular contact bearings. (7) Measure gear backlash: specification is 0.003-0.006 inches. CRITICAL: Do NOT use mineral oil — it will damage the nitrile seals. Sundyne warranty is voided if non-approved lubricant is used."),
        ("Sulzer MSD", "PUMP", "troubleshooting", "Sulzer MSD - Differential Pressure Diagnosis",
         "Sulzer MSD (Multi-Stage Diffuser) pump differential pressure troubleshooting: The MSD is a horizontal split-case multistage pump designed for high-pressure pipeline service. ABNORMAL DIFFERENTIAL PRESSURE INDICATORS: (1) Rising differential pressure at constant flow suggests downstream obstruction or closing valve. (2) Falling differential pressure suggests wear ring clearance increase, impeller erosion, or internal bypass leakage. SULZER-SPECIFIC: (3) The MSD crossover passages between stages can accumulate debris — if stage efficiency drops unevenly, inspect crossover passages. (4) Check inter-stage bushings for wear — Sulzer spec is maximum 0.020 inches diametral clearance. (5) Impeller wear rings on MSD are replaceable without removing rotor from case — access through suction/discharge nozzle. Use Sulzer BLUE diagnostic service for remote condition monitoring via embedded sensors. Part numbers for wear components reference Sulzer configuration code on pump nameplate."),
        ("Ariel JGK/4", "COMPRESSOR", "troubleshooting", "Ariel JGK/4 - Valve Failure Diagnosis",
         "Ariel JGK/4 four-throw reciprocating compressor valve failure troubleshooting: The JGK/4 uses Ariel self-acting plate valves with typical life of 8,000-16,000 hours in clean dry gas. VALVE FAILURE INDICATORS: (1) Discharge temperature differential between cylinders > 15F — use Ariel AutoValve temperature monitoring if installed. (2) Capacity drop — failed suction valve reduces volumetric efficiency 10-25%. (3) Power increase — failed discharge valve causes re-compression. ARIEL JGK/4 SPECIFIC: (4) The JGK/4 frame uses 4 throws with 90-degree offset — compare pulsation signatures between throws to isolate failed cylinder. (5) Valve types: suction uses 3-ring valve (P/N A-12345), discharge uses 4-ring valve (P/N A-12346). (6) Check valve lift measurement with Ariel feeler gauge set — maximum lift is 0.090 inches for standard and 0.060 inches for high-speed application. (7) Ariel recommends valve inspection at each piston ring change interval. Valve spare kits: Ariel P/N VK-JGK-SUC (suction) and VK-JGK-DIS (discharge)."),
        ("Ariel JGK/4", "COMPRESSOR", "maintenance_procedure", "Ariel JGK/4 - Oil System Maintenance",
         "Ariel JGK/4 frame and cylinder lubrication maintenance: FRAME OIL SYSTEM: The JGK/4 frame sump holds 55 gallons. Oil type: Ariel-approved mineral oil meeting Ariel specification EGS-1070 (ISO VG 150 for ambient > 40F, ISO VG 100 for < 40F). Change interval: 8,000 hours or annually. Daily checks: level (sight glass), pressure (45-65 psi at main gallery), temperature (140-180F normal). Oil filter: 10 micron nominal, change at 15 psi differential. CYLINDER LUBRICATION: Force-feed lubricator delivers Ariel-approved synthetic cylinder oil to each packing and cylinder point. Rate: per Ariel lube rate chart based on cylinder bore and gas composition. Check: count drops per minute at each divider block outlet — compare to Ariel specification. Low lube rate causes premature packing and ring wear. ARIEL JGK/4 SPECIFIC: This frame has 12 lube points (3 per throw). Monitor divider block with Ariel SmartLube electronic monitoring."),
        ("Ariel JGK/4", "COMPRESSOR", "parts_list", "Ariel JGK/4 - Model-Specific Spare Parts",
         "Ariel JGK/4 recommended spare parts: RUNNING SPARES (keep on-site): Valve assemblies — 4x suction (VK-JGK-SUC), 4x discharge (VK-JGK-DIS). Piston rings — 2x sets per cylinder (PEEK for dry gas, filled PTFE for wet gas). Packing rings — 2x sets per cylinder. Rod packing case — 1x complete assembly. Oil filter elements — 6x. Cylinder lube oil — 10 gallons. CAPITAL SPARES (regional warehouse): 1x piston + rod assembly, 1x crosshead assembly, 1x set main bearings (4 mains + 4 rods), 1x crankshaft. ARIEL JGK/4 SPECIFIC: Frame casting cracks are warranty items if detected within first 5 years — document and report to Ariel. Crosshead pin bushing (P/N CB-JGK) has 16,000 hour replacement interval."),
        ("Dresser-Rand DATUM", "COMPRESSOR", "troubleshooting", "Dresser-Rand DATUM - Surge Prevention Guide",
         "Dresser-Rand DATUM centrifugal compressor surge prevention and recovery: The DATUM line features integrally-geared multi-stage centrifugal compression up to 1,200 psi discharge. SURGE CHARACTERISTICS ON DATUM: (1) Compression ratio spike above 1.5x normal indicates approaching surge line. (2) Discharge temperature excursion > 20% above normal operating point. (3) Flow oscillation at frequencies of 1-10 Hz — distinct from mechanical vibration. (4) Oil pressure drop on thrust bearing due to rotor axial displacement. (5) Inlet temperature increase from flow reversal heating. DATUM-SPECIFIC PREVENTION: (6) The DATUM uses Dresser-Rand OptiRamp anti-surge control — verify controller tuning quarterly. Response time must be < 250ms from surge detection to valve opening. (7) Hot gas bypass valve must stroke fully open in < 1.5 seconds. Test monthly. (8) Minimum flow setpoint for DATUM: typically 65-70% of design flow — exact value on Dresser-Rand performance map (document DRI-DATUM-PM-xxx). (9) After surge event: inspect thrust bearing, labyrinth seals, and coupling. Do NOT restart until vibration monitoring confirms rotor integrity. CRITICAL: Three surge events within 1 hour requires full mechanical inspection before restart."),
        ("Dresser-Rand DATUM", "COMPRESSOR", "maintenance_procedure", "Dresser-Rand DATUM - Performance Optimization",
         "Dresser-Rand DATUM centrifugal compressor performance optimization and overhaul: PERFORMANCE MONITORING: (1) Log polytropic efficiency weekly — deviation > 3% from baseline indicates fouling or seal wear. (2) Monitor inter-stage temperatures for heat balance. (3) Track compression ratio vs. flow curve position relative to surge line. FOULING MITIGATION: (4) On-line water wash: inject demineralized water per Dresser-Rand procedure DRI-OLW-100. Frequency: monthly or when efficiency drops 2%. (5) Off-line cleaning: required when on-line wash no longer restores performance. DATUM OVERHAUL: (6) Major overhaul interval: 40,000 hours. Includes impeller refurbishment, seal replacement, bearing inspection. (7) The DATUM integrally-geared design requires specialized Dresser-Rand alignment tooling for gear mesh verification. (8) Impeller tip clearance specification: 0.020-0.030 inches (check Dresser-Rand data sheet for exact model). Contact Dresser-Rand Service at service@dresser-rand.com for overhaul planning."),
        ("Dresser-Rand DATUM", "COMPRESSOR", "parts_list", "Dresser-Rand DATUM - Model-Specific Spare Parts",
         "Dresser-Rand DATUM recommended spares: RUNNING SPARES: 1x complete labyrinth seal set (all stages), 1x set tilting-pad journal bearings, 1x set thrust bearings (active + inactive), 1x set dry gas seals (if equipped) or mechanical seals, 2x sets O-rings and gaskets. CAPITAL SPARES: 1x rotor assembly (all impellers + shaft), 1x set of diaphragms, 1x coupling assembly. DATUM-SPECIFIC: The integrally-geared design means bull gear and pinion bearings are different from single-shaft machines — order by DATUM serial number from Dresser-Rand Parts Center. Anti-surge valve actuator rebuild kit. OptiRamp controller spare module. Lead times: seals 3-4 weeks, rotor 12-16 weeks, diaphragms 8-10 weeks."),
        ("Atlas Copco GA-90", "COMPRESSOR", "troubleshooting", "Atlas Copco GA-90 - Overheating Diagnosis",
         "Atlas Copco GA-90 rotary screw compressor overheating troubleshooting: The GA-90 uses oil-injected screw elements with normal discharge air temperature of 70-90C above ambient. OVERHEATING INDICATORS: (1) Discharge air temperature > 110C triggers high temp alarm. (2) Oil temperature above 100C indicates cooling system problem. (3) Element temperature differential (discharge minus intake) > 100C suggests internal wear. GA-90 SPECIFIC DIAGNOSIS: (4) Check oil/air cooler — the GA-90 uses an integrated oil/air combined cooler. Fouled fins reduce cooling by 30-50%. Clean with compressed air from engine side. (5) Verify thermostat valve operation — the GA-90 thermostat opens at 72C. Stuck closed = oil bypasses cooler. Stuck open = overcooling at startup. (6) Oil level in separator tank — low oil means less cooling capacity. Minimum level: at red mark on sight glass. (7) Check minimum pressure valve — if stuck open, system pressure too low and oil carryover increases. Atlas Copco Elektronikon controller displays all temperature readings on main screen. Error code A0017 = high element discharge temperature."),
        ("Atlas Copco GA-90", "COMPRESSOR", "maintenance_procedure", "Atlas Copco GA-90 - Service Schedule",
         "Atlas Copco GA-90 scheduled maintenance: 2,000 HOURS: Oil filter change (Atlas Copco P/N 1621737800), air filter change (P/N 1621574200), oil separator element (P/N 1621574300), oil sample analysis. 4,000 HOURS: Complete oil change (Atlas Copco Roto-Xtend Duty Fluid, 28 liters), check all safety valves, inspect coupling, clean cooler. 8,000 HOURS: Screw element inspection — check bearing clearances, rotor tip clearance (specification 0.05-0.10mm). Replace inlet valve overhaul kit. 20,000 HOURS: Major overhaul — element bearing replacement, shaft seal replacement, motor bearing replacement. Use only Atlas Copco OEM parts for warranty coverage. GA-90 Elektronikon controller tracks service intervals automatically — access via Service > Maintenance plan menu. Reset service timer after each service. Contact Atlas Copco Service: use QR code on unit nameplate for direct connection."),
        ("Ingersoll Rand Centac", "COMPRESSOR", "troubleshooting", "Ingersoll Rand Centac - High Vibration Analysis",
         "Ingersoll Rand Centac centrifugal compressor vibration analysis: The Centac uses a multi-stage bull gear / pinion design with impellers mounted directly on pinion shafts. Normal vibration: < 25 microns peak-to-peak at pinion bearings. CENTAC-SPECIFIC VIBRATION ISSUES: (1) Pinion bearing wear — the Centac uses sleeve bearings; wear shows as sub-synchronous vibration at 0.3-0.48x running speed. (2) Gear mesh issues — vibration at gear mesh frequency (bull gear RPM x number of teeth) with sidebands indicates gear wear or misalignment. (3) Impeller fouling — mass imbalance creates 1x running speed vibration. Verify by comparing amplitudes across stages. (4) Bull gear bearing — journal bearing clearance specification is 0.002-0.003 inches per inch of journal diameter. (5) The Centac Xe controller monitors all vibration channels — alarm at 50 microns, trip at 75 microns. Check probe gap voltage (6-10 VDC for Bently Nevada 3300 series probes). INGERSOLL RAND SERVICE: Contact IR Customer Center for vibration analysis support and remote diagnostic via Centac Xe connected services."),
        ("Ingersoll Rand Centac", "COMPRESSOR", "maintenance_procedure", "Ingersoll Rand Centac - Bearing Service",
         "Ingersoll Rand Centac bearing inspection and replacement: The Centac uses hydrodynamic sleeve bearings (tilting pad on pinion, plain journal on bull gear). INSPECTION INTERVAL: Pinion bearings every 4 years, bull gear bearings every 6 years. PROCEDURE: (1) Remove top bearing housing cover. (2) Inspect babbitt surface — acceptable surface is smooth, silver-gray. Reject if: wiping marks, embedded particles, or discoloration visible. (3) Measure bearing clearance with Plastigage — specification 0.0015-0.003 per inch of journal diameter. (4) Check shaft condition at bearing journals — 16 micro-inch finish or better. (5) Inspect thrust bearing pads — the Centac uses a self-equalizing design. All pads must be within 0.0005 inches of each other in thickness. CENTAC-SPECIFIC: The pinion can be removed without pulling the bull gear on Centac C-series (C400+). For older models, full gear train disassembly is required. Bearing part numbers are model-specific — reference Centac IOM manual section 8 with unit serial number."),
    ]

    for section_type, title, content in pump_sections:
        docs.append({
            "doc_id": doc_id,
            "asset_type": "PUMP",
            "model_name": None,
            "section_type": section_type,
            "title": title,
            "content": content,
            "source_url": "https://www.api.org/standards/610",
        })
        doc_id += 1

    for section_type, title, content in compressor_sections:
        docs.append({
            "doc_id": doc_id,
            "asset_type": "COMPRESSOR",
            "model_name": None,
            "section_type": section_type,
            "title": title,
            "content": content,
            "source_url": "https://www.api.org/standards/618",
        })
        doc_id += 1

    for model_name, asset_type, section_type, title, content in model_specific_docs:
        docs.append({
            "doc_id": doc_id,
            "asset_type": asset_type,
            "model_name": model_name,
            "section_type": section_type,
            "title": title,
            "content": content,
            "source_url": f"https://www.{model_name.split()[0].lower()}.com/manuals",
        })
        doc_id += 1

    return docs

# ---------------------------------------------------------------------------
# 8) Feature engineering
# ---------------------------------------------------------------------------
def compute_features(assets, telemetry_rows, maintenance_logs):
    """Compute rolling window features from telemetry for ML training.
    
    Samples every 6h for healthy assets and every 1h during degradation windows
    to produce enough labeled failure-mode examples for reliable classification.
    """
    tdf = pd.DataFrame(telemetry_rows)
    tdf["TS"] = pd.to_datetime(tdf["TS"])

    asset_map = {a["asset_id"]: a for a in assets}
    maint_df = pd.DataFrame(maintenance_logs)
    maint_df["TS"] = pd.to_datetime(maint_df["TS"])

    features = []
    normal_sample_interval = 24   # every 6h for healthy periods
    degrade_sample_interval = 4   # every 1h during degradation periods

    for aid in tdf["ASSET_ID"].unique():
        at = tdf[tdf["ASSET_ID"] == aid].sort_values("TS").reset_index(drop=True)
        asset = asset_map[aid]
        asset_maint = maint_df[maint_df["ASSET_ID"] == aid].sort_values("TS")

        deg_info = DEGRADING_ASSETS.get(aid)
        degrade_start_ts = None
        if deg_info:
            degrade_start_ts = NOW - timedelta(days=deg_info["start_offset_days"])

        window_size = 96
        idx = window_size
        while idx < len(at):
            window = at.iloc[max(0, idx - window_size):idx]
            ts = at.iloc[idx]["TS"]

            past_maint = asset_maint[asset_maint["TS"] < ts]
            last_maint = past_maint.iloc[-1]["TS"] if len(past_maint) > 0 else ts - timedelta(days=180)
            days_since = (ts - last_maint).days
            maint_count_90d = len(past_maint[past_maint["TS"] > ts - timedelta(days=90)])

            if deg_info:
                if ts >= degrade_start_ts:
                    progress = min(1.0, (ts - degrade_start_ts).total_seconds() / (timedelta(days=deg_info["start_offset_days"]).total_seconds()))
                    days_to_fail = max(0, deg_info["target_rul_now"] + deg_info["start_offset_days"] * (1 - progress))
                    label = deg_info["mode"]
                    step = degrade_sample_interval
                else:
                    days_to_fail = (degrade_start_ts - ts).days + deg_info["start_offset_days"] + deg_info["target_rul_now"]
                    label = "NORMAL"
                    step = normal_sample_interval
            else:
                label = "NORMAL"
                days_to_fail = 999.0
                step = normal_sample_interval

            row = {
                "ASSET_ID": aid,
                "AS_OF_TS": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "ASSET_TYPE": asset["asset_type"],
                "VIBRATION_MEAN_24H": round(float(window["VIBRATION"].mean()), 2),
                "VIBRATION_STD_24H": round(float(window["VIBRATION"].std()), 4),
                "VIBRATION_MAX_24H": round(float(window["VIBRATION"].max()), 2),
                "VIBRATION_TREND": round(float(np.polyfit(range(len(window)), window["VIBRATION"].values, 1)[0] if len(window) > 1 else 0), 6),
                "TEMPERATURE_MEAN_24H": round(float(window["TEMPERATURE"].mean()), 2),
                "TEMPERATURE_STD_24H": round(float(window["TEMPERATURE"].std()), 4),
                "TEMPERATURE_MAX_24H": round(float(window["TEMPERATURE"].max()), 2),
                "TEMPERATURE_TREND": round(float(np.polyfit(range(len(window)), window["TEMPERATURE"].values, 1)[0] if len(window) > 1 else 0), 6),
                "PRESSURE_MEAN_24H": round(float(window["PRESSURE"].mean()), 2),
                "PRESSURE_STD_24H": round(float(window["PRESSURE"].std()), 4),
                "FLOW_RATE_MEAN_24H": round(float(window["FLOW_RATE"].mean()), 2),
                "RPM_MEAN_24H": round(float(window["RPM"].mean()), 2),
                "RPM_STD_24H": round(float(window["RPM"].std()), 4),
                "POWER_DRAW_MEAN_24H": round(float(window["POWER_DRAW"].mean()), 2),
                "DIFF_PRESSURE_MEAN_24H": round(float(window["DIFFERENTIAL_PRESSURE"].mean()), 2) if window["DIFFERENTIAL_PRESSURE"].notna().any() else None,
                "SEAL_TEMP_MEAN_24H": round(float(window["SEAL_TEMPERATURE"].mean()), 2) if window["SEAL_TEMPERATURE"].notna().any() else None,
                "DISCHARGE_TEMP_MEAN_24H": round(float(window["DISCHARGE_TEMP"].mean()), 2) if window["DISCHARGE_TEMP"].notna().any() else None,
                "COMPRESSION_RATIO_MEAN": round(float(window["COMPRESSION_RATIO"].mean()), 4) if window["COMPRESSION_RATIO"].notna().any() else None,
                "OIL_PRESSURE_MEAN_24H": round(float(window["OIL_PRESSURE"].mean()), 2) if window["OIL_PRESSURE"].notna().any() else None,
                "DAYS_SINCE_MAINTENANCE": days_since,
                "MAINTENANCE_COUNT_90D": maint_count_90d,
                "OPERATING_HOURS": round((ts - datetime.strptime(asset["install_date"], "%Y-%m-%d")).total_seconds() / 3600, 1),
                "FAILURE_LABEL": label,
                "DAYS_TO_FAILURE": round(days_to_fail, 1),
            }
            features.append(row)
            idx += step

    return features

# ---------------------------------------------------------------------------
# MAIN - connect and load
# ---------------------------------------------------------------------------
def connect_to_snowflake():
    conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME")
    if conn_name:
        return snowflake.connector.connect(connection_name=conn_name)

    account = os.getenv("SNOWFLAKE_ACCOUNT", "")
    user = os.getenv("SNOWFLAKE_USER", "")
    pat = os.getenv("SNOWFLAKE_PAT", "")

    if pat:
        return snowflake.connector.connect(
            account=account,
            user=user,
            token=pat,
            authenticator="oauth",
            warehouse=WAREHOUSE,
            database=DATABASE,
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        )

    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default"),
    )


def load_table(conn, df, schema, table):
    """Write a pandas DataFrame into a Snowflake table."""
    conn.cursor().execute(f"USE SCHEMA {DATABASE}.{schema}")
    print(f"  Loading {len(df)} rows into {DATABASE}.{schema}.{table}...")
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table, database=DATABASE, schema=schema, auto_create_table=False, overwrite=True
    )
    print(f"  -> {nrows} rows loaded ({nchunks} chunks)")


def main():
    print("=== Midstream PDM - Synthetic Data Generator ===\n")

    print("[1/9] Generating stations...")
    stations_df = pd.DataFrame(STATIONS)
    stations_df.columns = [c.upper() for c in stations_df.columns]

    print("[2/9] Generating assets...")
    assets = generate_assets()
    assets_df = pd.DataFrame(assets)
    assets_df.columns = [c.upper() for c in assets_df.columns]

    print("[3/9] Generating telemetry (this may take a minute)...")
    telemetry_rows = generate_telemetry(assets)
    telemetry_df = pd.DataFrame(telemetry_rows)
    print(f"  Generated {len(telemetry_df)} telemetry rows")

    print("[4/9] Generating maintenance logs...")
    maint_logs = generate_maintenance(assets)
    maint_df = pd.DataFrame(maint_logs)

    print("[5/9] Generating parts inventory...")
    parts = generate_parts()
    parts_df = pd.DataFrame(parts)
    parts_df.columns = [c.upper() for c in parts_df.columns]

    print("[6/9] Setting up technicians...")
    tech_df = pd.DataFrame(TECHNICIANS)
    tech_df.columns = [c.upper() for c in tech_df.columns]

    print("[7/9] Generating manuals...")
    manuals = generate_manuals()
    manuals_df = pd.DataFrame(manuals)
    manuals_df.columns = [c.upper() for c in manuals_df.columns]

    print("[8/8] Computing feature store...")
    features = compute_features(assets, telemetry_rows, maint_logs)
    features_df = pd.DataFrame(features)
    print(f"  Generated {len(features_df)} feature rows")

    print("\nConnecting to Snowflake...")
    conn = connect_to_snowflake()
    conn.cursor().execute(f"USE WAREHOUSE {WAREHOUSE}")
    conn.cursor().execute(f"USE DATABASE {DATABASE}")

    print("\nLoading data into Snowflake tables:")
    load_table(conn, stations_df, "RAW", "STATIONS")
    load_table(conn, assets_df, "RAW", "ASSETS")
    load_table(conn, telemetry_df, "RAW", "TELEMETRY")
    load_table(conn, maint_df, "RAW", "MAINTENANCE_LOGS")
    load_table(conn, parts_df, "RAW", "PARTS_INVENTORY")
    load_table(conn, tech_df, "RAW", "TECHNICIANS")
    load_table(conn, manuals_df, "APP", "MANUALS")
    load_table(conn, features_df, "ANALYTICS", "FEATURE_STORE")

    print("\n=== Data generation complete! ===")
    print(f"  Stations: {len(stations_df)}")
    print(f"  Assets: {len(assets_df)}")
    print(f"  Telemetry rows: {len(telemetry_df)}")
    print(f"  Maintenance logs: {len(maint_df)}")
    print(f"  Parts: {len(parts_df)}")
    print(f"  Technicians: {len(tech_df)}")
    print(f"  Manual sections: {len(manuals_df)}")
    print(f"  Feature rows: {len(features_df)}")
    print("\nNOTE: Run ml_training.ipynb then score_fleet.ipynb to generate predictions.")

    conn.close()


if __name__ == "__main__":
    main()
