-- ============================================================================
-- Route Planner Stored Procedure (v2 - Intelligent Routing)
-- Plans optimized service routes with AI-driven efficiency:
--   - Technician certifications, skills, location & availability
--   - Asset condition (risk/RUL) with nearest-neighbor geographic routing
--   - ALWAYS bundles same-site work first before considering next location
--   - Looks ahead for preventive/co-replacement opportunities
--   - Explains reasoning for each routing decision
-- ============================================================================

USE DATABASE PDM_DEMO;
USE WAREHOUSE PDM_DEMO_WH;

CREATE OR REPLACE PROCEDURE APP.PLAN_ROUTE(
    TECH_ID VARCHAR,
    PRIMARY_ASSET_ID INT,
    HORIZON_DAYS INT DEFAULT 3,
    MAX_STOPS INT DEFAULT 5,
    AS_OF_TS_PARAM VARCHAR DEFAULT NULL,
    ALLOW_OVERTIME BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'plan_route'
AS $$
def plan_route(session, tech_id, primary_asset_id, horizon_days, max_stops, as_of_ts_param=None, allow_overtime=False):
    import json
    import math
    from datetime import datetime, timedelta

    MAX_DAY_HOURS_NORMAL = 8.0
    MAX_DAY_HOURS_OT = 10.0
    TRAVEL_SPEED_MPH = 45
    REPAIR_HOURS = {
        "BEARING_WEAR": 4.0, "SEAL_LEAK": 3.5, "VALVE_FAILURE": 5.0,
        "SURGE": 3.0, "OVERHEATING": 2.5, "CAVITATION": 3.5,
        "NORMAL": 2.0, "OFFLINE": 6.0,
    }
    CO_MAINT_BUDGET_RATIO = 0.35

    max_day_hours = MAX_DAY_HOURS_OT if allow_overtime else MAX_DAY_HOURS_NORMAL

    tech_rows = session.sql(f"""
        SELECT HOME_BASE_LAT, HOME_BASE_LON, HOME_BASE_CITY, NAME, CERTIFICATIONS,
               SPECIALTY_NOTES, AVAILABILITY, YEARS_EXPERIENCE, HOURLY_RATE
        FROM PDM_DEMO.RAW.TECHNICIANS
        WHERE TECH_ID = '{tech_id}'
    """).collect()

    if not tech_rows:
        return json.dumps({"error": f"Technician {tech_id} not found"})

    tech = tech_rows[0]
    home_lat = float(tech.HOME_BASE_LAT)
    home_lon = float(tech.HOME_BASE_LON)

    certs_raw = tech.CERTIFICATIONS or ""
    try:
        tech_certs = json.loads(certs_raw) if certs_raw.startswith("[") else [c.strip() for c in certs_raw.split(",") if c.strip()]
    except:
        tech_certs = [c.strip() for c in certs_raw.split(",") if c.strip()]
    tech_certs_lower = [c.lower() for c in tech_certs]
    specialty = (tech.SPECIALTY_NOTES or "").lower()

    base_date_str = as_of_ts_param[:10] if as_of_ts_param else "2026-03-13"
    base_date = datetime.strptime(base_date_str, "%Y-%m-%d")

    sched_rows = session.sql(f"""
        SELECT TO_CHAR(SCHEDULE_DATE, 'YYYY-MM-DD') AS SCHED_DATE,
               SUM(ESTIMATED_HOURS) AS BOOKED,
               LISTAGG(DISTINCT BLOCK_TYPE, ',') AS BLOCK_TYPES
        FROM PDM_DEMO.APP.TECH_SCHEDULES
        WHERE TECH_ID = '{tech_id}'
          AND SCHEDULE_DATE >= '{base_date_str}'::DATE
          AND SCHEDULE_DATE < DATEADD('day', {horizon_days}, '{base_date_str}'::DATE)
        GROUP BY SCHEDULE_DATE
    """).collect()

    booked_by_date = {}
    blocked_dates = set()
    for r in sched_rows:
        booked_by_date[r.SCHED_DATE] = float(r.BOOKED)
        block_types = (r.BLOCK_TYPES or "").upper()
        if any(bt in block_types for bt in ['PTO', 'VACATION', 'TRAINING', 'SICK']):
            if float(r.BOOKED) >= 6.0:
                blocked_dates.add(r.SCHED_DATE)

    ts_filter = ""
    if as_of_ts_param:
        ts_filter = f"AND TS::TIMESTAMP_NTZ <= '{as_of_ts_param}'::TIMESTAMP_NTZ"

    candidates = session.sql(f"""
        WITH latest_pred AS (
            SELECT 
                PUMP_ID AS ASSET_ID,
                TS AS PREDICTION_TS,
                PREDICTED_CLASS,
                PREDICTED_RUL_DAYS,
                CONFIDENCE,
                TOP_FEATURE,
                CASE 
                    WHEN PREDICTED_CLASS = 'OFFLINE' THEN 'FAILED'
                    WHEN PREDICTED_RUL_DAYS IS NOT NULL AND PREDICTED_RUL_DAYS <= 7 THEN 'CRITICAL'
                    WHEN PREDICTED_RUL_DAYS IS NOT NULL AND PREDICTED_RUL_DAYS <= 14 THEN 'WARNING'
                    ELSE 'HEALTHY'
                END AS RISK_LEVEL
            FROM PDM_DEMO.ANALYTICS.PREDICTIONS
            WHERE 1=1 {ts_filter}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY PUMP_ID ORDER BY TS::TIMESTAMP_NTZ DESC) = 1
        ),
        primary_station AS (
            SELECT s.STATION_ID, s.LAT, s.LON
            FROM PDM_DEMO.RAW.ASSETS a
            JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
            WHERE a.ASSET_ID = {primary_asset_id}
        )
        SELECT
            a.ASSET_ID, a.ASSET_TYPE, a.STATION_ID, a.MODEL_NAME,
            s.NAME AS STATION_NAME, s.LAT, s.LON,
            p.PREDICTED_CLASS, p.PREDICTED_RUL_DAYS, p.RISK_LEVEL, p.TOP_FEATURE,
            HAVERSINE(s.LAT, s.LON, ps.LAT, ps.LON) AS DISTANCE_MILES
        FROM PDM_DEMO.RAW.ASSETS a
        JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
        JOIN latest_pred p ON a.ASSET_ID = p.ASSET_ID
        CROSS JOIN primary_station ps
        LEFT JOIN PDM_DEMO.APP.WORK_ORDERS wo
            ON a.ASSET_ID = wo.ASSET_ID AND wo.STATUS IN ('SCHEDULED', 'IN_PROGRESS')
        WHERE (
            a.ASSET_ID = {primary_asset_id}
            OR (p.RISK_LEVEL IN ('WARNING', 'CRITICAL', 'FAILED') AND wo.WO_ID IS NULL)
        )
        ORDER BY
            CASE WHEN a.ASSET_ID = {primary_asset_id} THEN 0 ELSE 1 END,
            CASE p.RISK_LEVEL WHEN 'FAILED' THEN 0 WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
            p.PREDICTED_RUL_DAYS ASC,
            DISTANCE_MILES ASC
        LIMIT {max_stops * horizon_days * 2}
    """).collect()

    co_located_rows = session.sql(f"""
        WITH latest_pred AS (
            SELECT 
                PUMP_ID AS ASSET_ID,
                TS AS PREDICTION_TS,
                PREDICTED_CLASS,
                PREDICTED_RUL_DAYS,
                CONFIDENCE,
                TOP_FEATURE,
                CASE 
                    WHEN PREDICTED_CLASS = 'OFFLINE' THEN 'FAILED'
                    WHEN PREDICTED_RUL_DAYS IS NOT NULL AND PREDICTED_RUL_DAYS <= 7 THEN 'CRITICAL'
                    WHEN PREDICTED_RUL_DAYS IS NOT NULL AND PREDICTED_RUL_DAYS <= 14 THEN 'WARNING'
                    ELSE 'HEALTHY'
                END AS RISK_LEVEL
            FROM PDM_DEMO.ANALYTICS.PREDICTIONS
            WHERE 1=1 {ts_filter}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY PUMP_ID ORDER BY TS::TIMESTAMP_NTZ DESC) = 1
        ),
        candidate_stations AS (
            SELECT DISTINCT a.STATION_ID
            FROM PDM_DEMO.RAW.ASSETS a
            JOIN latest_pred p ON a.ASSET_ID = p.ASSET_ID
            WHERE a.ASSET_ID = {primary_asset_id}
               OR p.RISK_LEVEL IN ('WARNING', 'CRITICAL', 'FAILED')
        ),
        latest_features AS (
            SELECT *
            FROM PDM_DEMO.ANALYTICS.FEATURE_STORE
            WHERE AS_OF_TS <= '{as_of_ts_param or "2026-03-13T00:00:00"}'::TIMESTAMP_NTZ
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSET_ID ORDER BY AS_OF_TS DESC) = 1
        ),
        last_maintenance AS (
            SELECT ASSET_ID,
                   MAX(TS) AS LAST_MAINT_TS,
                   MAX_BY(MAINTENANCE_TYPE, TS) AS LAST_MAINT_TYPE,
                   MAX_BY(DESCRIPTION, TS) AS LAST_MAINT_DESC,
                   COUNT(*) AS TOTAL_MAINT_COUNT
            FROM PDM_DEMO.RAW.MAINTENANCE_LOGS
            WHERE TS <= '{as_of_ts_param or "2026-03-13T00:00:00"}'::TIMESTAMP_NTZ
            GROUP BY ASSET_ID
        )
        SELECT
            a.ASSET_ID, a.ASSET_TYPE, a.STATION_ID, a.MODEL_NAME, a.MANUFACTURER,
            a.INSTALL_DATE,
            s.NAME AS STATION_NAME, s.LAT, s.LON,
            p.PREDICTED_CLASS, p.PREDICTED_RUL_DAYS, p.RISK_LEVEL,
            f.VIBRATION_MEAN_24H, f.VIBRATION_TREND,
            f.TEMPERATURE_MEAN_24H, f.TEMPERATURE_TREND,
            f.DAYS_SINCE_MAINTENANCE, f.OPERATING_HOURS,
            f.SEAL_TEMP_MEAN_24H, f.OIL_PRESSURE_MEAN_24H,
            f.PRESSURE_MEAN_24H, f.PRESSURE_STD_24H,
            m.LAST_MAINT_TS, m.LAST_MAINT_TYPE, m.LAST_MAINT_DESC, m.TOTAL_MAINT_COUNT
        FROM PDM_DEMO.RAW.ASSETS a
        JOIN PDM_DEMO.RAW.STATIONS s ON a.STATION_ID = s.STATION_ID
        JOIN latest_pred p ON a.ASSET_ID = p.ASSET_ID
        JOIN candidate_stations cs ON a.STATION_ID = cs.STATION_ID
        LEFT JOIN latest_features f ON a.ASSET_ID = f.ASSET_ID
        LEFT JOIN last_maintenance m ON a.ASSET_ID = m.ASSET_ID
        LEFT JOIN PDM_DEMO.APP.WORK_ORDERS wo
            ON a.ASSET_ID = wo.ASSET_ID AND wo.STATUS IN ('SCHEDULED', 'IN_PROGRESS')
        WHERE p.RISK_LEVEL = 'HEALTHY'
          AND wo.WO_ID IS NULL
    """).collect()

    def build_co_maint_tasks(r, base_date):
        tasks = []
        asset_type = r.ASSET_TYPE
        vib_trend = float(r.VIBRATION_TREND) if r.VIBRATION_TREND else 0.0
        temp_trend = float(r.TEMPERATURE_TREND) if r.TEMPERATURE_TREND else 0.0
        days_since = int(r.DAYS_SINCE_MAINTENANCE) if r.DAYS_SINCE_MAINTENANCE else 999
        op_hours = float(r.OPERATING_HOURS) if r.OPERATING_HOURS else 0.0
        seal_temp = float(r.SEAL_TEMP_MEAN_24H) if r.SEAL_TEMP_MEAN_24H else None
        oil_press = float(r.OIL_PRESSURE_MEAN_24H) if r.OIL_PRESSURE_MEAN_24H else None
        vib_mean = float(r.VIBRATION_MEAN_24H) if r.VIBRATION_MEAN_24H else 0.0
        temp_mean = float(r.TEMPERATURE_MEAN_24H) if r.TEMPERATURE_MEAN_24H else 0.0
        press_std = float(r.PRESSURE_STD_24H) if r.PRESSURE_STD_24H else 0.0
        last_maint_type = r.LAST_MAINT_TYPE or ""
        model = r.MODEL_NAME or "Unknown"
        manufacturer = r.MANUFACTURER or ""

        if vib_trend > 0.001:
            tasks.append({
                "task": f"Vibration trending up — {model} at +{vib_trend:.4f}/hr, inspect bearings & alignment",
                "estimated_hours": 1.25,
                "trigger": "sensor_trend",
                "priority": 1,
                "reasoning": f"Vibration increasing at {vib_trend:.4f}/hr indicates developing bearing wear or misalignment. Early intervention prevents catastrophic failure."
            })
        elif vib_mean > 4.0:
            tasks.append({
                "task": f"Elevated vibration — {model} at {vib_mean:.1f} mm/s, check mounting & balance",
                "estimated_hours": 0.75,
                "trigger": "sensor_threshold",
                "priority": 2,
                "reasoning": f"Vibration level {vib_mean:.1f} mm/s exceeds 4.0 mm/s threshold. Verify mounting bolts and impeller balance."
            })

        if temp_trend > 0.005:
            tasks.append({
                "task": f"Temperature rising — {model} at +{temp_trend:.4f}°F/hr, check cooling & lubrication",
                "estimated_hours": 0.75,
                "trigger": "sensor_trend",
                "priority": 1,
                "reasoning": f"Temperature increasing {temp_trend:.4f}°F/hr suggests degrading lubrication or blocked cooling. Address before thermal damage occurs."
            })

        if asset_type == "PUMP" and seal_temp is not None and seal_temp > 160:
            tasks.append({
                "task": f"Seal temp elevated — {manufacturer} at {seal_temp:.0f}°F (limit 170°F)",
                "estimated_hours": 1.0,
                "trigger": "sensor_threshold",
                "priority": 1,
                "reasoning": f"Seal temperature {seal_temp:.0f}°F approaching 170°F limit. Inspect for dry running or flush plan issues."
            })

        if press_std > 3.0:
            tasks.append({
                "task": f"Pressure instability — {model} variance {press_std:.1f} PSI, inspect valves",
                "estimated_hours": 0.75,
                "trigger": "sensor_threshold",
                "priority": 2,
                "reasoning": f"Pressure variance of {press_std:.1f} PSI indicates possible valve seat wear or relief valve issues."
            })

        if days_since > 45:
            tasks.append({
                "task": f"Overdue PM — {model} at {days_since}d since service (rec. 30d)",
                "estimated_hours": 1.5,
                "trigger": "maintenance_overdue",
                "priority": 1,
                "reasoning": f"Maintenance interval exceeded by {days_since - 30} days. Full preventive service required to maintain reliability."
            })
        elif days_since > 25:
            tasks.append({
                "task": f"PM due soon — {model} at {days_since}d, lubrication & filter check",
                "estimated_hours": 0.75,
                "trigger": "maintenance_due_soon",
                "priority": 3,
                "reasoning": f"Approaching 30-day maintenance interval. Performing service now while on-site saves a dedicated trip."
            })

        if op_hours > 100000 and not any(t["trigger"] == "sensor_trend" for t in tasks):
            tasks.append({
                "task": f"High-hour inspection — {model} at {op_hours:,.0f} hrs",
                "estimated_hours": 1.0,
                "trigger": "operating_hours",
                "priority": 2,
                "reasoning": f"Equipment has {op_hours:,.0f} operating hours. Recommend wear component inspection and oil analysis."
            })

        if not tasks:
            tasks.append({
                "task": f"Visual inspection — {model} ({manufacturer})",
                "estimated_hours": 0.5,
                "trigger": "routine",
                "priority": 4,
                "reasoning": "No specific issues detected. Basic visual inspection while on-site is low-cost preventive measure."
            })

        tasks.sort(key=lambda t: t["priority"])
        return tasks

    co_by_station = {}
    for r in co_located_rows:
        sid = int(r.STATION_ID)
        if sid not in co_by_station:
            co_by_station[sid] = []
        co_by_station[sid].append({
            "ASSET_ID": r.ASSET_ID, "ASSET_TYPE": r.ASSET_TYPE,
            "MODEL_NAME": r.MODEL_NAME or "Unknown",
            "PREDICTED_CLASS": r.PREDICTED_CLASS or "NORMAL",
            "PREDICTED_RUL_DAYS": float(r.PREDICTED_RUL_DAYS) if r.PREDICTED_RUL_DAYS else None,
            "tasks": build_co_maint_tasks(r, base_date),
        })

    def haversine(lat1, lon1, lat2, lon2):
        R = 3959
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
        return R * 2 * math.asin(math.sqrt(a))

    def check_cert(asset_type):
        at = asset_type.lower()
        return any(at in c for c in tech_certs_lower)

    def check_specialty(pred_class):
        skill_map = {
            "BEARING_WEAR": ["bearing", "vibration"],
            "SEAL_LEAK": ["seal", "alignment"],
            "VALVE_FAILURE": ["valve", "calibration"],
            "OVERHEATING": ["thermal", "overheating", "cooling"],
            "CAVITATION": ["cavitation", "npsh", "suction"],
        }
        skills = skill_map.get(pred_class, [])
        if not skills:
            return False, []
        matched = [s for s in skills if s in specialty]
        return len(matched) > 0, matched

    cand_list = []
    for row in candidates:
        asset_type = row.ASSET_TYPE
        cert_ok = check_cert(asset_type)
        spec_match, spec_skills = check_specialty(row.PREDICTED_CLASS)

        cand_list.append({
            "ASSET_ID": row.ASSET_ID, "ASSET_TYPE": asset_type,
            "STATION_ID": int(row.STATION_ID),
            "STATION_NAME": row.STATION_NAME,
            "MODEL_NAME": row.MODEL_NAME or "Unknown",
            "LAT": float(row.LAT), "LON": float(row.LON),
            "PREDICTED_CLASS": row.PREDICTED_CLASS,
            "PREDICTED_RUL_DAYS": float(row.PREDICTED_RUL_DAYS) if row.PREDICTED_RUL_DAYS else None,
            "RISK_LEVEL": row.RISK_LEVEL,
            "TOP_FEATURE": row.TOP_FEATURE,
            "DISTANCE_MILES": float(row.DISTANCE_MILES),
            "CERT_MATCH": cert_ok,
            "SPECIALTY_MATCH": spec_match,
            "SPECIALTY_SKILLS": spec_skills,
        })

    primary = [c for c in cand_list if c["ASSET_ID"] == primary_asset_id]
    others = [c for c in cand_list if c["ASSET_ID"] != primary_asset_id]

    ordered = list(primary)
    cur_lat = ordered[0]["LAT"] if ordered else home_lat
    cur_lon = ordered[0]["LON"] if ordered else home_lon

    while others:
        others.sort(key=lambda c: haversine(cur_lat, cur_lon, c["LAT"], c["LON"]))
        nxt = others.pop(0)
        ordered.append(nxt)
        cur_lat, cur_lon = nxt["LAT"], nxt["LON"]

    stops = []
    prev_lat, prev_lon = home_lat, home_lon
    current_day = 1
    day_hours_used = booked_by_date.get(base_date.strftime("%Y-%m-%d"), 0.0)
    day_stops_count = 0
    visited_stations = set()
    warnings = []
    routing_decisions = []

    if not primary or not primary[0].get("CERT_MATCH"):
        if primary:
            p = primary[0]
            warnings.append(f"Tech {tech.NAME} lacks {p['ASSET_TYPE'].lower()} certification but is assigned to primary asset {primary_asset_id}")

    for row in ordered:
        parts_rows = session.sql(f"""
            SELECT DISTINCT PART_NAME, CATEGORY
            FROM PDM_DEMO.RAW.PARTS_INVENTORY
            WHERE ASSET_TYPE = '{row["ASSET_TYPE"]}'
              AND CATEGORY IN (
                  CASE '{row["PREDICTED_CLASS"]}'
                      WHEN 'BEARING_WEAR' THEN 'bearing'
                      WHEN 'SEAL_LEAK' THEN 'seal'
                      WHEN 'VALVE_FAILURE' THEN 'valve'
                      WHEN 'CAVITATION' THEN 'impeller'
                      WHEN 'OVERHEATING' THEN 'filter'
                      ELSE 'general'
                  END, 'general'
              )
            LIMIT 5
        """).collect()

        lat, lon = row["LAT"], row["LON"]
        leg = round(haversine(prev_lat, prev_lon, lat, lon), 1)
        travel_h = round(leg / TRAVEL_SPEED_MPH, 1) if leg > 0 else 0.0
        repair_h = REPAIR_HOURS.get(row["PREDICTED_CLASS"], 3.0)
        stop_h = travel_h + repair_h

        sched_date_str = (base_date + timedelta(days=current_day - 1)).strftime("%Y-%m-%d")
        day_budget = max_day_hours - booked_by_date.get(sched_date_str, 0.0) if current_day > 1 else max_day_hours - day_hours_used + booked_by_date.get(sched_date_str, 0.0)
        if current_day == 1:
            day_budget = max_day_hours - day_hours_used

        if len(stops) > 0 and (day_hours_used + stop_h > max_day_hours or day_stops_count >= max_stops):
            current_day += 1
            sched_date_str = (base_date + timedelta(days=current_day - 1)).strftime("%Y-%m-%d")
            while sched_date_str in blocked_dates and current_day <= horizon_days:
                current_day += 1
                sched_date_str = (base_date + timedelta(days=current_day - 1)).strftime("%Y-%m-%d")
            day_hours_used = booked_by_date.get(sched_date_str, 0.0)
            day_stops_count = 0

        if current_day > horizon_days:
            break

        day_hours_used += stop_h
        day_stops_count += 1
        sched_date = (base_date + timedelta(days=current_day - 1)).strftime("%Y-%m-%d")
        station_id = row["STATION_ID"]
        visited_stations.add(station_id)

        reason_parts = []
        reason_parts.append(f"{row['PREDICTED_CLASS']} predicted")
        if row["PREDICTED_RUL_DAYS"] is not None:
            reason_parts.append(f"{row['PREDICTED_RUL_DAYS']:.1f}d RUL")
        if row["CERT_MATCH"]:
            reason_parts.append("cert match")
        if row["SPECIALTY_MATCH"]:
            reason_parts.append(f"specialist: {'/'.join(row['SPECIALTY_SKILLS'])}")

        stop_reasoning = f"Asset {row['ASSET_ID']} ({row['MODEL_NAME']}) shows {row['PREDICTED_CLASS']} failure pattern"
        if row["TOP_FEATURE"]:
            stop_reasoning += f" driven by {row['TOP_FEATURE']}"
        if row["PREDICTED_RUL_DAYS"] is not None:
            stop_reasoning += f". Estimated {row['PREDICTED_RUL_DAYS']:.1f} days until failure."
        
        routing_decisions.append({
            "asset_id": row["ASSET_ID"],
            "decision": "primary_stop" if row["ASSET_ID"] == primary_asset_id else "added_to_route",
            "reasoning": stop_reasoning
        })

        stops.append({
            "stop_number": len(stops) + 1,
            "asset_id": row["ASSET_ID"],
            "asset_type": row["ASSET_TYPE"],
            "model_name": row["MODEL_NAME"],
            "station": row["STATION_NAME"],
            "station_id": station_id,
            "lat": lat, "lon": lon,
            "predicted_class": row["PREDICTED_CLASS"],
            "rul_days": row["PREDICTED_RUL_DAYS"],
            "risk_level": row["RISK_LEVEL"],
            "top_feature": row["TOP_FEATURE"],
            "distance_from_primary": round(row["DISTANCE_MILES"], 1),
            "leg_miles": leg,
            "travel_hours": travel_h,
            "estimated_repair_hours": repair_h,
            "scheduled_day": current_day,
            "scheduled_date": sched_date,
            "parts_needed": [{"name": p.PART_NAME, "category": p.CATEGORY} for p in parts_rows],
            "co_maintenance": [],
            "cert_match": row["CERT_MATCH"],
            "specialty_match": row["SPECIALTY_MATCH"],
            "reason": ", ".join(reason_parts),
            "reasoning": stop_reasoning,
        })
        prev_lat, prev_lon = lat, lon

    day_hours_by_day = {}
    for s in stops:
        d = s["scheduled_day"]
        day_hours_by_day[d] = day_hours_by_day.get(d, 0) + s["travel_hours"] + s["estimated_repair_hours"]

    day_summary = {}

    for d in sorted(day_hours_by_day.keys()):
        sched_date_str = (base_date + timedelta(days=d - 1)).strftime("%Y-%m-%d")
        booked_h = booked_by_date.get(sched_date_str, 0.0)
        work_hours = day_hours_by_day.get(d, 0)
        co_maint_hours = 0.0
        remaining = max_day_hours - work_hours - booked_h

        day_stops = [s for s in stops if s["scheduled_day"] == d]
        if not day_stops:
            continue

        co_maint_budget = remaining * CO_MAINT_BUDGET_RATIO
        
        for stop in day_stops:
            station_id = stop["station_id"]
            co_assets = co_by_station.get(station_id, [])
            
            for ca in sorted(co_assets, key=lambda x: x["tasks"][0]["priority"] if x["tasks"] else 99):
                if ca["ASSET_ID"] == stop["asset_id"]:
                    continue
                already_assigned = any(
                    ca["ASSET_ID"] in [cm["asset_id"] for cm in st["co_maintenance"]]
                    for st in stops
                )
                if already_assigned:
                    continue
                
                for task in ca["tasks"]:
                    if remaining >= task["estimated_hours"]:
                        stop["co_maintenance"].append({
                            "asset_id": ca["ASSET_ID"],
                            "asset_type": ca["ASSET_TYPE"],
                            "model_name": ca["MODEL_NAME"],
                            "task": task["task"],
                            "estimated_hours": task["estimated_hours"],
                            "trigger": task["trigger"],
                            "reasoning": task.get("reasoning", ""),
                        })
                        remaining -= task["estimated_hours"]
                        co_maint_hours += task["estimated_hours"]
                        
                        routing_decisions.append({
                            "asset_id": ca["ASSET_ID"],
                            "decision": "co_maintenance",
                            "reasoning": task.get("reasoning", f"Added {task['task']} while technician is on-site")
                        })
                    else:
                        break

        day_summary[d] = {
            "day": d,
            "date": sched_date_str,
            "work_hours": round(work_hours, 1),
            "co_maintenance_hours": round(co_maint_hours, 1),
            "prior_bookings_hours": round(booked_h, 1),
            "total_hours": round(work_hours + co_maint_hours + booked_h, 1),
            "max_hours": max_day_hours,
            "utilization_pct": round((work_hours + co_maint_hours + booked_h) / max_day_hours * 100, 1),
        }

    for s in stops:
        co_maint_h = sum(cm["estimated_hours"] for cm in s["co_maintenance"])
        s["co_maintenance_hours"] = round(co_maint_h, 2)
        s["stop_total_hours"] = round(s["travel_hours"] + s["estimated_repair_hours"] + co_maint_h, 2)

    total_miles = sum(s["leg_miles"] for s in stops)
    total_days = max(s["scheduled_day"] for s in stops) if stops else 0
    total_co_maint = sum(len(s["co_maintenance"]) for s in stops)

    route_explanation_parts = []
    if stops:
        primary_stop = stops[0]
        primary_rul = f"{primary_stop['rul_days']:.0f}d RUL" if primary_stop['rul_days'] else "unknown RUL"
        route_explanation_parts.append(
            f"**Primary target:** Asset {primary_stop['asset_id']} ({primary_stop['model_name']}) at {primary_stop['station']} — "
            f"{primary_stop['predicted_class']} failure pattern detected, {primary_rul}, {primary_stop['risk_level']} priority."
        )
        
        if primary_stop.get('top_feature'):
            route_explanation_parts.append(
                f"Key driver: {primary_stop['top_feature']} sensor readings indicate {primary_stop['predicted_class'].lower().replace('_', ' ')}."
            )
        
        critical_stops = [s for s in stops[1:] if s['risk_level'] in ('CRITICAL', 'FAILED')]
        warning_stops = [s for s in stops[1:] if s['risk_level'] == 'WARNING']
        
        if critical_stops:
            critical_summary = ", ".join([f"Asset {s['asset_id']} ({s['rul_days']:.0f}d RUL)" for s in critical_stops[:3]])
            route_explanation_parts.append(
                f"**Critical additions:** {len(critical_stops)} asset(s) requiring immediate attention: {critical_summary}."
            )
        
        if warning_stops:
            warning_summary = ", ".join([f"Asset {s['asset_id']}" for s in warning_stops[:3]])
            route_explanation_parts.append(
                f"**Route optimization:** Bundled {len(warning_stops)} warning-level asset(s) ({warning_summary}) along the geographic path for efficiency."
            )
        
        if total_co_maint > 0:
            co_maint_triggers = {}
            for s in stops:
                for cm in s.get("co_maintenance", []):
                    trigger = cm.get("trigger", "routine")
                    co_maint_triggers[trigger] = co_maint_triggers.get(trigger, 0) + 1
            
            trigger_summary = ", ".join([f"{count} {trigger.replace('_', ' ')}" for trigger, count in co_maint_triggers.items()])
            route_explanation_parts.append(
                f"**Preventive bundling:** {total_co_maint} co-maintenance task(s) on healthy equipment at visited stations ({trigger_summary}). "
                f"This maximizes technician utilization and prevents future trips."
            )
        
        if total_miles > 0:
            route_explanation_parts.append(
                f"**Route efficiency:** {total_miles:.0f} miles over {total_days} day(s), sequenced via nearest-neighbor from {tech.NAME}'s base in {tech.HOME_BASE_CITY}."
            )
        
        avg_utilization = sum(ds["utilization_pct"] for ds in day_summary.values()) / len(day_summary) if day_summary else 0
        route_explanation_parts.append(
            f"**Schedule fit:** Average {avg_utilization:.0f}% daily utilization, respecting existing commitments."
        )

    route_explanation = "\n\n".join(route_explanation_parts) if route_explanation_parts else "No route generated."

    return json.dumps({
        "tech_id": tech_id,
        "tech_name": tech.NAME,
        "tech_certifications": tech_certs,
        "tech_availability": tech.AVAILABILITY,
        "primary_asset_id": primary_asset_id,
        "home_lat": home_lat,
        "home_lon": home_lon,
        "route": stops,
        "total_stops": len(stops),
        "total_days": total_days,
        "estimated_travel_miles": round(total_miles, 1),
        "co_maintenance_count": total_co_maint,
        "day_summary": [day_summary[d] for d in sorted(day_summary.keys())],
        "allow_overtime": allow_overtime,
        "max_hours_per_day": max_day_hours,
        "warnings": warnings,
        "route_explanation": route_explanation,
        "routing_decisions": routing_decisions,
    })
$$;
