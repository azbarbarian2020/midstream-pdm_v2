export interface Station {
  STATION_ID: number;
  NAME: string;
  LAT: number;
  LON: number;
  REGION: string;
  STATION_TYPE: string;
  ASSET_COUNT?: number;
}

export interface Asset {
  ASSET_ID: number;
  ASSET_TYPE: "PUMP" | "COMPRESSOR";
  MODEL_NAME: string;
  MANUFACTURER: string;
  INSTALL_DATE: string;
  RATED_CAPACITY: number;
  STATION_ID: number;
  STATION_NAME: string;
  LAT: number;
  LON: number;
  PREDICTED_CLASS: string | null;
  PREDICTED_RUL_DAYS: number | null;
  RISK_LEVEL: "HEALTHY" | "WARNING" | "CRITICAL" | "OFFLINE" | "FAILED" | null;
  CONFIDENCE: number | null;
  TOP_FEATURE_1: string | null;
  TOP_FEATURE_1_DELTA_PCT: number | null;
  TOP_FEATURE_2: string | null;
  TOP_FEATURE_2_DELTA_PCT: number | null;
  TOP_FEATURE_3: string | null;
  TOP_FEATURE_3_DELTA_PCT: number | null;
  ASSIGNED_TECH_ID: string | null;
  ASSIGNED_TECH_NAME: string | null;
}

export interface Prediction {
  ASSET_ID: number;
  AS_OF_TS: string;
  PREDICTED_CLASS: string;
  PREDICTED_RUL_DAYS: number;
  RISK_LEVEL: string;
  MODEL_VERSION: string;
  SCORED_AT: string;
  CONFIDENCE: number | null;
  TOP_FEATURE_1: string | null;
  TOP_FEATURE_1_DELTA_PCT: number | null;
  TOP_FEATURE_2: string | null;
  TOP_FEATURE_2_DELTA_PCT: number | null;
  TOP_FEATURE_3: string | null;
  TOP_FEATURE_3_DELTA_PCT: number | null;
}

export interface TelemetryPoint {
  ASSET_ID: number;
  TS: string;
  VIBRATION?: number;
  TEMPERATURE?: number;
  PRESSURE?: number;
  FLOW_RATE?: number;
  RPM?: number;
  POWER_DRAW?: number;
  [key: string]: number | string | undefined;
}

export interface MaintenanceLog {
  LOG_ID: number;
  TS: string;
  MAINTENANCE_TYPE: string;
  DESCRIPTION: string;
  TECHNICIAN_ID: string;
  PARTS_USED: { name: string; qty: number }[];
  DURATION_HRS: number;
  COST: number;
}

export interface KPIs {
  total_assets: number;
  offline: number;
  critical: number;
  warning: number;
  healthy: number;
  avg_rul: number;
}

export interface CoMaintenance {
  asset_id: number;
  asset_type: string;
  task: string;
  estimated_hours: number;
}

export interface RouteStop {
  asset_id: number;
  asset_type: string;
  station: string;
  station_id?: number;
  lat: number;
  lon: number;
  predicted_class: string;
  rul_days: number;
  risk_level: string;
  leg_miles: number;
  travel_hours: number;
  estimated_repair_hours: number;
  scheduled_day: number;
  scheduled_date: string;
  stop_number: number;
  parts_needed: { name: string; category: string }[];
  co_maintenance: CoMaintenance[];
  co_maintenance_hours?: number;
  stop_total_hours?: number;
  cert_match?: boolean;
  specialty_match?: boolean;
  reason: string;
}

export interface RouteResult {
  tech_id: string;
  tech_name: string;
  tech_availability: string;
  tech_certifications?: string[];
  home_lat: number;
  home_lon: number;
  primary_asset_id: number;
  route: RouteStop[];
  total_stops: number;
  total_days: number;
  estimated_travel_miles: number;
  co_maintenance_count?: number;
  allow_overtime?: boolean;
  max_hours_per_day?: number;
  warnings?: string[];
}

export interface Technician {
  TECH_ID: string;
  NAME: string;
  HOME_BASE_LAT: number;
  HOME_BASE_LON: number;
  HOME_BASE_CITY: string;
  CERTIFICATIONS: string[];
  AVAILABILITY: string;
  YEARS_EXPERIENCE: number;
  SPECIALTY_NOTES: string;
  BIO: string;
  PHOTO_URL: string | null;
  HOURLY_RATE: number;
}

export interface TechScheduleBlock {
  SCHEDULE_ID: number;
  TECH_ID: string;
  TECH_NAME: string;
  SCHEDULE_DATE: string;
  BLOCK_TYPE: string;
  WO_ID: number | null;
  ASSET_ID: number | null;
  STATION_NAME: string | null;
  ESTIMATED_HOURS: number;
  NOTES: string;
  IS_BASELINE: boolean;
}
