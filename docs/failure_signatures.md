# PDM Failure Signatures Design

## Sensor Baseline Ranges (from existing data)

| Sensor | Normal Min | Normal Mean | Normal Max | Units |
|--------|-----------|-------------|------------|-------|
| VIBRATION | 2.0 | 3.5 | 5.0 | mm/s |
| TEMPERATURE | 160 | 180 | 200 | °F |
| PRESSURE | 450 | 520 | 600 | PSI |
| RPM | 2400 | 2500 | 2600 | rpm |
| OIL_PRESSURE | 50 | 60 | 70 | PSI |
| POWER_DRAW | 250 | 295 | 340 | kW |
| FLOW_RATE | 800 | 1000 | 1200 | gpm |
| DIFFERENTIAL_PRESSURE | 15 | 25 | 35 | PSI |
| COMPRESSION_RATIO | 2.8 | 3.0 | 3.2 | ratio |

## Failure Signature Definitions

### 1. NORMAL
- All sensors within baseline ranges
- Low variance (random noise only)
- No trends
- RUL: 999 (effectively infinite)

### 2. BEARING_WEAR
Primary indicator: VIBRATION increases progressively
- Vibration: 3.5 → 12 mm/s over 14 days (linear increase)
- Temperature: 180 → 210°F (slight increase from friction)
- Oil_Pressure: 60 → 45 PSI (gradual decrease)
- RPM: stable
- Power_Draw: slight increase (+10%)

Degradation curve: Linear
Detection point: ~Day 5 (vibration crosses 6 mm/s threshold)
Failure point: Day 14 (vibration > 10 mm/s)

### 3. OVERHEATING
Primary indicator: TEMPERATURE rises sharply
- Temperature: 180 → 260°F (exponential last 3 days)
- Power_Draw: 295 → 400 kW (increases to compensate)
- RPM: 2500 → 2300 (decreases as system struggles)
- Vibration: slight increase
- Oil_Pressure: drops as oil thins

Degradation curve: Exponential acceleration in final 72h
Detection point: ~Day 7 (temp crosses 220°F)
Failure point: Day 12 (temp > 250°F)

### 4. SEAL_LEAK
Primary indicator: PRESSURE variance and drops
- Pressure: 520 → 380 PSI (step decreases)
- Flow_Rate: 1000 → 700 gpm (decreases with pressure)
- Temperature: slight increase (from inefficiency)
- Vibration: slight increase
- Variance: pressure readings become erratic

Degradation curve: Step changes (each step = seal degradation)
Detection point: ~Day 6 (first pressure step down)
Failure point: Day 13 (pressure critically low)

### 5. VALVE_FAILURE
Primary indicator: DIFFERENTIAL_PRESSURE spikes
- Differential_Pressure: 25 → spikes to 60+ PSI intermittently
- Flow_Rate: becomes erratic
- Pressure: swings ±100 PSI
- RPM: compensatory swings

Degradation curve: Intermittent spikes become constant
Detection point: ~Day 5 (first spike pattern)
Failure point: Day 11 (stuck valve, constant high diff pressure)

### 6. SURGE (Compressor-specific)
Primary indicator: COMPRESSION_RATIO oscillations
- Compression_Ratio: 3.0 → oscillates 2.2-4.0
- RPM: rapid oscillations
- Pressure: rapid oscillations
- Temperature: rapid swings
- Power_Draw: spikes during surge events

Degradation curve: Rapid deterioration in final 48h
Detection point: ~Day 4 (first surge event)
Failure point: Day 9 (continuous surge)

## Asset Allocation (100 total)

| Asset IDs | Class | Count | Failure Day Range |
|-----------|-------|-------|-------------------|
| 1-50 | NORMAL | 50 | N/A |
| 51-60 | BEARING_WEAR | 10 | Days 10-14 |
| 61-70 | OVERHEATING | 10 | Days 9-13 |
| 71-80 | SEAL_LEAK | 10 | Days 10-14 |
| 81-90 | VALVE_FAILURE | 10 | Days 8-12 |
| 91-100 | SURGE | 10 | Days 7-11 |

## Train/Test Split (by asset_id)

Train (80 assets):
- Normal: 1-40
- Bearing_Wear: 51-58
- Overheating: 61-68
- Seal_Leak: 71-78
- Valve_Failure: 81-88
- Surge: 91-98

Test (20 assets):
- Normal: 41-50
- Bearing_Wear: 59-60
- Overheating: 69-70
- Seal_Leak: 79-80
- Valve_Failure: 89-90
- Surge: 99-100

## Demo Asset Mapping

Test assets will be mapped to existing demo asset IDs:

| Test Asset | Failure Type | Demo Asset ID | Station | At Now (T-7d) | At +7d (T) |
|------------|--------------|---------------|---------|---------------|------------|
| 59 | BEARING_WEAR | 27 | Andrews Booster | RED (7d) | OFFLINE |
| 69 | OVERHEATING | 41 | Stanton Hub | YELLOW (12d) | RED (5d) |
| 79 | SEAL_LEAK | 34 | Midland Junction | YELLOW (18d) | YELLOW (11d) |
| 89 | VALVE_FAILURE | 12 | Kermit Junction | YELLOW (22d) | YELLOW (15d) |
| 99 | SURGE | 8 | Big Spring | YELLOW (9d) | RED (2d) |
| 50 | NORMAL→YELLOW | 18 | Pecos Gathering | GREEN (35d) | YELLOW (28d) |

Asset 27 (hero): Bearing wear, fails exactly at T (day 14), so at T-7d has RUL=7, at T has RUL=0.
