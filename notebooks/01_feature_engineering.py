# %% [markdown]
# # PDM Feature Engineering
# 
# This notebook creates rolling statistics and rate-of-change features from telemetry data
# for training failure classification and RUL regression models.

# %%
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window
import os

# Connect to Snowflake
connection_params = {
    "connection_name": os.getenv("SNOWFLAKE_CONNECTION_NAME", "jdrew")
}
session = Session.builder.configs(connection_params).create()
print(f"Connected to: {session.get_current_account()}")

# %% [markdown]
# ## Load Training Data

# %%
# Load training data
training_df = session.table("PDM_DEMO.ML.TRAINING_DATA")
print(f"Total rows: {training_df.count()}")
training_df.limit(5).show()

# %% [markdown]
# ## Feature Engineering
# 
# We'll create the following features:
# - Rolling statistics (mean, std, min, max) over 1h, 6h, 24h windows
# - Rate of change (current vs 1h ago, 6h ago)
# - Sensor value normalized to baseline

# %%
# Define window specifications
# 1 hour = 12 rows (5-min intervals), 6 hours = 72 rows, 24 hours = 288 rows
window_1h = Window.partition_by("asset_id").order_by("epoch").rows_between(-12, 0)
window_6h = Window.partition_by("asset_id").order_by("epoch").rows_between(-72, 0)
window_24h = Window.partition_by("asset_id").order_by("epoch").rows_between(-288, 0)

# Key sensors for features
sensors = ['vibration', 'temperature', 'pressure', 'rpm', 'oil_pressure', 'power_draw', 
           'flow_rate', 'differential_pressure', 'compression_ratio']

# Start with base columns
feature_df = training_df.select(
    F.col("asset_id"),
    F.col("ts"),
    F.col("epoch"),
    F.col("failure_mode"),
    F.col("rul_days"),
    F.col("is_train"),
    # Raw sensor values
    *[F.col(s) for s in sensors]
)

# Add rolling features for each sensor
for sensor in sensors:
    # 1-hour rolling stats
    feature_df = feature_df.with_column(
        f"{sensor}_mean_1h", F.avg(F.col(sensor)).over(window_1h)
    ).with_column(
        f"{sensor}_std_1h", F.stddev(F.col(sensor)).over(window_1h)
    )
    
    # 6-hour rolling stats
    feature_df = feature_df.with_column(
        f"{sensor}_mean_6h", F.avg(F.col(sensor)).over(window_6h)
    ).with_column(
        f"{sensor}_std_6h", F.stddev(F.col(sensor)).over(window_6h)
    )
    
    # 24-hour rolling stats
    feature_df = feature_df.with_column(
        f"{sensor}_mean_24h", F.avg(F.col(sensor)).over(window_24h)
    ).with_column(
        f"{sensor}_std_24h", F.stddev(F.col(sensor)).over(window_24h)
    ).with_column(
        f"{sensor}_min_24h", F.min(F.col(sensor)).over(window_24h)
    ).with_column(
        f"{sensor}_max_24h", F.max(F.col(sensor)).over(window_24h)
    )

print(f"Feature columns: {len(feature_df.columns)}")

# %% [markdown]
# ## Add Rate of Change Features

# %%
# Rate of change: current value vs lagged value
window_lag = Window.partition_by("asset_id").order_by("epoch")

for sensor in sensors:
    # Value 1 hour ago
    feature_df = feature_df.with_column(
        f"{sensor}_lag_1h", F.lag(F.col(sensor), 12).over(window_lag)
    )
    # Rate of change per hour
    feature_df = feature_df.with_column(
        f"{sensor}_roc_1h", 
        (F.col(sensor) - F.col(f"{sensor}_lag_1h")) / F.lit(1.0)
    )
    # Drop the lag column
    feature_df = feature_df.drop(f"{sensor}_lag_1h")

# %% [markdown]
# ## Add Normalized Features (relative to baseline)

# %%
# Baselines from design doc
baselines = {
    'vibration': 3.5,
    'temperature': 180,
    'pressure': 520,
    'rpm': 2500,
    'oil_pressure': 60,
    'power_draw': 295,
    'flow_rate': 1000,
    'differential_pressure': 25,
    'compression_ratio': 3.0,
}

for sensor, baseline in baselines.items():
    feature_df = feature_df.with_column(
        f"{sensor}_pct_baseline", 
        (F.col(sensor) - F.lit(baseline)) / F.lit(baseline) * 100
    )

# %% [markdown]
# ## Save Features Table

# %%
# Write to FEATURES table
feature_df.write.mode("overwrite").save_as_table("PDM_DEMO.ML.FEATURES")
print("Features saved to PDM_DEMO.ML.FEATURES")

# Verify
result = session.sql("SELECT COUNT(*) as cnt FROM PDM_DEMO.ML.FEATURES").collect()
print(f"Rows saved: {result[0]['CNT']}")

# Show sample
session.table("PDM_DEMO.ML.FEATURES").limit(3).show()

# %%
session.close()
print("Done!")
