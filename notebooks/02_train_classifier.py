# %% [markdown]
# # PDM Failure Classification Model
# 
# Train XGBoost classifier to predict failure mode from sensor features.
# Register model in Snowflake ML Registry.

# %%
from snowflake.snowpark import Session
from snowflake.ml.modeling.xgboost import XGBClassifier
from snowflake.ml.registry import Registry
from snowflake.ml.modeling.metrics import accuracy_score, f1_score
import os

# Connect
connection_params = {"connection_name": os.getenv("SNOWFLAKE_CONNECTION_NAME", "jdrew")}
session = Session.builder.configs(connection_params).create()
print(f"Connected: {session.get_current_account()}")

# %% [markdown]
# ## Load Features

# %%
# Feature columns for classification
feature_cols = [
    'vibration', 'temperature', 'pressure', 'rpm', 'oil_pressure', 'power_draw',
    'flow_rate', 'differential_pressure', 'compression_ratio',
    'vibration_mean_1h', 'vibration_std_1h', 'vibration_mean_24h', 'vibration_std_24h',
    'vibration_max_24h', 'vibration_roc_1h',
    'temperature_mean_1h', 'temperature_std_1h', 'temperature_mean_24h', 
    'temperature_max_24h', 'temperature_roc_1h',
    'pressure_mean_24h', 'pressure_std_24h', 'pressure_min_24h',
    'oil_pressure_mean_24h', 'oil_pressure_roc_1h',
    'power_draw_mean_24h',
    'diff_pressure_max_24h', 'diff_pressure_std_24h',
    'comp_ratio_std_24h', 'flow_rate_mean_24h',
    'vibration_pct_baseline', 'temperature_pct_baseline', 
    'pressure_pct_baseline', 'oil_pressure_pct_baseline'
]

target_col = 'failure_mode'

# Load data
features_df = session.table("PDM_DEMO.ML.FEATURES")

# Train/test split (already marked in data)
train_df = features_df.filter(features_df["is_train"] == 1)
test_df = features_df.filter(features_df["is_train"] == 0)

print(f"Train rows: {train_df.count()}, Test rows: {test_df.count()}")

# %% [markdown]
# ## Train XGBoost Classifier

# %%
# Initialize classifier
classifier = XGBClassifier(
    input_cols=feature_cols,
    label_cols=[target_col],
    output_cols=["predicted_class"],
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
    random_state=42
)

# Train
classifier.fit(train_df)
print("Training complete")

# %% [markdown]
# ## Evaluate Model

# %%
# Predict on test set
predictions = classifier.predict(test_df)

# Get accuracy
predictions_pd = predictions.select(target_col, "predicted_class").to_pandas()
accuracy = (predictions_pd[target_col] == predictions_pd["predicted_class"]).mean()
print(f"Test Accuracy: {accuracy:.4f}")

# Class distribution
print("\nPrediction distribution:")
print(predictions_pd["predicted_class"].value_counts())

# %% [markdown]
# ## Register Model

# %%
# Get registry
registry = Registry(session, database_name="PDM_DEMO", schema_name="ML")

# Sample input for schema inference
sample_df = train_df.select(feature_cols).limit(10)

# Log model
model_ref = registry.log_model(
    model=classifier,
    model_name="PDM_CLASSIFIER",
    version_name="v1",
    sample_input_data=sample_df,
    metrics={"accuracy": float(accuracy)},
    comment="XGBoost classifier for failure mode prediction (6 classes)"
)

print(f"Model registered: {model_ref.name} version {model_ref.version}")

# %%
session.close()
print("Done!")
