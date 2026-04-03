# Troubleshooting Guide

## Service Won't Start

### Compute Pool Not Active
```sql
DESCRIBE COMPUTE POOL PDM_DEMO_POOL;
-- If status is IDLE or SUSPENDED:
ALTER COMPUTE POOL PDM_DEMO_POOL RESUME;
```

### Check Service Logs
```sql
SELECT SYSTEM$GET_SERVICE_LOGS('PDM_DEMO.APP.PDM_FRONTEND', 0, 'frontend', 100);
```

### Service Status Details
```sql
SELECT SYSTEM$GET_SERVICE_STATUS('PDM_DEMO.APP.PDM_FRONTEND');
```

## Authentication Issues

### JWT Token is Invalid (390144)
The JWT claims (`iss`, `sub`) must use the **account LOCATOR** (e.g. `LNB24417`), not the org-account format (e.g. `SFSENORTHAMERICA-CLEANBARBARIAN`).

Get your locator:
```sql
SELECT CURRENT_ACCOUNT();
```

The setup script auto-detects this and passes it as `SNOWFLAKE_ACCOUNT_LOCATOR` in the service YAML. If you see this error, re-run `./setup.sh`.

### Key-Pair Authentication Failure
Verify the public key is assigned:
```sql
DESCRIBE USER <username>;
-- Check RSA_PUBLIC_KEY is set
```

Verify the fingerprint matches:
```bash
openssl rsa -in ~/.snowflake/keys/<connection>.p8 -pubout -outform DER 2>/dev/null | openssl dgst -sha256 -binary | openssl enc -base64
```
Compare with:
```sql
DESCRIBE USER <username>;
-- RSA_PUBLIC_KEY_FP should match SHA256:<fingerprint>
```

Regenerate if needed:
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out ~/.snowflake/keys/<connection>.p8
openssl rsa -in ~/.snowflake/keys/<connection>.p8 -pubout -out /tmp/key.pub
PUBLIC_KEY=$(grep -v 'BEGIN\|END' /tmp/key.pub | tr -d '\n')
```
```sql
ALTER USER <username> SET RSA_PUBLIC_KEY='<PUBLIC_KEY>';
```

### CLI Connection Test Fails
If `snow connection test` fails with a warehouse error after teardown:
```sql
ALTER USER <username> UNSET DEFAULT_WAREHOUSE;
```

Or test with a simple query instead:
```bash
snow sql --connection <conn> -q "SELECT CURRENT_USER()"
```

## Cortex Agent Errors

### "External access integration not found"
The service must be created with the integration:
```sql
ALTER SERVICE PDM_DEMO.APP.PDM_FRONTEND
  SET EXTERNAL_ACCESS_INTEGRATIONS = (PDM_CORTEX_EXTERNAL_ACCESS, PDM_DEMO_EXTERNAL_ACCESS);
```

### "Network rule host mismatch"
The `SNOWFLAKE_API_RULE` must match your account's host:
```sql
ALTER NETWORK RULE PDM_DEMO.APP.SNOWFLAKE_API_RULE
  SET VALUE_LIST = ('<your-org>-<your-account>.snowflakecomputing.com:443');
```

### Agent Returning Empty Responses
Verify the semantic view and search service exist:
```sql
SHOW SEMANTIC VIEWS IN SCHEMA PDM_DEMO.APP;
SHOW CORTEX SEARCH SERVICES IN SCHEMA PDM_DEMO.APP;
DESCRIBE AGENT PDM_DEMO.APP.PDM_AGENT;
```

## Data Issues

### TIMESTAMP_NTZ vs TIMESTAMP_LTZ
All timestamp comparisons in this demo use `TIMESTAMP_NTZ`. If you see incorrect results (e.g., wrong risk levels), ensure timestamps are explicitly cast:
```sql
-- CORRECT:
WHERE AS_OF_TS <= '2026-03-13T00:00:00'::TIMESTAMP_NTZ

-- WRONG (implicit LTZ conversion causes timezone offset):
WHERE AS_OF_TS <= '2026-03-13T00:00:00'
```

### Missing Predictions
If the fleet dashboard shows no data:
```sql
SELECT COUNT(*) FROM PDM_DEMO.ANALYTICS.PREDICTIONS;
-- Should be ~11,250 rows
-- If 0, re-run:
-- SNOWFLAKE_CONNECTION_NAME=<conn> python3 snowflake/score_fleet.py
```

### RUL Display Rounding
All RUL values display with 1 decimal place (e.g., 7.5d not 8d) for consistency across map, cards, and detail pages.

## Docker Issues

### Build Fails on ARM Mac
Always build for linux/amd64:
```bash
docker buildx build --platform linux/amd64 -t <tag> -f frontend/Dockerfile frontend --load
```

### Push Unauthorized
Re-authenticate with the registry:
```bash
snow spcs image-registry login --connection <conn>
```

### Image Not Found by Service
Verify the image path in your service YAML matches what was pushed:
```sql
SHOW IMAGES IN IMAGE REPOSITORY PDM_DEMO.APP.PDM_REPO;
```

## Network Issues

### Map Tiles Not Loading
Ensure the OSM external access integration exists:
```sql
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'PDM_DEMO%';
-- Should see PDM_DEMO_EXTERNAL_ACCESS with OSM_TILES_RULE
```

### Cortex REST API Timeouts
Ensure the S3 result rule allows response retrieval:
```sql
DESCRIBE NETWORK RULE PDM_DEMO.APP.S3_RESULT_RULE;
-- Should show: *.s3.*.amazonaws.com:443
```

## Complete Reset

If something is irreversibly broken, tear down and reinstall:
```bash
./teardown.sh
./setup.sh
```
