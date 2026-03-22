-- ============================================================================
-- Cortex Agent - Midstream PDM Assistant
-- Requires: Cortex Search service, Semantic View, Route Planner SP
-- Run AFTER cortex_services.sql and route_planner_sp.sql
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE PDM_DEMO;
USE WAREHOUSE PDM_DEMO_WH;

CREATE OR REPLACE AGENT APP.PDM_AGENT
  COMMENT = 'Predictive maintenance assistant for midstream pipeline operations'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "claude-4-sonnet"
    },
    "instructions": {
      "orchestration": "You are a predictive maintenance expert for midstream oil and gas operations in the Permian Basin. You help field engineers and operations managers make data-driven maintenance decisions.\n\nUse fleet_analyst for quantitative questions: asset counts, RUL averages, risk distributions, sensor trends, maintenance costs, and comparisons.\n\nUse manual_search for maintenance procedures, troubleshooting guides, safety information, API standards, and historical maintenance logs. IMPORTANT: When diagnosing a specific asset, ALWAYS filter manual_search by the asset's model name (e.g. 'Grundfos CRN', 'Ariel JGK/4') to find the correct manual for that specific equipment. Each model has its own procedures and parts.\n\nUse plan_route when asked about service routing, bundling work orders, what else to service nearby, or optimizing technician dispatch.\n\nAlways ground recommendations in data from these tools. When discussing an asset, include its current risk level and predicted failure mode.\n\nMODEL-SPECIFIC DIAGNOSIS: The fleet has 8 equipment models: PUMP models (Flowserve HPRT, Grundfos CRN, Sulzer MSD, Sundyne LMV-311) and COMPRESSOR models (Ariel JGK/4, Atlas Copco GA-90, Dresser-Rand DATUM, Ingersoll Rand Centac). Always reference the correct manual for the asset's specific model when providing maintenance guidance.\n\nFAILURE-MODE SENSOR MAPPING: When explaining predictions, reference the key contributing sensors:\n- BEARING_WEAR: vibration, temperature, RPM, oil pressure\n- VALVE_FAILURE: pressure, flow rate, differential pressure, temperature\n- SEAL_LEAK: pressure, seal temperature, vibration, suction pressure\n- OVERHEATING: temperature, discharge temp, inlet temp, power draw\n- SURGE: flow rate, pressure, cavitation index, compression ratio, RPM\n\nCO-REPLACEMENT INTELLIGENCE: When recommending parts replacement, consider co-occurring wear patterns from maintenance history. For example, bearing replacements typically coincide with seal wear and lubrication system service. Valve work often requires gasket sets and actuator inspection. Always suggest bundled parts to avoid repeat visits. Use manual_search to find co-replacement patterns in maintenance logs.",
      "response": "Be concise and actionable. Structure recommendations as:\n1. Key signals from telemetry and predictions (cite specific sensor values and which are outside nominal bounds)\n2. Relevant manual excerpts with source citations (reference the specific equipment model manual)\n3. Clear action list with parts needed (include co-replacement suggestions — e.g. 'Part X typically wears at the same time as Part Y, suggest replacing both')\n4. Safety cautions if applicable\n\nUse technical language appropriate for field engineers. Include specific sensor values and thresholds when available. When suggesting parts, always consider what else typically needs replacement at the same time based on maintenance history."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "fleet_analyst",
          "description": "Query fleet KPIs, asset predictions, maintenance history, and telemetry metrics using natural language. Use for questions about asset counts, risk levels, RUL values, sensor trends, maintenance costs, station comparisons, and fleet summaries. Returns structured data from SQL queries."
        }
      },
      {
        "tool_spec": {
          "type": "cortex_search",
          "name": "manual_search",
          "description": "Search pump and compressor operating manuals, maintenance procedures, troubleshooting guides, safety checklists, and historical maintenance logs. Returns relevant excerpts from API 610 (pump) and API 618 (compressor) standards, along with past maintenance records. Use when asked about how to fix something, safety procedures, parts lists, or maintenance best practices."
        }
      },
      {
        "tool_spec": {
          "type": "generic",
          "name": "plan_route",
          "description": "Plan an optimized service route for a technician. Given a primary asset to visit, finds nearby at-risk assets to bundle into the trip based on predicted risk and geographic proximity. Returns an ordered stop list with reasons, parts needed, and distances. Use when asked about service routing, bundling work, or what other assets to check while visiting a site.",
          "input_schema": {
            "type": "object",
            "properties": {
              "tech_id": {
                "type": "string",
                "description": "Technician ID (e.g. TECH-001)"
              },
              "primary_asset_id": {
                "type": "integer",
                "description": "The primary asset ID to visit"
              },
              "horizon_days": {
                "type": "integer",
                "description": "Planning horizon in days. Default 3."
              },
              "max_stops": {
                "type": "integer",
                "description": "Maximum number of stops on the route. Default 5."
              }
            },
            "required": ["tech_id", "primary_asset_id"]
          }
        }
      }
    ],
    "tool_resources": {
      "fleet_analyst": {
        "semantic_view": "PDM_DEMO.APP.FLEET_SEMANTIC_VIEW",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "PDM_DEMO_WH"
        },
        "query_timeout": 60
      },
      "manual_search": {
        "search_service": "PDM_DEMO.APP.MANUAL_SEARCH",
        "max_results": 10,
        "columns": ["CONTENT", "TITLE", "ASSET_TYPE", "SECTION_TYPE"]
      },
      "plan_route": {
        "type": "procedure",
        "identifier": "PDM_DEMO.APP.PLAN_ROUTE",
        "execution_environment": {
          "type": "warehouse",
          "name": "PDM_DEMO_WH"
        }
      }
    }
  }
  $$;

GRANT USAGE ON AGENT PDM_DEMO.APP.PDM_AGENT TO ROLE DEMO_PDM_ADMIN;

SELECT 'Cortex Agent PDM_AGENT created.' AS STATUS;
