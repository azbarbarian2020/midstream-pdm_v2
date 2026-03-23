import { NextRequest } from "next/server";
import { getRestConfig } from "@/lib/snowflake";

export async function POST(req: NextRequest) {
  const body = await req.json();
  let messageText = body.message;

  if (body.context) {
    messageText = `[Context: Asset ${body.context.asset_id}, Type: ${body.context.asset_type}, Predicted: ${body.context.predicted_class}, RUL: ${body.context.rul_days} days]\n\n${messageText}`;
  }

  if (body.as_of_ts) {
    const label = body.offset_label || body.as_of_ts;
    messageText = `[Time context: Viewing predictions as of ${body.as_of_ts} (${label}). Use this timestamp when querying fleet_analyst and plan_route.]\n\n${messageText}`;
  }

  const { baseUrl, headers, authMethod } = getRestConfig();
  const url = `${baseUrl}/api/v2/databases/PDM_DEMO/schemas/APP/agents/PDM_AGENT:run`;
  console.log(`[Agent] POST ${url}`);
  console.log(`[Agent] Auth method: ${authMethod}, Role: ${headers["X-Snowflake-Role"]}`);
  console.log(`[Agent] Auth header: ${headers.Authorization?.substring(0, 30)}...`);

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify({
        messages: [
          {
            role: "user",
            content: [{ type: "text", text: messageText }],
          },
        ],
      }),
    });

    console.log(`[Agent] Response status: ${resp.status}`);

    if (!resp.ok) {
      const errText = await resp.text();
      console.error(`[Agent] Error body: ${errText}`);
      return new Response(JSON.stringify({ error: `Agent error: ${resp.status}`, detail: errText }), {
        status: resp.status,
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response(resp.body, {
      headers: { "Content-Type": "text/event-stream" },
    });
  } catch (err) {
    console.error(`[Agent] Fetch error:`, err);
    return new Response(JSON.stringify({ error: "Agent fetch failed", detail: String(err) }), {
      status: 502,
      headers: { "Content-Type": "application/json" },
    });
  }
}
