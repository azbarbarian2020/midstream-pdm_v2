import snowflake from "snowflake-sdk";
import jwt from "jsonwebtoken";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import os from "os";

snowflake.configure({ logLevel: "ERROR" });

let _connection: snowflake.Connection | null = null;

const SNOWFLAKE_HOST = process.env.SNOWFLAKE_HOST || "sfsenorthamerica-jdrew.snowflakecomputing.com";
const SNOWFLAKE_ACCOUNT = process.env.SNOWFLAKE_ACCOUNT || "SFSENORTHAMERICA-JDREW";

function getPrivateKey(): string | null {
  const raw = process.env.SNOWFLAKE_PRIVATE_KEY;
  if (raw) return raw.replace(/\\n/g, "\n");
  return null;
}

function readConnectionConfig(): Record<string, string> {
  const tomlPath = path.join(os.homedir(), ".snowflake", "connections.toml");
  if (!fs.existsSync(tomlPath)) return {};
  const raw = fs.readFileSync(tomlPath, "utf-8");
  const connName = process.env.SNOWFLAKE_CONNECTION_NAME || "jdrew";
  const lines = raw.split("\n");
  let inSection = false;
  const cfg: Record<string, string> = {};
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.startsWith("[")) {
      inSection = trimmed === `[${connName}]`;
      continue;
    }
    if (inSection && trimmed.includes("=")) {
      const [key, ...rest] = trimmed.split("=");
      cfg[key.trim()] = rest.join("=").trim().replace(/^["']|["']$/g, "");
    }
  }
  return cfg;
}

function generateJwtToken(): string | null {
  const privateKey = getPrivateKey();
  if (!privateKey) return null;

  const username = (process.env.SNOWFLAKE_USER || "ADMIN").toUpperCase();

  const accountLocator = process.env.SNOWFLAKE_ACCOUNT_LOCATOR;
  let qualifiedAccount: string;
  if (accountLocator) {
    qualifiedAccount = accountLocator.toUpperCase();
  } else {
    const accountParts = SNOWFLAKE_ACCOUNT.toUpperCase().split("-");
    qualifiedAccount = accountParts.length >= 2
      ? `${accountParts[0]}.${accountParts.slice(1).join(".")}`
      : SNOWFLAKE_ACCOUNT.toUpperCase();
  }

  const pubKeyDer = crypto.createPublicKey(privateKey).export({ type: "spki", format: "der" });
  const fingerprint = crypto.createHash("sha256").update(pubKeyDer).digest("base64");
  const qualifiedUsername = `${qualifiedAccount}.${username}`;

  const now = Math.floor(Date.now() / 1000);
  const payload = {
    iss: `${qualifiedUsername}.SHA256:${fingerprint}`,
    sub: qualifiedUsername,
    iat: now,
    exp: now + 3600,
  };

  return jwt.sign(payload, privateKey, { algorithm: "RS256" });
}

function getConnectionConfig(): snowflake.ConnectionOptions {
  const baseConfig = {
    account: SNOWFLAKE_ACCOUNT,
    host: SNOWFLAKE_HOST,
    username: process.env.SNOWFLAKE_USER || "ADMIN",
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || "PDM_DEMO_WH",
    database: process.env.SNOWFLAKE_DATABASE || "PDM_DEMO",
    schema: process.env.SNOWFLAKE_SCHEMA || "APP",
  };

  const privateKey = getPrivateKey();
  if (privateKey) {
    console.log("Using Key-Pair JWT authentication");
    return {
      ...baseConfig,
      authenticator: "SNOWFLAKE_JWT",
      privateKey: privateKey,
    };
  }

  const spcsTokenPath = "/snowflake/session/token";
  try {
    if (fs.existsSync(spcsTokenPath)) {
      const token = fs.readFileSync(spcsTokenPath, "utf8").trim();
      console.log("Using SPCS OAuth authentication (token found)");
      return {
        ...baseConfig,
        authenticator: "OAUTH",
        token: token,
      };
    }
  } catch (e) {
    console.log("Error reading SPCS token:", (e as Error).message);
  }

  console.log("WARNING: No authentication method available");
  return baseConfig;
}

function isRetryableError(err: unknown): boolean {
  const error = err as { message?: string; code?: number };
  return !!(
    error.message?.includes("OAuth access token expired") ||
    error.message?.includes("terminated connection") ||
    error.code === 407002
  );
}

function getConnection(): Promise<snowflake.Connection> {
  return new Promise((resolve, reject) => {
    if (_connection) {
      resolve(_connection);
      return;
    }

    const conn = snowflake.createConnection(getConnectionConfig());
    conn.connect((err) => {
      if (err) {
        console.error("Connection error:", err.message);
        reject(err);
        return;
      }
      _connection = conn;
      console.log("Connected to Snowflake");
      resolve(conn);
    });
  });
}

export async function query<T = Record<string, unknown>>(
  sql: string,
  binds: snowflake.Binds = [],
  retries = 1
): Promise<T[]> {
  try {
    const conn = await getConnection();
    return await new Promise((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        binds: binds as snowflake.Binds,
        complete: (err, _stmt, rows) => {
          if (err) {
            reject(err);
            return;
          }
          resolve((rows || []) as T[]);
        },
      });
    });
  } catch (err) {
    console.error("Query error:", (err as Error).message);
    if (retries > 0 && isRetryableError(err)) {
      _connection = null;
      return query(sql, binds, retries - 1);
    }
    throw err;
  }
}

export function getRestConfig(): { baseUrl: string; headers: Record<string, string> } {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json",
  };

  const jwtToken = generateJwtToken();
  if (jwtToken) {
    headers["Authorization"] = `Bearer ${jwtToken}`;
    headers["X-Snowflake-Authorization-Token-Type"] = "KEYPAIR_JWT";
    return { baseUrl: `https://${SNOWFLAKE_HOST}`, headers };
  }

  try {
    if (fs.existsSync("/snowflake/session/token")) {
      const token = fs.readFileSync("/snowflake/session/token", "utf8").trim();
      headers["Authorization"] = `Snowflake Token="${token}"`;
      return { baseUrl: `https://${SNOWFLAKE_HOST}`, headers };
    }
  } catch { }

  console.log("WARNING: No auth token available for REST API");
  return { baseUrl: `https://${SNOWFLAKE_HOST}`, headers };
}
