import snowflake from "snowflake-sdk";
import crypto from "crypto";
import jwt from "jsonwebtoken";
import fs from "fs";
import path from "path";
import os from "os";

snowflake.configure({ logLevel: "ERROR" });

let _connection: snowflake.Connection | null = null;

const SNOWFLAKE_HOST = process.env.SNOWFLAKE_HOST || "";
const SNOWFLAKE_ACCOUNT = process.env.SNOWFLAKE_ACCOUNT || "";
const PAT_TOKEN = process.env.SNOWFLAKE_PAT || "";

let cachedJWT: string | null = null;
let jwtExpiresAt = 0;

function getPrivateKeyPem(): string | null {
  const raw = process.env.SNOWFLAKE_PRIVATE_KEY;
  if (!raw) return null;
  return raw.replace(/\\n/g, "\n");
}

function generateJWT(): string {
  const now = Math.floor(Date.now() / 1000);
  if (cachedJWT && now < jwtExpiresAt - 60) return cachedJWT;

  const privateKeyPem = getPrivateKeyPem();
  if (!privateKeyPem) throw new Error("SNOWFLAKE_PRIVATE_KEY not set");

  const privateKeyObj = crypto.createPrivateKey({ key: privateKeyPem, format: "pem" });
  const publicKeyObj = crypto.createPublicKey(privateKeyObj);
  const publicKeyDer = publicKeyObj.export({ type: "spki", format: "der" });
  const fingerprint = "SHA256:" + crypto.createHash("sha256").update(publicKeyDer).digest("base64");

  const account = SNOWFLAKE_ACCOUNT.toUpperCase();
  const user = (process.env.SNOWFLAKE_USER || "").toUpperCase();
  const qualifiedUser = `${account}.${user}`;

  const payload = {
    iss: `${qualifiedUser}.${fingerprint}`,
    sub: qualifiedUser,
    iat: now,
    exp: now + 3540,
  };

  cachedJWT = jwt.sign(payload, privateKeyPem, { algorithm: "RS256" });
  jwtExpiresAt = payload.exp;
  console.log("Generated Key-Pair JWT for REST API calls");
  return cachedJWT;
}

function readConnectionConfig(): Record<string, string> {
  const tomlPath = path.join(os.homedir(), ".snowflake", "connections.toml");
  if (!fs.existsSync(tomlPath)) return {};
  const raw = fs.readFileSync(tomlPath, "utf-8");
  const connName = process.env.SNOWFLAKE_CONNECTION_NAME || "default";
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

function getConnectionConfig(): snowflake.ConnectionOptions {
  const baseConfig = {
    account: SNOWFLAKE_ACCOUNT,
    host: SNOWFLAKE_HOST,
    username: process.env.SNOWFLAKE_USER || "ADMIN",
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || "PDM_DEMO_WH",
    database: process.env.SNOWFLAKE_DATABASE || "PDM_DEMO",
    schema: process.env.SNOWFLAKE_SCHEMA || "APP",
  };

  const privateKeyPem = getPrivateKeyPem();
  if (privateKeyPem) {
    console.log("Using Key-Pair JWT authentication");
    return {
      ...baseConfig,
      authenticator: "SNOWFLAKE_JWT",
      privateKey: privateKeyPem,
    };
  }

  if (PAT_TOKEN) {
    console.log("Using PAT authentication");
    return {
      ...baseConfig,
      password: PAT_TOKEN,
    };
  }

  const spcsTokenPath = "/snowflake/session/token";
  try {
    if (fs.existsSync(spcsTokenPath)) {
      const token = fs.readFileSync(spcsTokenPath, "utf8").trim();
      console.log("Using SPCS OAuth authentication (token found)");
      return {
        account: SNOWFLAKE_ACCOUNT,
        host: SNOWFLAKE_HOST,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE || "COMPUTE_WH",
        database: process.env.SNOWFLAKE_DATABASE || "PDM_DEMO",
        schema: process.env.SNOWFLAKE_SCHEMA || "APP",
        authenticator: "OAUTH",
        token: token,
      };
    } else {
      console.log("SPCS token file not found at:", spcsTokenPath);
    }
  } catch (e) {
    console.log("Error reading SPCS token:", (e as Error).message);
  }

  console.log("Falling back to no auth");
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

  const privateKeyPem = getPrivateKeyPem();
  if (privateKeyPem) {
    const jwtToken = generateJWT();
    headers["Authorization"] = `Bearer ${jwtToken}`;
    headers["X-Snowflake-Authorization-Token-Type"] = "KEYPAIR_JWT";
    return { baseUrl: `https://${SNOWFLAKE_HOST}`, headers };
  }

  if (PAT_TOKEN) {
    headers["Authorization"] = `Bearer ${PAT_TOKEN}`;
    headers["X-Snowflake-Authorization-Token-Type"] = "PROGRAMMATIC_ACCESS_TOKEN";
    return { baseUrl: `https://${SNOWFLAKE_HOST}`, headers };
  }

  try {
    if (fs.existsSync("/snowflake/session/token")) {
      const token = fs.readFileSync("/snowflake/session/token", "utf8").trim();
      headers["Authorization"] = `Snowflake Token="${token}"`;
      return { baseUrl: `https://${SNOWFLAKE_HOST}`, headers };
    }
  } catch {}

  return { baseUrl: `https://${SNOWFLAKE_HOST}`, headers };
}
