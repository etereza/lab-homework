import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# --- 1) Завантажуємо .env з кореня проєкту ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(ENV_PATH)

def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"ENV var {name} is missing. Check {ENV_PATH}")
    return val

def get_env():
    return {
        "PG_HOST":      _require("PG_HOST"),
        "PG_PORT":      int(os.getenv("PG_PORT", "5432")),
        "PG_DB":        _require("PG_DB"),
        "PG_USER":      _require("PG_USER"),
        "PG_PASSWORD":  _require("PG_PASSWORD"),
        "PG_SSLMODE":   os.getenv("PG_SSLMODE", "require"),
    }

def spark():
    # --- 2) Підтягуємо JDBC драйвер Postgres прямо з maven ---
    return (
        SparkSession.builder
        .appName("TourismWeatherETL")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

def jdbc_url(env):
    # --- 3) Формуємо валідний JDBC URL ---
    params = f"sslmode={env['PG_SSLMODE']}"
    return f"jdbc:postgresql://{env['PG_HOST']}:{env['PG_PORT']}/{env['PG_DB']}?{params}"

def jdbc_props(env):
    return {
        "user": env["PG_USER"],
        "password": env["PG_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }
