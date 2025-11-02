from pyspark.sql import functions as F
from utils import spark, get_env, jdbc_url, jdbc_props

def main():
    env = get_env()
    sp = spark()
    url = jdbc_url(env); props = jdbc_props(env)

    w = (sp.read.format("jdbc").option("url", url).option("dbtable", "core.weather_daily_clean").options(**props).load())
    t = (sp.read.format("jdbc").option("url", url).option("dbtable", "core.tourism_monthly_clean").options(**props).load())

    w_m = (w.groupBy("month_start")
             .agg(F.avg("avg_temp_c").alias("avg_temp_c"),
                  F.avg("humidity_avg").alias("humidity_avg"),
                  F.avg("wind_speed_avg_ms").alias("wind_speed_avg_ms"),
                  F.sum("precipitation_mm").alias("precipitation_mm_sum"))
          )

    fact = (t.join(w_m, on="month_start", how="left")
              .select(
                  "month_start", "region",
                  "tourists_total", "female_count", "male_count",
                  "female_share", "male_share",
                  "avg_temp_c", "humidity_avg", "wind_speed_avg_ms", "precipitation_mm_sum"
              ))

    fact.cache()

    (fact.write.format("jdbc")
        .option("url", url)
        .option("dbtable", "core.fact_tourism_weather")
        .options(**props)
        .mode("overwrite")
        .save())

    print(f"[SPARK] Wrote rows: {fact.count()} to core.fact_tourism_weather")

if __name__ == "__main__":
    main()
