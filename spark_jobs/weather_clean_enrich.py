
from pyspark.sql import functions as F
from utils import spark, get_env, jdbc_url, jdbc_props

def main():
    env = get_env()
    sp = spark()
    url = jdbc_url(env); props = jdbc_props(env)

    weather = (sp.read.format("jdbc")
        .option("url", url)
        .option("dbtable", "staging.weather_daily")
        .options(**props)
        .load())

    w = (weather.dropDuplicates(["dt"])
         .withColumn("dt", F.to_date("dt"))
         .withColumn("avg_temp_c", F.col("avg_temp_c").cast("double"))
         .withColumn("min_temp_c", F.col("min_temp_c").cast("double"))
         .withColumn("max_temp_c", F.col("max_temp_c").cast("double"))
         .withColumn("humidity_avg", F.col("humidity_avg").cast("double"))
         .withColumn("wind_speed_avg_ms", F.col("wind_speed_avg_ms").cast("double"))
         .withColumn("precipitation_mm", F.col("precipitation_mm").cast("double"))
         .na.fill({"precipitation_mm": 0.0})
         .withColumn("month_start", F.trunc("dt", "month"))
         .withColumn("year", F.year("dt"))
         .withColumn("month", F.month("dt"))
         .withColumn("temp_bucket", F.when(F.col("avg_temp_c") < 0, F.lit("below_0"))
                                     .when(F.col("avg_temp_c") < 10, F.lit("0_10"))
                                     .when(F.col("avg_temp_c") < 20, F.lit("10_20"))
                                     .otherwise(F.lit("20_plus")))
        )
    w.cache()

    (w.write.format("jdbc")
        .option("url", url)
        .option("dbtable", "core.weather_daily_clean")
        .options(**props)
        .mode("overwrite")
        .save())

    print(f"[SPARK] Wrote rows: {w.count()} to core.weather_daily_clean")

if __name__ == "__main__":
    main()
