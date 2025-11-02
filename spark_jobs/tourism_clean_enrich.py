from pyspark.sql import functions as F
from utils import spark, get_env, jdbc_url, jdbc_props

def main():
    env = get_env()
    sp = spark()

    url = jdbc_url(env); props = jdbc_props(env)

    t = (sp.read.format("jdbc")
        .option("url", url)
        .option("dbtable", "staging.tourism_monthly")
        .options(**props)
        .load())

    t = (t.dropDuplicates(["period_month"])
           .withColumn("month_start", F.to_date("period_month"))
           .withColumn("region", F.coalesce(F.col("region"), F.lit("Drohobych")))
           .withColumn("tourists_total", F.col("tourists_total").cast("int"))
           .withColumn("female_count", F.col("female_count").cast("int"))
           .withColumn("male_count", F.col("male_count").cast("int"))
           .withColumn("year", F.year("month_start"))
           .withColumn("month", F.month("month_start"))
        )

    # обчислимо частки (де total > 0)
    t = (t.withColumn("female_share", F.when(F.col("tourists_total") > 0,
                                             F.col("female_count")/F.col("tourists_total")).otherwise(None))
           .withColumn("male_share",   F.when(F.col("tourists_total") > 0,
                                             F.col("male_count")/F.col("tourists_total")).otherwise(None))
        )

    t.cache()

    (t.write.format("jdbc")
        .option("url", url)
        .option("dbtable", "core.tourism_monthly_clean")
        .options(**props)
        .mode("overwrite")
        .save())

    print(f"[SPARK] Wrote rows: {t.count()} to core.tourism_monthly_clean")

if __name__ == "__main__":
    main()
