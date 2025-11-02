import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"
# D:\Tourist\spark_jobs\diagnose_jdbc.py
from utils import spark, get_env, jdbc_url, jdbc_props
from pyspark.sql import functions as F

env = get_env()
print("[ENV]", env)

sp = spark()
print("[CONF] spark.jars.packages =", sp.sparkContext.getConf().get("spark.jars.packages"))

url  = jdbc_url(env); props = jdbc_props(env)

# 1) Просте читання з БД (без таблиць) — SELECT 1
df = (sp.read.format("jdbc")
      .option("url", url)
      .option("query", "select 1 as ok")
      .options(**props)
      .load())
print("[READ] select 1 ->", df.collect())

# 2) Пробне створення/запис у core.test_spark
mini = sp.createDataFrame([(1, "hello")], ["id", "txt"])
(mini.write.format("jdbc")
     .option("url", url)
     .option("dbtable", "core.test_spark")
     .options(**props)
     .mode("overwrite")
     .save())
print("[WRITE] wrote core.test_spark OK")
