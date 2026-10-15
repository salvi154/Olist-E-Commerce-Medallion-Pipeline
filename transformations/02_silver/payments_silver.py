from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_or_fail("rule1",'order_id IS NOT NULL')
@dp.expect_or_drop("rule2",'payment_value > 0')
@dp.table(name="silver.payments_silver")
def payments_silver():
    return (
        spark.readStream.table('olist_medallion.bronze.payments_bronze')
        .dropDuplicates(['order_id','payment_sequential'])
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("payment_sequential",F.col("payment_sequential").cast("int"))
        .withColumn("payment_installments",F.col("payment_installments").cast("int"))
        .withColumn("payment_value",F.col("payment_value").cast("double"))
    )