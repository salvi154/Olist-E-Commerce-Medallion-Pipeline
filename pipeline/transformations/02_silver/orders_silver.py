from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_or_fail("rule1",'order_id IS NOT NULL and customer_id IS NOT NULL')
@dp.expect_or_drop("rule3",'order_purchase_timestamp IS NOT NULL')
@dp.expect("rule4","order_status IN ('delivered', 'invoiced', 'processing', 'shipped','unavailable','canceled','created','approved')")
@dp.table(name="silver.orders_silver")
def orders_silver():
    return (
        spark.readStream.table("olist_medallion.bronze.orders_bronze")
        .dropDuplicates(["order_id"])
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("order_purchase_timestamp",F.col("order_purchase_timestamp").cast("timestamp"))
        .withColumn("order_approved_at",F.col("order_approved_at").cast("timestamp"))
        .withColumn("order_delivered_carrier_date",F.col("order_delivered_carrier_date").cast("timestamp"))
        .withColumn("order_delivered_customer_date",F.col("order_delivered_customer_date").cast("timestamp"))
        .withColumn("order_estimated_delivery_date",F.col("order_estimated_delivery_date").cast("timestamp"))
    )