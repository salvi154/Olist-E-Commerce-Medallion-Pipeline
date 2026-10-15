from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_or_fail("rule1",'order_id is not null and order_item_id IS NOT NULL')
@dp.expect_or_drop("rule2",'price > 0')
@dp.table(name="silver.order_items_silver")
def order_items_silver():
    return (
        spark.readStream.table('olist_medallion.bronze.order_items_bronze')
        .dropDuplicates(['order_id','order_item_id'])
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("shipping_limit_date",F.col("shipping_limit_date").cast("timestamp"))
        .withColumn("price",F.col("price").cast("double"))
        .withColumn("freight_value",F.col("freight_value").cast("double"))
    )