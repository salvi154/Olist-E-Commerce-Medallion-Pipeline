from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_or_fail("rule1",'customer_id IS NOT NULL')
@dp.table(name="silver.customers_silver")
def customers_silver():
    return (
        spark.readStream.table('olist_medallion.bronze.customers_bronze')
        .dropDuplicates(['customer_id'])
        .withColumn("ingest_ts", F.current_timestamp())
    )