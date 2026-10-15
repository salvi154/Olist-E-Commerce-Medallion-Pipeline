from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_or_fail("rule1",'geolocation_zip_code_prefix is not null')
@dp.table(name="silver.geolocation_silver")
def geolocation_silver():
    return (
        spark.readStream.table('olist_medallion.bronze.geolocation_bronze')
        .dropDuplicates(['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng', 'geolocation_city', 'geolocation_state'])
        .withColumn("ingest_ts", F.current_timestamp())
    ) 