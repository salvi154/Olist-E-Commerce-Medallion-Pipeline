from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_or_fail("rule1",'product_id IS NOT NULL')
@dp.expect("rule3",'product_category_name IS NOT NULL')
@dp.table(name="silver.products_silver")
def products_silver():
    df1 =  (
        spark.readStream.table("olist_medallion.bronze.products_bronze")
        .dropDuplicates(["product_id"])
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("product_name_lenght",F.col("product_name_lenght").cast("int"))
        .withColumn("product_description_lenght",F.col("product_description_lenght").cast("int"))
        .withColumn("product_photos_qty",F.col("product_photos_qty").cast("int"))
        .withColumn("product_weight_g",F.col("product_weight_g").cast("double"))
        .withColumn("product_length_cm",F.col("product_length_cm").cast("double"))
        .withColumn("product_height_cm",F.col("product_height_cm").cast("double"))
        .withColumn("product_width_cm",F.col("product_width_cm").cast("double"))
    )
    df2 = (
        spark.read.table("olist_medallion.bronze.category_translation_bronze")
        .select("product_category_name", "product_category_name_english")
        .dropDuplicates(["product_category_name"])
    )

    return df1.join(df2,on="product_category_name",how="left")