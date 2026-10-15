from pyspark import pipelines as dp 
from pyspark.sql.functions import *



@dp.materialized_view(name='gold.fact_order_item')
def fact_order_item():
  order_item = spark.table('olist_medallion.silver.order_items_silver')
  product = spark.table('olist_medallion.gold.dim_product')
  seller = spark.table('olist_medallion.gold.dim_seller')
  dates = spark.table('olist_medallion.gold.dim_date')
  order_item = order_item.withColumn('shipping_limit_date_key',date_format(col('shipping_limit_date'),'yyyyMMdd').cast('int'))
  joined_df = order_item.join(product, on="product_id",how='left').join(seller, on='seller_id',how='left').join(dates, order_item.shipping_limit_date_key == dates.date_id,'left')
  return joined_df

