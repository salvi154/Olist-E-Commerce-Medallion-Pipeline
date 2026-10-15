from pyspark import pipelines as dp 


@dp.materialized_view(name='gold.dim_seller')
def dim_seller():
  return spark.table('olist_medallion.silver.sellers_silver').select('seller_id','seller_zip_code_prefix','seller_city','seller_state')