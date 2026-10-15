from pyspark import pipelines as dp 


@dp.materialized_view(name='gold.dim_product')
def dim_product():
  return spark.table('olist_medallion.silver.products_silver').select('product_category_name','product_id','product_name_lenght','product_description_lenght','product_photos_qty','product_weight_g','product_length_cm','product_height_cm','product_width_cm','product_category_name_english')