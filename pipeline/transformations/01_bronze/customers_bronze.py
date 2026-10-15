from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table
def customers_bronze():
    return (spark.readStream.format('cloudFiles')
            .option("cloudFiles.format","csv")
            .option("cloudFiles.schemaLocation","/Volumes/olist_medallion/landing/schema/customers") 
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")                  
            .load("/Volumes/olist_medallion/landing/raw_files/customers")  
            .withColumn("ingestion_timestamp",current_timestamp())
            .withColumn("source_file",col("_metadata.file_path"))
            )