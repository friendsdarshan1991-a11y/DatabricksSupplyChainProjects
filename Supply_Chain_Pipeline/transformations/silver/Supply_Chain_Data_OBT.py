from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(name="supply_chain_project.silver.silver_obt")
def silver_obt():
    df=spark.readStream.table("supply_chain_project.bronze.bronze_obt")

    df=(
        df.withColumn("customer_country",when(col("customer_country")=="EE. UU.", "USA").otherwise(col("customer_country")))
            .withColumn("customer_zipcode",when(col("customer_zipcode").isNull(), "Unknown").otherwise(col("customer_zipcode").cast("string")))
            .withColumn("order_zipcode",when(col("order_zipcode").isNull(), "Unknown").otherwise(col("order_zipcode").cast("string")))
    ).drop("product_description","customer_email","customer_password")

    return df