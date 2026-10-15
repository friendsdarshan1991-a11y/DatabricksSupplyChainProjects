from pyspark import pipelines as dp
from pyspark.sql.functions import *

#Creating Temporary Streaming Table for Dim
@dp.table(name="Temp_Dim_products")

def Temp_Dim_products():
    df=spark.readStream.table("supply_chain_project.silver.silver_obt")
    df=df.select(
        "product_card_id",
        "product_name",
        "category_name",
        "department_name",
        "product_price",
        "order_date_dateorders"
    )

    return df

#Creating target Table for Dim
dp.create_streaming_table(name="supply_chain_project.gold.Dim_products")

#applying CDC-1
dp.create_auto_cdc_flow(
    target="supply_chain_project.gold.Dim_products",
    source="Temp_Dim_products",
    keys=["product_card_id"],
    sequence_by="order_date_dateorders",
    stored_as_scd_type=1
)
