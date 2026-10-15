from pyspark import pipelines as dp
from pyspark.sql.functions import *

#Creating Temporary Streaming Table for Dim
@dp.table(name="Temp_Dim_customer")

def Temp_Dim_customer():
    df=spark.readStream.table("supply_chain_project.silver.silver_obt")
    df=df.select(
        "customer_id",
        "customer_fname",
        "customer_lname",
        "customer_country",
        "customer_state",
        "customer_segment",
        "order_date_dateorders"
    )

    return df

#Creating target Table for Dim
dp.create_streaming_table(name="supply_chain_project.gold.Dim_customer")

#applying CDC-1
dp.create_auto_cdc_flow(
    target="supply_chain_project.gold.Dim_customer",
    source="Temp_Dim_customer",
    keys=["customer_id"],
    sequence_by="order_date_dateorders",
    stored_as_scd_type=1
)
