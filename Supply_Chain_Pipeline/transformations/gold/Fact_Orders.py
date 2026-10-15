from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table(name="supply_chain_project.gold.Fact_Orders")

def Fact_Orders():
    df=spark.readStream.table("supply_chain_project.silver.silver_obt")

    df=df.select(
        "order_item_id",
        "order_id",
        "customer_id",
        "product_card_id",
        "order_item_product_price",
        "order_item_quantity",
        "order_item_discount_rate",
        (col("order_item_product_price")*col("order_item_quantity")). alias ("Total_Amount"),
        "order_date_dateorders"
    )

    return df
