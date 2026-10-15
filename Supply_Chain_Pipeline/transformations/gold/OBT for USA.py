from pyspark import pipelines as dp
from pyspark.sql.functions import *

#creating OBT for USA
@dp.table(name="supply_chain_project.gold.OBT_USA")
def OBT_USA():
    orders=spark.read.table("supply_chain_project.gold.fact_orders")
    products=spark.read.table("supply_chain_project.gold.dim_products")
    customer=spark.read.table("supply_chain_project.gold.dim_customer")
    
    df=orders.join(products,products.product_card_id==orders.product_card_id,"inner").join(customer,customer.customer_id==orders.customer_id,"inner").filter(col("customer_country")=="USA")
    
    df_final_col=df.select(
        "customer_fname",
        "customer_lname",
        "customer_country",
        "product_name",
        "category_name",
        "total_amount"
    )
    
    return df_final_col