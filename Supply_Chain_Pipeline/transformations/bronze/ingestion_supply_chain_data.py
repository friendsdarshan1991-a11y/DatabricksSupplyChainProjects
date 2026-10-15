from pyspark import pipelines as dp
import re

#Key Vault Config to Access the Azure Delta Lake
spark.conf.set("fs.azure.account.auth.type.proddatalakefordatabrick.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.proddatalakefordatabrick.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.proddatalakefordatabrick.dfs.core.windows.net", "8b03b30e-c838-4568-a41e-a801634f0e96")
spark.conf.set("fs.azure.account.oauth2.client.secret.proddatalakefordatabrick.dfs.core.windows.net", dbutils.secrets.get(scope="darshanscope", key="app-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.proddatalakefordatabrick.dfs.core.windows.net", "https://login.microsoftonline.com/5ff33f54-37cd-4d6d-9156-f6c00f164999/oauth2/token")


#reading the files from Streaming folder 
@dp.table(name="supply_chain_project.bronze.bronze_obt")
def bronze_obttt():


    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(
            "abfss://obtfolder@proddatalakefordatabrick.dfs.core.windows.net/source/data"
        )
    )

    clean_cols = [
    re.sub(r'_+', '_',
           re.sub(r'[^a-zA-Z0-9]', '_', col.strip().lower())
          ).strip('_')
    for col in df.columns
    ]
    df = df.toDF(*clean_cols)
    
    return df

