from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf,explode_outer, explode
from datetime import datetime

def extract_id(url):
    if not url:
        id = ""
    else:
        id = f"{url}".rsplit('/', 2)[-2]
    return id

if __name__ == "__main__":

    #Hora e Data atual para dtinsert
    dateTimeCurrent = datetime.now()
    date_atual = str(dateTimeCurrent.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession.builder.getOrCreate()
    extractIdUDF = udf(lambda url:  extract_id(url))
    dfEvolutionChainRaw = spark.read.parquet("/home/urtiga/Projetos/desafios/pikpay/data/data_lake/raw/evolution_chain")

    # dfEvolutionChainRaw.show(1)
    # dfEvolutionChainRaw.printSchema()
    # print(dfEvolutionChainRaw.count())

    # dfEvoChain =dfEvolutionChainRaw.select(
    #     col("id").alias("evol_chain_id"),
    #     col("baby_trigger_item.name").alias("baby_trigger_item"),
    # ).dropDuplicates()

    dfEvoChain =dfEvolutionChainRaw.select(
        col("id").alias("evol_chain_id"),
        extractIdUDF("chain.species.url").alias("species_id"),
        col("chain.is_baby").alias("is_baby"),
        col("baby_trigger_item.name").alias("baby_trigger_item"),
        extractIdUDF("baby_trigger_item.url").alias("baby_trigger_item_url"),
        explode_outer("chain.evolution_details").alias("evolution_details")
    ).dropDuplicates()

    dfEvoChain.filter("evol_chain_id = 1").show(20,False)
    #dfEvoChain.printSchema()
    #print(dfEvoChain.count())

    # dfFinal = dfEvoChain.withColumn("dtinsert", lit(date_atual))
    # dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/evolution_chain_1F")