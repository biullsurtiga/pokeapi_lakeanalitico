from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
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
    dfPokemonRaw = spark.read.parquet("/home/urtiga/Projetos/desafios/pikpay/data/data_lake/raw/pokemon")

    dfSpecies = dfPokemonRaw.select(
        col("id").alias("pokemon_id"),
        extractIdUDF("species.url").alias("species_id"),
        col("species.name").alias("species_name")
    ).dropDuplicates()

    # dfSpecies.show(10,False)
    # dfSpecies.printSchema()
    # print(dfSpecies.count())
    # dfSpecies.select("species_id", "species_name", lit(1).alias("qtd")).groupBy("species_id", "species_name").agg(sum(col("qtd")).alias("sqtd")).filter(col("sqtd") > lit(1)).show(10,False)

    dfFinal = dfSpecies.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/species")
