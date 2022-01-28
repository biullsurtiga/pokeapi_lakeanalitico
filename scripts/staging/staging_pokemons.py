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

    
    dfPokemon = dfPokemonRaw.select(
            col("id").alias("id"),
            col("name").alias("name"),
            col("order").alias("order"),
            col("weight").alias("weight"),
            col("height").alias("height"),
            col("is_default").alias("is_default"),
            extractIdUDF("location_area_encounters").alias("location_area_encounters"),
            col("base_experience").alias("base_experience")
            ).dropDuplicates()

    # dfPokemon.show(10,False)
    # dfPokemon.printSchema()
    # print(dfPokemon.count())

    dfFinal = dfPokemon.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/pokemon")
