from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf,explode_outer
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

    dfTypes = dfPokemonRaw \
                    .withColumn("types", explode_outer("types")) \
                    .select(
                        col("id").alias("pokemon_id"),
                        extractIdUDF("types.type.url").alias("type_id"),
                        col("types.type.name").alias("type_name")
                    ).dropDuplicates()
    
    # dfTypes.filter("pokemon_id = 1").show(20,False)
    # dfTypes.printSchema()
    # print(dfTypes.count())

    dfFinal = dfTypes.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/types")
