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

    dfGameVersions= dfPokemonRaw \
                .withColumn("game_indices", explode_outer("game_indices")) \
                    .select(
                        col("id").alias("pokemon_id"),
                        #col("game_indices.game_index").alias("game_index"),
                        extractIdUDF("game_indices.version.url").alias("version_id"),
                        col("game_indices.version.name").alias("version_name"),
                    ).dropDuplicates().drop("game_indices")
    
    # dfGameVersions.filter("pokemon_id = 25").show(30,False)
    # print(dfGameVersions.count())
    #dfGameVersions.printSchema()
    #dfGameVersions.select("version_id", "version_name", lit(1).alias("qtd")).groupBy("version_id", "version_name").agg(sum(col("qtd")).alias("sqtd")).filter(col("sqtd") > lit(1)).show(10,False)
  
    dfFinal = dfGameVersions.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/game_versions")
