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

    dfStats = dfPokemonRaw \
                    .withColumn("stats", explode_outer("stats")) \
                    .select(
                        col("id").alias("pokemon_id"),
                        extractIdUDF("stats.stat.url").alias("stat_id"),
                        col("stats.stat.name").alias("stat_name"),
                        col("stats.base_stat").alias("base_stat"),
                        col("stats.effort").alias("effort"),
                    ).dropDuplicates()
    
    #dfStats.filter("pokemon_id = 1").show(10,False)
    #dfStats.printSchema()
    #print(dfStats.count())
    #dfStats.select(col("stat_id"), col("stat_name"), lit(1).alias("qtd")).groupBy(col("stat_id"), col("stat_name")).agg(sum(col("qtd")).alias("sqtd")).filter(col("sqtd") > lit(1)).show(10,false)

    dfFinal = dfStats.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/stats")
