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

    dfPastTypes = dfPokemonRaw \
                    .withColumn("past_types", explode_outer("past_types")) \
                    .withColumn("types", explode_outer("past_types.types")) \
                    .select(
                        col("id").alias("pokemon_id"),
                        extractIdUDF("past_types.generation.url").alias("generation_id"),
                        col("past_types.generation.name").alias("generation_name"),
                        extractIdUDF("types.type.url").alias("type_id"),
                        col("types.type.name").alias("type_name"),
                        col("types.slot").alias("slot"),
                    ).dropDuplicates()
    
    # dfPastTypes.show(20,False)
    # dfPastTypes.printSchema()
    # print(dfPastTypes.count())
    #dfPastTypes.select(col("generation_id"), col("generation_name"), lit(1).alias("qtd")).groupBy(col("generation_id"), col("generation_name")).agg(sum(col("qtd")).alias("sqtd")).filter(col("sqtd") > lit(1)).show(10,false)
    
    dfFinal = dfPastTypes.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/past_types")
