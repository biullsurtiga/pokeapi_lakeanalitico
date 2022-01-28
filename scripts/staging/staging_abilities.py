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

    dfAbilitities = dfPokemonRaw \
                .withColumn("abilities", explode_outer("abilities")) \
                    .select(
                        col("id").alias("pokemon_id"),
                        extractIdUDF("abilities.ability.url").alias("ability_id"),
                        col("abilities.ability.name").alias("ability_name")
                    ).dropDuplicates()
    
    # dfAbilitities.filter("id = 3").show(10,False)
    # #dfAbilitities.printSchema()
    # print(dfAbilitities.count())
    #dfAbilitities.select("ability_id", "ability_name", lit(1).alias("qtd")).groupBy("ability_id", "ability_name").agg(sum(col("qtd")).alias("sqtd")).filter(col("sqtd") > lit(1)).show(10,False)
    
    dfFinal = dfAbilitities.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/abilities")
