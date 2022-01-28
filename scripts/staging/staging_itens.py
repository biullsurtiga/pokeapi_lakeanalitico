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

    dfItens= dfPokemonRaw \
                .withColumn("held_items", explode_outer("held_items")) \
                    .select(
                        col("id").alias("pokemon_id"),
                        extractIdUDF("held_items.item.url").alias("item_id"),
                        col("held_items.item.name").alias("item_name"),
                    ).dropDuplicates()
    
    # dfItens.show(10,False)
    # print(dfItens.count())
    # dfItens.printSchema()
    #dfItens.select(col("item_id"),col("item_name"), lit(1).alias("qtd")).groupBy(col("item_id"),col("item_name")).agg(sum(col("qtd")).alias("sqtd")).filter(col("sqtd") > lit(1)).show(10,false)
  
    dfFinal = dfItens.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/itens")
