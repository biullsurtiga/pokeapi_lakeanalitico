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

    dfMoves= dfPokemonRaw \
                    .withColumn("moves", explode_outer("moves")) \
                    .withColumn("version_group_details", explode_outer("moves.version_group_details")) \
                    .select(
                        col("id").alias("pokemon_id"),
                        extractIdUDF("moves.move.url").alias("move_id"),
                        col("moves.move.name").alias("move_name"),
                        extractIdUDF("version_group_details.move_learn_method.url").alias("move_learn_method_id"),
                        col("version_group_details.move_learn_method.name").alias("move_learn_method_name"),
                        extractIdUDF("version_group_details.version_group.url").alias("version_group_id"),
                        col("version_group_details.version_group.name").alias("version_group_name")
                    ).dropDuplicates()
    
    # dfMoves.show(10,False)
    # print(dfMoves.count())
    # dfMoves.printSchema()
    # dfMoves.select(col("move_id"),col("move_name"), lit(1).alias("qtd")).groupBy(col("move_id"),col("move_name")).agg(sum(col("qtd")).alias("sqtd")).filter(col("sqtd") > lit(1)).show(10,false)
    
    dfFinal = dfMoves.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(10).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/moves")
