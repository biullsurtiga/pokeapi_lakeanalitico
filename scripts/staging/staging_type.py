from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf,explode_outer,explode, asc
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
    dfTypeRaw = spark.read.parquet("/home/urtiga/Projetos/desafios/pikpay/data/data_lake/raw/type_demage")

    dfTypeDetails = dfTypeRaw \
                    .withColumn("double_damage_from", explode_outer("damage_relations.double_damage_from")) \
                        .withColumn("double_damage_to", explode_outer("damage_relations.double_damage_to")) \
                    .select(
                        col("id").alias("type_id"),
                        col("name").alias("type_name"),
                        #extractIdUDF("pokemon.pokemon.url").alias("pokemon_id"),
                        #col("pokemon.pokemon.name").alias("pokemon_name"),
                        #extractIdUDF("moves.url").alias("moves_id"),
                        #col("moves.name").alias("moves_name"),
                        extractIdUDF("double_damage_from.url").alias("damage_from_id"),
                        col("double_damage_from.name").alias("damage_from_name"),
                        extractIdUDF("double_damage_to.url").alias("damage_to_id"),
                        col("double_damage_to.name").alias("damage_to_name")
                    ).dropDuplicates()
    
    # dfTypeDetails.filter("type_name = 'fire'").show(20,False)
    # dfTypeDetails.printSchema()
    # print(dfTypeDetails.count())


    dfDemageFrom = dfTypeDetails.select(
                    col("type_id").alias("type_id"),
                    col("type_name").alias("type_name"),
                    col("damage_from_id").alias("damage_from_id"),
                    col("damage_from_name").alias("damage_from_name")
                    ).dropDuplicates()

    dfDemageTo = dfTypeDetails.select(
                    col("type_id").alias("type_id"),
                    col("type_name").alias("type_name"),
                    col("damage_to_id").alias("damage_to_id"),
                    col("damage_to_name").alias("damage_to_name")
                    ).dropDuplicates()
    
    #dfDemageTo.select("*").orderBy(asc("type_id")).show(100,False)
    #dfDemageFrom.printSchema()
    #print(dfDemageTo.count())

    dfFinal = dfDemageFrom.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/types_damage_from")

    dfFinal2 = dfDemageTo.withColumn("dtinsert", lit(date_atual))
    dfFinal2.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/types_damage_to")
