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
    dfEvolutionTriggerRaw = spark.read.parquet("/home/urtiga/Projetos/desafios/pikpay/data/data_lake/raw/evolution_trigger")

    dfLngEvoTrigger = dfEvolutionTriggerRaw.withColumn("names", explode_outer("names")) \
        .select(
        col("id").alias("evol_trigger_id"),
        extractIdUDF("names.language.url").alias("language_id"),
        col("names.language.name").alias("language"),
        col("names.name").alias("name"),
    ).dropDuplicates()

    # dfLngEvoTrigger.show(20,False)
    # dfLngEvoTrigger.printSchema()
    # print(dfLngEvoTrigger.count())

    dfFinal = dfLngEvoTrigger.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/language_evol_trigger")