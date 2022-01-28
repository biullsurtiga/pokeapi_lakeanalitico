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

    dfEvoTrigger =dfEvolutionTriggerRaw.select(
        col("id").alias("evol_trigger_id"),
        col("name").alias("evol_trigger_name")
    ).dropDuplicates()

    # dfEvoTrigger.show(20,False)
    # dfEvoTrigger.printSchema()
    # print(dfEvoTrigger.count())

    dfFinal = dfEvoTrigger.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/evolution_trigger")