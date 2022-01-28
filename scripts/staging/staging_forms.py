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

    dfForms = dfPokemonRaw \
                .withColumn("forms", explode_outer("forms")) \
                    .select(
                        col("id").alias("pokemon_id"),
                        extractIdUDF("forms.url").alias("forms_id"),
                        col("forms.name").alias("forms_name")
                    ).dropDuplicates()
    
    # dfForms.filter("id = 3").show(10,False)
    # print(dfForms.count())
    # dfForms.printSchema()
    #dfForms.select("forms_id", "forms_name", lit(1).alias("qtd")).groupBy("forms_id", "forms_name").agg(sum(col("qtd")).alias("sqtd")).filter(col("sqtd") > lit(1)).show(10,False)
    
    dfFinal = dfForms.withColumn("dtinsert", lit(date_atual))
    dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/forms")
