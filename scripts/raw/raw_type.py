from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    #Hora e Data atual para dtinsert
    dateTimeCurrent = datetime.now()
    date_atual = str(dateTimeCurrent.strftime("%Y-%m-%d %H:%M:%S"))

    # Write Pokemon raw
    dfTyperaw = spark.read.option("inferSchema", True).json("./data/data_lake/landing/types.json")
    dfType = dfTyperaw.withColumn("dtinsert", lit(date_atual))
    dfType.write.format("parquet").mode("overwrite").save("./data/data_lake/raw/type")

    # dfPokemon.select(col("id"),col("name"), col("dtinsert")).show(10, truncate=False)
    # dfPokemon.printSchema()
    # print(dfPokemon.count())
    