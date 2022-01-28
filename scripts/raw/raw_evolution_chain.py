from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    #Hora e Data atual para dtinsert
    dateTimeCurrent = datetime.now()
    date_atual = str(dateTimeCurrent.strftime("%Y-%m-%d %H:%M:%S"))

    # Write Evolution Chain raw
    dfEvoChainraw = spark.read.option("inferSchema", True).json("./data/data_lake/landing/evolutions-chain.json")
    dfEvoChain = dfEvoChainraw.withColumn("dtinsert", lit(date_atual))
    dfEvoChain.write.format("parquet").mode("overwrite").save("./data/data_lake/raw/evolution_chain")

    # dfEvoChain.select(col("id"), col("dtinsert")).show(10, truncate=False)
    # dfEvoChain.printSchema()
    # print(dfEvoChain.count())
    