from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf,explode_outer, explode
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
    dfEvolutionChainRaw = spark.read.parquet("/home/urtiga/Projetos/desafios/pikpay/data/data_lake/raw/evolution_chain")

    dfEvoChain2F =dfEvolutionChainRaw.withColumn("evolves_to", explode("chain.evolves_to")) \
            .withColumn("evolution_details", explode_outer("evolves_to.evolution_details")) \
            .select(
            col("id").alias("evol_chain_id"),
            extractIdUDF("evolves_to.species.url").alias("species_id"),
            col("evolves_to.is_baby").alias("is_baby"),
            col("evolution_details.gender").alias("gender"),
            extractIdUDF("evolution_details.held_item.url").alias("held_item_id"),
            col("evolution_details.held_item.name").alias("held_item_name"),
            extractIdUDF("evolution_details.item.url").alias("item_id"),
            col("evolution_details.item.name").alias("item_name"),
            extractIdUDF("evolution_details.known_move.url").alias("known_move_id"),
            col("evolution_details.known_move.name").alias("known_move_name"),
            extractIdUDF("evolution_details.known_move_type.url").alias("known_move_type_id"),
            col("evolution_details.known_move_type.name").alias("known_move_type_name"),
            extractIdUDF("evolution_details.location.url").alias("location_id"),
            col("evolution_details.location.name").alias("location_name"),
            col("evolution_details.min_affection").alias("min_affection"),
            col("evolution_details.min_beauty").alias("min_beauty"),
            col("evolution_details.min_happiness").alias("min_happiness"),
            col("evolution_details.min_level").alias("min_level"),
            col("evolution_details.needs_overworld_rain").alias("needs_overworld_rain"),
            extractIdUDF("evolution_details.party_species.url").alias("party_species_id"),
            col("evolution_details.party_species.name").alias("party_species_name"),
            extractIdUDF("evolution_details.party_type.url").alias("party_type_id"),
            col("evolution_details.party_type.name").alias("party_type_name"),
            col("evolution_details.relative_physical_stats").alias("relative_physical_stats"),
            col("evolution_details.time_of_day").alias("time_of_day"),
            extractIdUDF("evolution_details.trade_species.url").alias("trade_species_id"),
            col("evolution_details.trade_species.name").alias("trade_species_name"),
            extractIdUDF("evolution_details.trigger.url").alias("trigger_id"),
            col("evolution_details.trigger.name").alias("trigger_name"),
            col("evolution_details.turn_upside_down").alias("turn_upside_down")
        ).dropDuplicates()

    dfEvoChain2F.filter("evol_chain_id = 1").show(20,False)
    # dfEvoChain2F.printSchema()
    # print(dfEvoChain2F.count())

    # dfFinal = dfEvoChain2F.withColumn("dtinsert", lit(date_atual))
    # dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/evolution_chain_2F")