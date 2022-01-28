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

    # dfEvolutionChainRaw.show(1)
    # dfEvolutionChainRaw.printSchema()
    # print(dfEvolutionChainRaw.count())

    # dfEvoChain =dfEvolutionChainRaw.select(
    #     col("id").alias("evol_chain_id"),
    #     col("baby_trigger_item.name").alias("baby_trigger_item"),
    # ).dropDuplicates()

    dfEvoChain3F =dfEvolutionChainRaw.withColumn("evolves_to_2", explode("chain.evolves_to")) \
            .withColumn("evolves_to_3", explode("evolves_to_2.evolves_to")) \
            .withColumn("evolution_details", explode_outer("evolves_to_3.evolution_details")) \
            .select(
            col("id").alias("evol_chain_id"),
            extractIdUDF("evolves_to_3.species.url").alias("species_id"),
            col("evolves_to_3.is_baby").alias("is_baby"),
            col("evolution_details.gender").alias("gender"),
            extractIdUDF("evolution_details.held_item.url").alias("held_item_id"),
            col("evolution_details.held_item.name").alias("held_item_name"),
            extractIdUDF("evolution_details.item.url").alias("item_id"),
            col("evolution_details.item.name").alias("item_name"),
            extractIdUDF("evolution_details.known_move.url").alias("known_move_id"),
            col("evolution_details.known_move.name").alias("known_move_name"),
            col("evolution_details.known_move_type").alias("known_move_type"),
            extractIdUDF("evolution_details.location.url").alias("location_id"),
            col("evolution_details.location.name").alias("location_name"),
            col("evolution_details.min_affection").alias("min_affection"),
            col("evolution_details.min_beauty").alias("min_beauty"),
            col("evolution_details.min_happiness").alias("min_happiness"),
            col("evolution_details.min_level").alias("min_level"),
            col("evolution_details.needs_overworld_rain").alias("needs_overworld_rain"),
            col("evolution_details.party_species").alias("party_species"),
            col("evolution_details.party_type").alias("party_type"),
            col("evolution_details.relative_physical_stats").alias("relative_physical_stats"),
            col("evolution_details.time_of_day").alias("time_of_day"),
            col("evolution_details.trade_species").alias("trade_species"),
            extractIdUDF("evolution_details.trigger.url").alias("trigger_id"),
            col("evolution_details.trigger.name").alias("trigger_name"),
            col("evolution_details.turn_upside_down").alias("turn_upside_down")
        ).dropDuplicates()

    dfEvoChain3F.filter("evol_chain_id = 1").show(20,False)
    # dfEvoChain3F.printSchema()
    # print(dfEvoChain3F.count())

    # dfFinal = dfEvoChain3F.withColumn("dtinsert", lit(date_atual))
    # dfFinal.coalesce(4).write.format("parquet").mode("overwrite").save("./data/data_lake/staging/evolution_chain_3F")