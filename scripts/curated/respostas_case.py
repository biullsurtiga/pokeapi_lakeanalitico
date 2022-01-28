# Databricks notebook source
# MAGIC %md ### Analise do jogo oficial do Pokémon (PokeApi) - Respostas 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md ##### Imports e carga das tabelas da camada Staging

# COMMAND ----------

#dfPokemonStaging = spark.read.parquet("/FileStore/tables/data_lake/staging/pokemon")
#dfSpecies = spark.read.parquet("/FileStore/tables/data_lake/staging/species")
#dfEvo1F = spark.read.parquet("/FileStore/tables/data_lake/staging/evolution_chain_1F")
#dfEvo2F = spark.read.parquet("/FileStore/tables/data_lake/staging/evolution_chain_2F")
#dfEvo3F = spark.read.parquet("/FileStore/tables/data_lake/staging/evolution_chain_3F")
dfTypes = spark.read.parquet("/FileStore/tables/data_lake/staging/types")
#dfMoves = spark.read.parquet("/FileStore/tables/data_lake/staging/moves")
#dfStats = spark.read.parquet("/FileStore/tables/data_lake/staging/stats")
#dfDamageFrom = spark.read.parquet("/FileStore/tables/data_lake/staging/types_damage_from/")
#dfDamageTo = spark.read.parquet("/FileStore/tables/data_lake/staging/types_damage_to/")

# COMMAND ----------

# MAGIC %md ### Questão 1

# COMMAND ----------

#QUESTAO 1
dfNew = dfPokemonStaging.withColumn(
      "weight_kg", round(col("weight") * 0.1, 2)
  ).withColumn(
      "height_m", round(col("height") / 10, 2)
  ).select(
      "id", "name", "weight","weight_kg", "height", "height_m"
  )
#EXISTEM POKEMONS GMAX E ETERNAMAX QUE SAO GIGANTES OS MAIORES SAO (eternatus-eternamax 100m e centiskorch-gmax 75m)
#MAS ELES FORAM RETIRADOS DO RESULTADO FINAL, DEIXANDO APENAS OS COM ALTURA NORMAL
#display(dfNew.select(col("name").alias("Pokémon"), col("height_m").alias("Altura")).orderBy(desc("height_m")).limit(10))

display(dfNew.filter("name not like ('%-%')").select(col("name").alias("Pokemon"), col("height_m").alias("Altura")).orderBy(desc("height_m")).limit(10))

# COMMAND ----------

# MAGIC %md ### Questão 2

# COMMAND ----------

# QUESTAO 2

# FOI AGRUPADO OS CAMINHOS DE EVOLUÇÃO E ESPECIES E FILTRADOS OS QUE TINHAM MAIS DE 1 CAMINHO
Q2_Evo2F = dfEvo2F.select("evol_chain_id", "species_id").groupby("evol_chain_id", "species_id").count().orderBy(desc("count")).filter("count > 1")
Q2_Evo3F = dfEvo3F.select("evol_chain_id", "species_id").groupby("evol_chain_id", "species_id").count().orderBy(desc("count")).filter("count > 1")

# DEPOIS OS DATAFRAMES FORAM UNIDOS E OS POKEMONS IDENTIFICADOS
Q2_dfFinal = Q2_Evo2F.unionAll(Q2_Evo3F)

Q2_Pokefinal = Q2_dfFinal.alias("evo").join(
    dfPokemonStaging.alias("poke"), col("evo.species_id") == col("poke.id"), "inner"
).select(
        col("poke.id").alias("id"),
        col("poke.name").alias("nome")
)

# E CONTADOS, RESULTANDO EM 8 
Q2_Pokefinal.count()

# COMMAND ----------

# MAGIC %md #### O número de Pokémon com mais de um caminho evolutivo é 8

# COMMAND ----------

# MAGIC %md ### Questão 3

# COMMAND ----------

#QUESTAO 3
dfPokemoTypes = dfPokemonStaging.alias("pokemon").join(
    dfTypes.alias("types"), col("pokemon.id") == col("types.pokemon_id"), "inner"
    ).select(
    col("pokemon.id"), col("pokemon.name"),col("pokemon.base_experience"),col("types.type_name")
    )

#POKEMONS NA FORMA PADRAO E FILTRADOS PELO TIPO ICE
Q3_df = dfPokemoTypes.alias("poketypes").join(
    dfEvo1F.alias("evol1F"), col("evol1F.species_id") == col("poketypes.id"), "inner"
).filter("poketypes.type_name = 'ice'").select(
    col("poketypes.id"), col("poketypes.name"),col("poketypes.base_experience"),col("poketypes.type_name")
    ).orderBy(desc("poketypes.base_experience"))

#POKEMONS ORDENADOS PELO QUE FORNECE MAIS XP AO SER DERROTADO
display(Q3_df.select(
        col("name").alias("Pokémon"),
        col("base_experience").alias("Experiência")
    ).limit(5))

# COMMAND ----------

# MAGIC %md ### Questão 4

# COMMAND ----------

#QUESTAO 4
#POKEMONS FORMA BASE
dfPoke1F = dfPokemonStaging.alias("pokemon").join(
    dfEvo1F.alias("evol1F"), col("evol1F.species_id") == col("pokemon.id"), "inner"
).select(
    col("pokemon.id"), col("pokemon.name"))

# FILTRADOS POR METODO level-up
dfPokeMoves = dfPoke1F.alias("poke1f").join(
    dfMoves.alias("moves"), col("poke1f.id") == col("moves.pokemon_id"), "inner"
).filter("move_learn_method_name = 'level-up'").select(
    col("poke1f.id"), col("poke1f.name"),col("moves.move_name"),col("moves.move_learn_method_name"), col("moves.version_group_name"))

# FOI ENCONTRADO QUE O GOLPE MAIS APRENDIDO POR POKÉMON PELO METODO "level-up" É O "tackle" NA VERSAO "ultra-sun-ultra-moon"
#display(dfPokeMoves.select(col("moves.move_name"), col("moves.version_group_name")).groupBy(col("moves.move_name"), col("moves.version_group_name")).count().orderBy(desc("count")))
#tackl|ultra-sun-ultra-moon|118

Q4_df = dfPokeMoves.alias("pokemoves").join(
    dfStats.alias("stats"), col("pokemoves.id") == col("stats.pokemon_id"), "inner"
).filter("move_name = 'tackle' and version_group_name = 'ultra-sun-ultra-moon' and stat_name = 'attack'").select(
    col("name"), col("version_group_name"), col("base_stat")).orderBy(desc("base_stat"))

#POKEMONS COM MAIOR VALOR DE ATRIBUTO ATTACK E A VERSAO DO JOGO
display(Q4_df.select(
        col("name").alias("Pokémon"),
        col("version_group_name").alias("Versão"),
        col("base_stat").alias("Valor do atributo")
    ).limit(5))

# COMMAND ----------

# MAGIC %md ### Questão 5

# COMMAND ----------

#QUESTAO 5

# CRIANDO OS DATAFRAMES COM OS POKEMONS DE 1 FORMA E 2 FORMA 

dfPoke1F = dfPokemonStaging.alias("pokemon").join(
    dfEvo1F.alias("evol1F"), col("evol1F.species_id") == col("pokemon.id"), "inner"
).select(
    col("pokemon.id").alias("id1"), col("pokemon.name").alias("name1"), col("evol1F.evol_chain_id"))

dfPoke2F = dfPokemonStaging.alias("pokemon").join(
   dfEvo2F.alias("evol2F"), col("evol2F.species_id") == col("pokemon.id"), "inner"
).select(
    col("pokemon.id").alias("id2"), col("pokemon.name").alias("name2"), col("evol2F.evol_chain_id"))


# ADD OS STATS DOS POKEMONS AOS DATAFRAMES 1 FORMA E 2 FORMA

dfPoke1Stat = dfPoke1F.alias("poke1f").join(
    dfStats.alias("stats"), col("poke1f.id1") == col("stats.pokemon_id") , "inner"  
).select(
    col("poke1f.id1"), col("poke1f.name1"), col("stats.stat_name").alias("stat_name1"), col("stats.base_stat").alias("base_stat1"),col("evol_chain_id")
)

dfPoke2Stat = dfPoke2F.alias("poke2f").join(
    dfStats.alias("stats"), col("poke2f.id2") == col("stats.pokemon_id") , "inner"  
).select(
    col("poke2f.id2"), col("poke2f.name2"), col("stats.stat_name").alias("stat_name2"), col("stats.base_stat").alias("base_stat2"),col("evol_chain_id")
)

# ADD COLUNA COM O AUMENTO DO ATRIBUTO

dfPoke = dfPoke1Stat.alias("poke1f").join(
   dfPoke2Stat.alias("poke2f"), [col("poke1f.evol_chain_id") == col("poke2f.evol_chain_id") , col("poke1f.stat_name1") == col("poke2f.stat_name2")], "inner"
).withColumn(
    "Aumento_do_atributo", col("base_stat2") - col("base_stat1")
).select(
    col("poke1f.name1").alias("Pre_evolucao"),col("poke2f.name2").alias("Evolucao"), col("poke1f.stat_name1").alias("Atributo"), col("Aumento_do_atributo")
)

Q5_df = dfPoke.select("*").orderBy(desc("Aumento_do_atributo"))
display(Q5_df.limit(7))


# COMMAND ----------

# MAGIC %md ### Questão 6

# COMMAND ----------

# QUESTAO 6
dfPoke1F = dfPokemonStaging.alias("pokemon").join(
    dfEvo1F.alias("evol1F"), col("evol1F.species_id") == col("pokemon.id"), "inner"
).select(
    col("pokemon.id"), col("pokemon.name"),col("evol1F.species_id"), col("evol1F.evol_chain_id"))

dfTypesto = dfTypes.alias("types").join(
    dfDamageTo.alias("demageto"), col("types.type_id") == col("demageto.type_id"), "inner"
).select(
     col("types.pokemon_id"), col("types.type_id"),col("types.type_name"), col("demageto.damage_to_id"), col("demageto.damage_to_name"))

dfPokeTypes = dfPoke1F.alias("poke").join(
    dfTypesto.alias("tyto"), col("poke.id") == col("tyto.pokemon_id"), "inner"
).select(
     col("poke.id"), col("poke.name"),col("tyto.type_name"), col("tyto.damage_to_name"))


#dfPokeTypes.filter("id = 639").select(col("poke.id"), col("poke.name"),col("tyto.type_name")).show()
dfPokeTypeCount = dfPokeTypes.select(
    col("poke.id"), col("poke.name"), col("type_name"), col("damage_to_name")
).groupby(
    col("poke.id"), col("poke.name"), col("type_name"), col("damage_to_name")
).count().orderBy(desc("poke.name"))

Q6_df = dfTypes.select("type_name").groupBy("type_name").count()

Q6_df2 = dfPokeTypeCount.alias("a").join(
    Q6_df.alias("b"), col("b.type_name") == col("a.damage_to_name"), "inner"
).select(
    col("a.name").alias("Pokemon"), col("b.count").alias("Qtd"),
).orderBy(desc("a.name"))

display(Q6_df2.groupBy("Pokemon").agg(sum("Qtd").alias("Soma")).orderBy(desc("Soma")).limit(10))
