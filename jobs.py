

import logging
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col




def process_data(spark, condominios_input_path, 
                 moradores_input_path,
                 imoveis_input_path,
                 transacoes_input_path,
                 output_path
                 ):

    # Read GCS
    df_condominios = spark.read \
        .format("avro") \
        .option("recursiveFileLookup", "true") \
        .option("path", condominios_input_path) \
        .load()

    df_condominios = df_condominios.select(
        col("payload.condominio_id"),
        col("payload.nome"),
        col("payload.endereco")
    )

    df_moradores = spark.read \
        .format("avro") \
        .option("recursiveFileLookup", "true") \
        .option("path", moradores_input_path) \
        .load()

    df_moradores = df_moradores.select(
        col("payload.morador_id"),
        col("payload.nome"),
        col("payload.condominio_id"),
        col("payload.data_registro")
    )

    df_imoveis = spark.read \
        .format("avro") \
        .option("recursiveFileLookup", "true") \
        .option("path", imoveis_input_path) \
        .load()

    df_imoveis = df_imoveis.select(
        col("payload.imovel_id"),
        col("payload.tipo"),
        col("payload.condominio_id"),
        col("payload.valor")
    )

    df_transacoes = spark.read \
        .format("avro") \
        .option("recursiveFileLookup", "true") \
        .option("path", transacoes_input_path) \
        .load()

    df_transacoes = df_transacoes.select(
        col("payload.transacao_id"),
        col("payload.imovel_id"),
        col("payload.morador_id"),
        col("payload.data_transacao"),
        col("payload.valor_transacao")
    )

    # 
    trans_por_condo = df_transacoes.join(df_imoveis, "imovel_id") \
                                    .groupBy("condominio_id") \
                                    .agg(F.count("transacao_id").alias("total_transactions"))
    
    valor_por_resi = df_transacoes.join(df_moradores, "morador_id") \
                               .groupBy("morador_id") \
                               .agg(F.sum("valor_transacao").alias("total_value"))

    daily_trans_por_tipo = df_transacoes.join(df_imoveis, "imovel_id") \
                                       .groupBy(F.col("data_transacao").alias("date"), "tipo") \
                                       .agg(F.count("transacao_id").alias("total_transactions"),
                                            F.sum("valor_transacao").alias("total_value"))
    
    #
    today = datetime.today().strftime('%Y-%m-%d')
    today = today.replace('-', '/')

    # 
    trans_por_condo.write \
        .format("parquet") \
        .option("path", f"{output_path}/aggregate/{today}/transacoes_p_condominio/") \
        .mode("append") \
        .save()

    valor_por_resi.write \
        .format("parquet") \
        .option("path", f"{output_path}/aggregate/{today}/valor_p_residente/") \
        .mode("append") \
        .save()

    daily_trans_por_tipo.write \
        .format("parquet") \
        .option("path", f"{output_path}/aggregate/{today}/transacoes_diarias_p_tipo/") \
        .mode("append") \
        .save()
    
    return True
    

