import sys
import logging

from pyspark.sql import SparkSession

from jobs import process_data

# App info, para debugs
LOG_FILENAME = 'dataproc.log'
APP_NAME = "CondoManage_processing"


if __name__ == '__main__':

    # Logs
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(sys.argv)

    # Check inputs
    if len(sys.argv) is not 6:
        logging.warning("GCS path ex: gs://bucket/pastas/* - 1 condominios, 2 moradores, 3 imoveis, 4 transacoes, 5 output path")
        sys.exit(1)

    condominios_input_path = sys.argv[1]
    moradores_input_path = sys.argv[2]
    imoveis_input_path = sys.argv[3]
    transacoes_input_path = sys.argv[4]
    output_path = sys.argv[5]

    # SparkSession
    spark = SparkSession.builder.appName("CondoManage_processing") \
        .config('spark.jars.packages',"org.apache.spark:spark-avro_2.12:3.4.0,org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.39.1,com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.1.3,com.google.guava:guava:r05") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar").getOrCreate()

    logging.info(f"Application Initialized: {APP_NAME}")

    process_data(spark, condominios_input_path, 
                 moradores_input_path,
                 imoveis_input_path,
                 transacoes_input_path,
                 output_path)

    logging.info(f"Application Done: {APP_NAME}")
    spark.stop()


