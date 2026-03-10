from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import time

spark = SparkSession.builder.appName("BenchmarkSpark").getOrCreate()

print("⏳ Iniciando Benchmark SPARK (1M registros)...")
inicio = time.time()

df = spark.read.parquet("/home/jovyan/work/dados_gigantes_limpos.parquet")

resultado = df.filter(col("valor") > 1000) \
    .groupBy("estado_loja") \
    .agg(
        _sum("is_fraud").alias("total_fraudes"),
        _sum("valor").alias("volume_financeiro")
    )

resultado.show(5)

fim = time.time()
tempo_total = fim - inicio

print(f"\n✅ SPARK FINALIZADO!")
print(f"⏱️ Tempo total: {tempo_total:.2f} segundos")
spark.stop()