from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# No Docker, não precisamos de master("local[*]") obrigatoriamente, 
# mas mantê-lo não faz mal.
spark = SparkSession.builder \
    .appName("SumUp-Fraud-Docker") \
    .getOrCreate()

# O caminho dentro do container será /home/jovyan/work/
path = "/home/jovyan/work/dados_fraude_limpos.parquet"

df = spark.read.parquet(path)
print("\n✅ SUCESSO! O Spark rodou dentro do Container:")
df.groupBy("estado_loja").count().orderBy("count", ascending=False).show(5)

df.filter(df.is_fraud == 0) \
  .groupBy("tipo_negocio") \
  .avg("valor") \
  .show()

spark.stop()