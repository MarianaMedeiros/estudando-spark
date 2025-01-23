from pyspark.sql import functions as func

# Primeiro definindo o schema do dataframe
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv("/home/vboxuser/download/despachantes.csv", header=False, schema=arqschema)

# Filtrando somente vendas acima de 20
despachantes.select("id", "nome", "vendas").where(func.col("vendas") > 20).show()

# Filtrar vendas > 20 e < 40
despachantes.select("id", "nome", "vendas").where((func.col("vendas") > 20) & (func.col("vendas") < 40)).show()

# Preciso criar um novo df para fazer uma alteração, já que df é IMUTÁVEL
novo_df = despachantes.withColumnRenamed("nome", "nomes")
novo_df.columns

from pyspark.sql.functions import *

despachantes2 = despachantes.withColumn("data2", to_timestamp(func.col("data"), "yyy-MM-dd"))
despachantes2.schema # a coluna data 2 foi adicionada ao schema com o tipo TimeStamp

despachantes2.select(year("data")).show()
despachantes2.select(year("data")).distinct().show()
despachantes2.select("nome", year("data")).orderBy("nome").show()

despachantes2.select("data").groupBy(year("data")).count().show()

despachantes2.select(func.sum("vendas")).show()