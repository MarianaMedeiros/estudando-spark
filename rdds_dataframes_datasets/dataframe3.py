from pyspark.sql.types import *

# Primeiro definindo o schema do dataframe
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv("/home/vboxuser/download/despachantes.csv", header=False, schema=arqschema)
despachantes.show()

# Depois, sem definir o schema e deixando o Spark fazer isso
desp_autoschema = spark.read.load("/home/vboxuser/download/despachantes.csv", header=False, format="csv", sep=",", inferSchema=True)
desp_autoschema.show() # as colunas vem nomeadas automaticamente como _c0, _c01

# Comparando os dois schemas:
desp_autoschema.schema
despachantes.schema