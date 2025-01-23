# Principais Ações e Transformações

from pyspark.sql import functions as func

# Primeiro definindo o schema do dataframe
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv("/home/vboxuser/download/despachantes.csv", header=False, schema=arqschema)

# Ações
despachantes.show() # obtém os dados de forma tabular
despachantes.collect() # retorna todos os elementos para o driver como uma lista
despachantes.take(1) # take(n) retorna os primeiros n elementos
despachantes.count() # conta o numero de elementos

despachantes.orderBy("vendas").show()
despachantes.orderBy(func.col("vendas").desc()).show()
despachantes.orderBy(func.col("cidade").desc(), func.col("vendas").desc()).show()

despachantes.groupBy("cidade").agg(sum("vendas")).show()

despachantes.groupBy("cidade").agg(sum("vendas")).orderBy(func.col("sum(vendas)").desc()).show()

despachantes.filter(func.col("nome") == "Deolinda Vilela").show()