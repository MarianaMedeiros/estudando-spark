from pyspark.sql.functions import sum

schema2 = "Produtos STRING, Vendas INT"
vendas = [["Caneta", 10],["Lápis", 20],["Caneta",40]]

df3 = spark.createDataFrame(vendas, schema2)
df3.show(1) # show top 1

agrupado = df3.groupBy("Produtos").agg(sum("Vendas"))
agrupado.show()

df3.select("Produtos", "Vendas").show() # Filtra apenas as colunas que queira ver
df3.select("Vendas", "Produtos").show()

from pyspark.sql.functions import expr

df3.select("Produtos", "Vendas", expr("Vendas * 0.2")).show()