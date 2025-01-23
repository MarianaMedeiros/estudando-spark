from pyspark.sql import functions as func

df_clientes = spark.read.format("parquet").load("/home/vboxuser/download/Atividades/Clientes.parquet")
ex1 = df_clientes.select("Cliente", "Estado", "Status")

ex2 = df_clientes.select("*").where((func.col("Status") == "Platinum") | (func.col("Status") == "Gold"))

df_vendas = spark.read.format("parquet").load("/home/vboxuser/download/Atividades/Vendas.parquet")

ex3 = df_vendas.join(df_clientes, df_vendas.ClienteID == df_clientes.ClienteID).groupBy(df_clientes.Status).agg(sum("Total")).orderBy(func.col("sum(Total)").desc())
