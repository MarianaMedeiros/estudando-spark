arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv("/home/vboxuser/download/despachantes.csv", header=False, schema=arqschema)

despachantes.write.format("parquet").save("/home/vboxuser/dfparquet")

despachantes.write.format("csv").save("/home/vboxuser/dfcsv")

despachantes.write.format("json").save("/home/vboxuser/dfjson")

despachantes.write.format("orc").save("/home/vboxuser/dforc")