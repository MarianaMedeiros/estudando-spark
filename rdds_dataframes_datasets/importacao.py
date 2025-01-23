par = spark.read.format("parquet").load("/home/vboxuser/dfparquet/despachantes.parquet")
par.show()
par.schema

js = spark.read.format("json").load("/home/vboxuser/dfjson/despachantes.json")
js.show()
js.schema

csv = spark.read.format("csv").load("/home/vboxuser/dfcsv/despachantes.csv")
csv.show()
csv.schema

orc = spark.read.format("orc").load("/home/vboxuser/dforc/despachantes.orc")
orc.show()
orc.schema

# Definindo o schema para o csv
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
csv2 = spark.read.csv("/home/vboxuser/dfcsv/despachantes.csv", header=False, schema=arqschema)
csv2.show()
csv2.schema