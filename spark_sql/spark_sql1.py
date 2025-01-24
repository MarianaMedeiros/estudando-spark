from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark.sql("show databases").show()

spark.sql("create database desp")

spark.sql("use desp").show()

arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv("/home/vboxuser/download/despachantes.csv", header=False, schema=arqschema)

despachantes.write.saveAsTable("Despachantes")

spark.sql("select * from Despachantes").show()

spark.sql("show tables").show()

despachantes.write.mode("overwrite").saveAsTable("Despachantes") #este modo sobrescreve a tabela

# por exemplo, se eu encerrar a sessão do spark, o dataframe despachantes se perde
# mas a tabela gerenciada é persistida
# desta forma, se eu quisesse obter novamente o dataframe, porém a partir da tabela gerenciada:
spark.sql("use desp").show()
despachantes = spark.sql("select * from despachantes")
despachantes.show()

# para criar uma tabela externa não gerenciada, a gente informa o caminho (path)
despachantes.write.format("parquet").save("/home/vboxuser/desparquet") # aqui gera um arquivo parquet

# como versões mais recentes do spark impedem sobrescrita acidental de dados 
# precisa fazer essa configuração adicional para permitir criar a tabela no mesmo path
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

despachantes.write.option("path", "/home/vboxuser/desparquet").saveAsTable("despachantes_ng") # aqui gera a tabela de fato, o comando é similar ao do arquivo

spark.sql("select * from despachantes_ng").show()

# verificar o create table da tabela gerenciada
spark.sql("show create table Despachantes").show(truncate=False)

spark.sql("show create table Despachantes_ng").show(truncate=False) # a tabela não gerenciada tem um LOCATION com o path dela

spark.catalog.listTables() # no retorno desse método, o atribuo "tableType" diz de é MANAGED ou EXTERNAL