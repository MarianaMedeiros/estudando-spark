from pyspark.sql import SparkSession

###################################################################
# Criando um DataFrame sem definir o schema                       #
# O spark automaticamente analisa os dados e descobre seus tipos  #
###################################################################

df1 = spark.createDataFrame([("Pedro", 10),("Maria", 20),("José", 40)])
df1.show()

############################################
# Criando um DataFrame definindo o schema  #
############################################

schema = "Id INT, Nome STRING"
dados = [[1,"Pedro"],[2,"Maria"]]
df2 = spark.createDataFrame(dados, schema)