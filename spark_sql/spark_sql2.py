from pyspark.sql import SparkSession
from pyspark.sql.types import *

# criação de views

spark.sql("use desp").show()
despachantes = spark.sql("select * from despachantes")

# view temporaria
despachantes.createOrReplaceTempView("Despachantes_view1") # aqui a view é super simples, mas poderia ser uma view com filtros, joins etc

spark.sql("select * from Despachantes_view1")

despachantes.createOrReplaceGlobalTempView("Despachantes_view2") 

# usar o prefixo por padrão em views globais
spark.sql("select * from global_temp.Despachantes_view2").show()

# criando view temporaria usando cláusula SQL diretamente
spark.sql("CREATE OR REPLACE TEMP VIEW DESP_VIEW AS select * from despachantes")
spark.sql("select * from DESP_VIEW")

# criando view global com cláusula SQL diretamente
spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW DESP_VIEW2 AS select * from despachantes")
spark.sql("select * from DESP_VIEW2")