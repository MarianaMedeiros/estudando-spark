# criando um rdd:
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

####################################################
# Parte I - Exemplos de operacoes simples com RDDs #
####################################################

numeros.take(5) # retorna os 5 primeiros da ordem

numeros.top(5) # retorna os 5 maiores em ordem desc

numeros.collect() # retorna tudo 
#não recomendado usar isso com grandes volumes de dados

numeros.count()

numeros.mean()

numeros.sum()

numeros.max()

numeros.min()

numeros.stdev()

filtro = numeros.filter(lambda filtro: filtro > 2)
filtro.collect()

amostra = numeros.sample(True, 0.5, 1)
amostra.collect()

mapa = numeros.map(lambda mapa: mapa * 2)
mapa.collect()

numeros2 = sc.parallelize([6,7,8,9,10])
uniao = numeros.union(numeros2)
uniao.collect()

interseccao = numeros.intersection(numeros2)
interseccao.collect()

subtrai = numeros.subtract(numeros2)
subtrai.collect()

cartesiano = numeros.cartesian(numeros2)
cartesiano.collect()
cartesiano.countByValue()

####################################################
# Parte II - Key/Value/Tuplas, map, join, subtract #
####################################################

compras = sc.parallelize([(1,200),(2,300),(3,120),(4,250),(5,78)])

chaves = compras.keys()
chaves.collect()

valores = compras.values()
valores.collect()

compras.countByKey()

soma = compras.mapValues(lambda soma: soma + 1)
soma.collect()

debitos = sc.parallelize([(1,20),(2,300)])
resultado = compras.join(debitos)
resultado.collect()

semdebito = compras.subtractByKey(debitos)
semdebito.collect()