# instalacion
# !apt-get install openjdk-8-jdk-headless -qq > /dev/null

# # Descargar Spark 3.2.2
# !wget -q http://apache.osuosl.org/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz

# # Descomprimir el archivo descargado de Spark
# !tar xf spark-3.2.2-bin-hadoop3.2.tgz
# !pip install -q findspark

# # Instalar pyspark
# !pip install -q pyspark




import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

# Probando la sesión de Spark
df = spark.createDataFrame([{"Hola": "Mundo"} for x in range(10)])

df.show(10, False)

#Spark sesion



import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("loca[*]").appName("pedro").getOrCreate()

spark

# Que es un RDD  Resilient Distributed Dataset

# Crear RDD

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext

rdd_vacio = sc.emptyRDD

rdd_vacio2 = sc.parallelize([],3)

rdd_vacio2.getNumPartitions()

rdd = sc.parallelize([1,2,3,4,7])

rdd

rdd.collect()

# Crear rdd apartir de un archivo de texo

rdd_texto = sc.textFile('./rdd_source.txt')
rdd_texto.collect()

rdd_texto_completo =sc.wholeTextFiles('./rdd_source.txt')
rdd_texto_completo.collect()

rdd_suma = rdd.map(lambda x: x + x)
rdd_suma.collect()

df = spark.createDataFrame([(1, 'pedro'),(2, 'Lilian'), (3, 'Carmina')], ['id', 'nombre'] )
df.show()

rdd_df = df.rdd
rdd_df.collect()

#Map

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
rdd = sc.parallelize([1,2,3,4,5])
rdd_par = rdd.map(lambda x: x % 2 == 0)

rdd_par.collect()

rdd_texto = sc.parallelize(['jose', 'juan', 'lucia'])

rdd_mayuscula = rdd_texto.map(lambda x: x.upper())

rdd_mayuscula.collect()

rdd_hola = rdd_texto.map(lambda x: 'Hola ' + x)

rdd_hola.collect()


# FlatMap


import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5])

rdd_cuadrado = rdd.map(lambda x: (x, x ** 2))

rdd_cuadrado.collect()

rdd_cuadrado_flat = rdd.flatMap(lambda x: (x, x ** 2))

rdd_cuadrado_flat.collect()

rdd_texto = sc.parallelize(['jose', 'juan', 'lucia'])

rdd_mayuscula = rdd_texto.flatMap(lambda x: (x, x.upper()))

rdd_mayuscula.collect()



#Filter

# Nueva sección
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])

rdd_par = rdd.filter(lambda x: x % 2 == 0)

rdd_par.collect()

rdd_impar = rdd.filter(lambda x: x % 2 != 0)

rdd_impar.collect()

rdd_texto = sc.parallelize(['jose', 'juaquin', 'juan', 'lucia', 'karla', 'katia'])

rdd_k = rdd_texto.filter(lambda x: x.startswith('k'))

rdd_k.collect()

rdd_filtro = rdd_texto.filter(lambda x: x.startswith('j') and x.find('u') == 1)

rdd_filtro.collect()

# Coalesce

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1,2,3.4,5], 10)

rdd.getNumPartitions()

rdd.coalesce(5)
rdd.getNumPartitions()

rdd5 = rdd.coalesce(5)

rdd5.getNumPartitions()

#Repartition

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5], 3)

rdd.getNumPartitions()

rdd7 = rdd.repartition(7)

rdd7.getNumPartitions()

#ReduceByKey

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(
    [('casa', 2),
     ('parque', 1),
     ('que', 5),
     ('casa', 1),
     ('escuela', 2),
     ('casa', 1),
     ('que', 1)]
)

rdd.collect()

rdd_reduciodo = rdd.reduceByKey(lambda x,y: x + y)

rdd_reduciodo.collect()

#Ejercicios

from pandas.core.array_algos import replace
from pandas.core.arrays.string_arrow import pa
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


# Cree un RDD llamado lenguajes que contenga los siguientes lenguajes de programación: Python, R, C, Scala, Rugby y SQL.
lenguajes = sc.parallelize(['Python', 'R', 'C', 'Scala', 'Rugby', 'SQL' ])


# Obtenga un nuevo RDD a partir del RDD lenguajes donde todos los lenguajes de programación estén en mayúsculas.

rdd_mayusculas = lenguajes.map(lambda x: x.upper())
rdd_mayusculas.collect()

# Obtenga un nuevo RDD a partir del RDD lenguajes donde todos los lenguajes de programación estén en minúsculas.

rdd_minusculas = lenguajes.map(lambda x: x.lower())
rdd_minusculas.collect()

# Cree un nuevo RDD que solo contenga aquellos lenguajes de programación que comiencen con la letra R.
rdd_r = lenguajes.filter(lambda x: x.startswith('R'))
rdd_r.collect()

# Cree un RDD llamado pares que contenga los números pares existentes en el intervalo [20;30].
pares = sc.parallelize([20, 21, 22, 23 , 24, 25, 26, 27, 28 ,29, 30]).filter(lambda x: x % 2 == 0)

pares.collect()

# Cree el RDD llamado sqrt, este debe contener la raíz cuadrada de los elementos que componen el RDD pares.
import math
sqrt = pares.map(lambda x: math.sqrt(x))
sqrt.collect()


# Obtenga una lista compuesta por los números pares en el intervalo [20;30] y sus respectivas raíces cuadradas. Un ejemplo del resultado deseado para el intervalo [50;60] sería la lista [50, 7.0710678118654755, 52, 7.211102550927978, 54, 7.3484692283495345, 56, 7.483314773547883, 58, 7.615773105863909, 60, 7.745966692414834].
lista = pares.flatMap(lambda x: (x, math.sqrt(x))).collect()
print('sqrt',lista)


# Eleve el número de particiones del RDD sqrt a 20.
sqrt20 = sqrt.repartition(20)
sqrt20.getNumPartitions()


# Si tuviera que disminuir el número de particiones luego de haberlo establecido en 20, ¿qué función utilizaría para hacer más eficiente su código?
sqrtDiminished = sqrt20.coalesce(5)
sqrtDiminished.getNumPartitions()


# Cree un RDD del tipo clave valor a partir de los datos adjuntos como recurso a esta lección. Tenga en cuenta que deberá procesar el RDD leído para obtener el resultado solicitado. Supongamos que el RDD resultante de tipo clave valor refleja las transacciones realizadas por número de cuentas. Obtenga el monto total por cada cuenta.
rddFile = sc.textFile('./transacciones')
rddFile.collect()

def procesoCV(s):
    return (s.replace('(', '').replace(')', '').split(', '))

rddCV = rddFile.map(procesoCV)
rddCV.collect()

# Tip: Cree su propia función para procesar el RDD leído.
rddCV.reduceByKey(lambda x, y: float(x) + float(y)).collect()
