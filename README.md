# MapReduce and Data Mining

Prácticas sobre MapReduce y algunos algoritmos de Minería de datos hechos en la asignatura SGDI del Máster en Ingeniería Informática de la UCM durante el curso 2015/16.

Escritas tanto en Python, como en Java. Para las versiones de Python es necesario instalar el módulo mrjob (https://pythonhosted.org/mrjob/guides/writing-mrjobs.html).

# MapReduce

## 1 - Temperatura

### Entrada

Fichero JCMB_last31days.csv (http://www.ed.ac.uk/schools-departments/geosciences/weather-station/download-weather-
data). Reúne los datos recogidos por una estación meteorológica de la Universidad de Edimburgo.

### Salida

Número de día  (temperatura mínima, temperatura máxima)

## 2 - Felicidad

### Entrada

Fichero happiness.txt (http://arxiv.org/abs/1101.5120) que recoge la valoración de felicidad de 10000 palabras inglesas, organizado de la siguiente manera:

word happiness_rank happiness_average happiness_standard_deviation
twitter_rank google_rank nyt_rank lyrics_rank

### Salida

Una sóla línea que contiene aquellas palabras extremadamente tristes, siendo esto que su happiness_average sea inferior a 2 con twitter_rank distinto de --

## 3 - Índice invertido

### Entrada

Cualquier fichero de texto plano.

### Salida

Aquellas palabras que aparezcan más de 20 veces, junto al fichero al que pertenecen y el número de ocurrencias.

## 4 - Log web

### Entrada

Fichero weblog.txt (http://ita.ee.lbl.gov/html/contrib/EPA-HTTP.html) con la siguiente estructura:

* host que realiza la petición (nombre de host o dirección IP)
* fecha en el formato [DD:HH:MM:SS], donde DD es el día y HH:MM:SS es la hora.
* Petición entre comillas. ¡Atención! La petición suele estar formada por 3 partes separadas por espacios: verbo HTTP (GET, POST, HEAD...), el recurso accedido y, opcionalmente, la versión de HTTP usada. El recurso accedido puede contener espacios y comillas.
* Código HTTP de respuesta: 200, 404, 302...
* Número de bytes de la contestación

### Salida

* Numero total de peticiones realizadas
* Tamaño total de los archivos servidos
* Número de peticiones que han recibido un error 4xx o 5xx
 
# Data Mining

## Clasificador K-nn

* read_file (filename)
Lee el fichero de texto de ruta filename formado por instancias en formato CSV y devuelve una lista de instancias representadas cada una como una lista. Supondremos que todos los atributos son continuos salvo la clase (último atributo) que es categórico. El resultado de invocar a la función read_file será una lista como:
[ [1.8, 3.9, 4.8, 'good'],
[2.0, 5.9, 4.7, 'bad' ],
[5.0, 1.2, 2.8, ' excellent ' ] ]

* knn(k, i , c)
Dado un valor de k, una instancia nueva i y un conjunto de entrenamiento c, devuelve la clase predicha para i utilizando únicamente la moda de la clase de los k vecinos más cercanos. No es necesario aplicar normalización a los atributos del conjunto de entrenamiento. La instancia i tendrá tantos atributos como las instancias del conjunto de entrenamiento c, pudiendo tener o no una clase asociada.

* test (k, trainset , testset )
Dado un valor de k, un conjunto de entrenamiento trainset y un conjunto de test testset calcula la proporción [0..1] de instancias de testset correctamente clasificadas utilizando la función knn.

## K-means

Algoritmo de clustering. Funciones:

* readfile (filelename)
Similar a la función utilizada en el apartado anterior salvo que ahora no existe atributo categórico clase. Por lo tanto tras la cabecera todas las instancias tienen el mismo número de atributos. Todos los atributos serán continuos.

* kmeans(k, instancias , centroides ini =None)
Recibe un valor de k, una lista de instancias (con el mismo formato que para k-NN ) y una lista de k centroides iniciales ( centroides ini ); y devuelve una agrupación aplicando el algoritmo de k-means. Concretamente el resultado es una pareja ( clustering , centroides ) tal que:

  * clustering es un diccionario representando el agrupamiento. Por ejemplo para k=3 y 5 instancias:
{ [ inst1 , inst2 , inst3 ],
[ inst4 ],
[ inst5 ] }
  * centroides es la lista con los centroides de los k clústeres finales. centroide [ i ] será el centroide del clúster número i.
Si centroides ini está vacío (su valor es None) entonces se escogerán los centroides iniciales intentando que estén lo más alejados posible entre sí.

El algoritmo será:

a) Se añade instancias [0] como primer centroide.
b) Elijo como siguiente centroide aquella instancia cuya minima distancia a los demás centroides es la máxima.
c) Termino cuando tengo k centroides.

### ID3

Algoritmo de clasificación TDIDT con ganancia de información para generación de árboles de clasificación. Supondremos que las instancias tienen todos los atributos categóricos, los ficheros siguen el formato CSV y que todos los valores de los atributos aparecen en el conjunto de entrenamiento. Para la generación del árbol se debe usar la ganancia de información (o disminución de
entropía) como criterio de selección de atributos.

Este algoritmo hace uso del programa xdot (https://github.com/jrfonseca/xdot.py) para la generación de la salida.

Para utilizarlo ejecutar:

```
python id3.py csv_file.csv output.dot

```

# Authors

* Jorge Casas Hernan (https://github.com/JorgeKsas)
* Mariano Hernandez Garcia (https://github.com/mariano2AA3)
