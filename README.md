# MapReduce

Prácticas sobre MapReduce hechas en la asignatura SGDI del Máster en Ingeniería Informática de la UCM durante el curso 2015/16.

Escritas tanto en Python, como en Java. Para las versiones de Python es necesario instalar el módulo mrjob (https://pythonhosted.org/mrjob/guides/writing-mrjobs.html).



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

## Authors

* Jorge Casas Hernan (https://github.com/JorgeKsas)
* Mariano Hernandez Garcia (https://github.com/mariano2AA3)