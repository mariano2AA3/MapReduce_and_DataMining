 #
 #	Asignatura:   DGPSI (Master Ingenieria Informatica)
 #	Practica:     1
 #	Autores:      Jorge Casas Hernan y Mariano Hernandez Garcia
 #	Declaracion:  Declaramos que hemos realizado este documento nosotros
 #		      mismos de manera exclusiva y sin compartir nada con
 #		      otros grupos. 
 #

from mrjob.job import MRJob


class MRTemperatura(MRJob):

	# Fase MAP (line es una cadena de texto)
	def mapper(self, key, line):
		# Si lo que le llega a este nodo no es la cabecera de este csv...
		values = line.split(",")
		yield values[2], values[8]	

	# Fase REDUCE (key es una cadena texto, values un generador de valores)
	def reducer(self, key, values):
		mint = float("inf")
		maxt = float("-inf")
		for t in values:
			temp = float(t)
			if temp > maxt:
				maxt = temp
			if temp < mint:
				mint = temp
		yield key, "({0}, {1})".format(mint, maxt)


if __name__ == '__main__':
    MRTemperatura.run()
