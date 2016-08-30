 #
 #	Asignatura:    DGPSI (Master Ingenieria Informatica)
 #	Practica:      1
 #	Autores:       Jorge Casas Hernan y Mariano Hernandez Garcia
 #	Declaracion:   Declaramos que hemos realizado este documento nosotros
 #		       mismos de manera exclusiva y sin compartir nada con
 #		       otros grupos. 
 #

from mrjob.job import MRJob


class MRFelicidad(MRJob):

	# Fase MAP (line es una cadena de texto)
	def mapper(self, key, line):
		values = line.split("\t")
		if float(values[2]) < 2.0 and values[5] != "--":
			yield 1, values[0]	

	# Fase REDUCE (key es una cadena texto, values un generador de valores)
	def reducer(self, key, values):
		sol = ""
		counter = 0
		for word in values:
			sol += word + ", "
			counter += 1
		
		# Quitamos la coma y el espacio finales 	
		sol = sol[:-2]
		yield counter, sol


if __name__ == '__main__':
    MRFelicidad.run()
