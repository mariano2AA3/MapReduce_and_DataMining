 #
 #  Asignatura:     DGPSI (Master Ingenieria Informatica)
 #  Practica:       1
 #  Autores:        Jorge Casas Hernan y Mariano Hernandez Garcia
 #  Declaracion:    Declaramos que hemos realizado este documento nosotros
 #                  mismos de manera exclusiva y sin compartir nada con
 #                  otros grupos. 
 #


from mrjob.job import MRJob


class MRLogWeb(MRJob):

	# Fase MAP (line es una cadena de texto)
	def mapper(self, key, line):
		values = line.split()
		d = values[len(values)-1]
		
		if d == "-":
			d = "0"
			
		yield values[0], [d, values[len(values)-2]]
		
	# Fase COMBINER (values es un generador de arrays)
	def combiner(self, key, values):
		req_count = 0
		bytes_count = 0
		num_errors = 0
		for data in values:
			req_count += 1
			bytes_count += int(data[0])
			if data[1][0] == "4" or data[1][0] == "5":
				num_errors += 1
			
		yield key, [req_count, bytes_count, num_errors]

	# Fase REDUCE (key es una cadena texto, values un generador de arrays)
	def reducer(self, key, values):
		req_count = 0
		bytes_count = 0
		num_errors = 0
		for data in values:
			req_count += data[0]
			bytes_count += data[1]
			num_errors += data[2]

		yield key, "({0}, {1}, {2})".format(req_count, bytes_count, num_errors)
		


if __name__ == '__main__':
    MRLogWeb.run()
