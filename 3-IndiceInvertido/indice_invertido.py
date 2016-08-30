 #
 #  Asignatura:     DGPSI (Master Ingenieria Informatica)
 #  Practica:       1
 #  Autores:        Jorge Casas Hernan y Mariano Hernandez Garcia
 #  Declaracion:    Declaramos que hemos realizado este documento nosotros
 #                  mismos de manera exclusiva y sin compartir nada con
 #                  otros grupos. 
 #

from mrjob.job import MRJob
import os
import operator


class MRIndiceInvertido(MRJob):

	# Fase MAP (line es una cadena de texto)
	def mapper(self, key, line):
		values = line.split()
                for w in values:
                    word = w.lower()
                    word = word.replace(",", "").replace(":", "").replace("-", "").replace("!", "").replace("?", "").replace("~", "").replace(".", "").replace(";", "").replace("'", "").replace("\"", "")
                    yield word, os.environ["map_input_file"]

	# Fase REDUCE (key es una cadena texto, values un generador de valores)
	def reducer(self, key, values):
                dic = {}
                # Contamos el numero de apariciones de cada palabra por libro.
		for book_name in values:
                    if book_name in dic.keys():
                        dic[book_name] += 1
                    else:
                        dic[book_name] = 1 

                results = ""
                morethan20 = False
                # De la lista ordenada de apariciones por libro obtenida del diccionario anterior formateamos la salida.
                for book_name in sorted(dic, key=dic.get, reverse=True):
                    if dic[book_name] >= 20:
                        morethan20 = True
                    results += "({0}, {1}), ".format(book_name, dic[book_name])

                if morethan20:
                    yield key, results[:-2]

if __name__ == '__main__':
    MRIndiceInvertido.run()
