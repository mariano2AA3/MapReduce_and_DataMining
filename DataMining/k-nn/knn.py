
 #
 #	Asignatura:   DGPSI (Master Ingenieria Informatica)
 #	Practica:     2
 #	Autores:      Jorge Casas Hernan y Mariano Hernandez Garcia
 #	Declaracion:  Declaramos que hemos realizado este documento nosotros
 #		      mismos de manera exclusiva y sin compartir nada con
 #		      otros grupos. 
 #


# -*- coding: utf-8 -*-
import csv
from scipy.spatial import distance
import Queue as queue
from collections import Counter

def read_file( filename ):
	datalist = []
	infile = open(filename, 'r')
	reader = csv.reader(infile)
	if csv.Sniffer().has_header(filename):
		next(reader, None)
	for row in reader:
		for i in range(4):
			row[i] = float(row[i])
		datalist.append(row)

	return datalist

def knn( k, i, c ):
	# Creamos una cola de prioridad de tamanio el conjunto de entrenamiento c (numero total de muestras).
	priority_queue = queue.PriorityQueue(len(c))
	k_classes = []

	# Metemos las distancias entre i y cada una de las instancias del conjunto de entrenamiento en la cola de prioridad
	# (junto con su clase)
	for instance in c:
		priority_queue.put((distance.euclidean(i[:4], instance[:4]), instance[4]))

	# Obtemos las k clases mas cercanas (usando la cola de prioridad) y lo almacenamos en un array
	for x in range(min(k, len(c))):
	 	k_classes.append(priority_queue.get()[1])
		
	# Devolvemos la moda de dicho array
	return Counter(k_classes).most_common(1)[0][0]

def test( k, trainset, testset ):
	nr_success = 0.0
	for i in testset:
		if knn(k, i, trainset) == i[-1]:
			nr_success += 1
	return nr_success / len(testset)

print test(11, read_file("iris.csv"), read_file("iris_test.csv"))
