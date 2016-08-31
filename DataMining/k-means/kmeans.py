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
import matplotlib.pyplot as plt
import Queue as queue
from collections import Counter

# El fichero no debe tener cabecera.
def read_file( filename ):
	datalist = []
	infile = open(filename, 'r')
	reader = csv.reader(infile)
	for row in reader:
		for i in range(len(row)):
			row[i] = int(row[i])
		datalist.append(row)

	infile.close()
	return datalist

def kmeans( k, instances, centroid_ini=None ):
	clustering = {}
	instances_cluster_map = [-1] * len(instances)
	centroids  = None
	change = True

	if centroid_ini is None:
		# Inicializamos los centroids con el algoritmo descrito en el enunciado de la practica
		centroids = [None] * k
		centroids[0] = instances[0][:]
		for i in range(1, k):
			max_distance = 0.0
			centroid_i = None
			for instance in instances:
				min_distance = float("Inf")
				instance_candidate = None
				for j in range(i):
					d = distance.euclidean(instance, centroids[j])
					if d < min_distance:
						min_distance = d
						instance_candidate = instance
				if min_distance > max_distance:
					max_distance = min_distance
					centroid_i = instance_candidate
			centroids[i] = centroid_i[:]
	else:
		centroids = centroid_ini

	# Mientras que alguna instancia cambie de cluster
	while change:
		# Inicializamos clustering
		clustering.clear()
		for i in range(k):
			clustering[i] = []

		change = False
		# Para cada instancia en C, la asignamos al cluster mas cercano
		for instance_i in range(len(instances)):
			min_distance = float("Inf")
			num_cluster = -1

			# Calculamos el cluster que le corresponde a la instancia-i 
			# (la distancia menor entre la instancia-i y los centroids)
			for ind in range(k):
				d = distance.euclidean(instances[instance_i], centroids[ind])
				if d < min_distance:
					min_distance = d
					num_cluster = ind
			
			if instances_cluster_map[instance_i] != num_cluster:
				instances_cluster_map[instance_i] = num_cluster
				change = True

			clustering[num_cluster].append(instances[instance_i])

		# Calcula la media aritmetica de todas las instancias de cada cluster para calcular su centroid
		for i in range(k):
			for j in range(len(centroids[0])):
				centroids[i][j] = 0.0
				for instance in clustering[i]:
					centroids[i][j] += instance[j]
				centroids[i][j] /= float(len(clustering[i]))

	return (clustering, centroids)

def cohesionCluster(cluster, centroid):
	radio = diameter = distance2average = 0.0
	for instance in cluster:
		d = distance.euclidean(centroid, instance)
		if d > radio:
			radio = d
		distance2average += (d*d)
		for instance2 in cluster:
			d2 = distance.euclidean(instance, instance2)
			if d2 > diameter:
				diameter = d2
	distance2average /= len(cluster)
	return (radio, diameter, distance2average)

def clusteringQualityEvaluation():
	min_k = 2
	max_k = 20
	instances = read_file( "customers.csv" )
	radio_data = []
	diameter_data = []
	dist2avg_data = []
	eje_x = range(min_k, max_k + 1)
	for k in eje_x:
		print "Calculando kmeans con k={0}\t({1}% completado)".format(k, int(((k - min_k)*100)/(max_k - min_k + 1)))
		(clustering, centroids) = kmeans(k, instances)
		sum_radio = sum_diameter = sum_dist2avg = 0.0
		for i in range(k):
			(r, d, d2a) = cohesionCluster(clustering[i], centroids[i])
			sum_radio += r
			sum_diameter += d
			sum_dist2avg += d2a
		radio_data.append(sum_radio / k)
		diameter_data.append(sum_diameter / k)
		dist2avg_data.append(sum_dist2avg / k)

	print "Terminado"
	plt.xlabel("Valor de k")

	plt.plot(eje_x, radio_data)
	plt.ylabel("Radio medio del clustering")
	plt.show()

	plt.plot(eje_x, diameter_data)
	plt.ylabel("Diametro medio del clustering")
	plt.show()

	plt.plot(eje_x, dist2avg_data)
	plt.ylabel("Distancia al cuadrado promedio")
	plt.show()

print kmeans(3, read_file("customers.csv"))
#clusteringQualityEvaluation()
