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
from collections import Counter
import math
import pprint
import sys

def read_file ( filename ):

	# Lista de instancias representadas como listas
	inst = []  			
	# Diccionario con los valores y posicion de cada atributo dentro de la instancia
	attrib_dic = {}	
	# Lista con los nombres de las posibles clases
	classes = []	

	infile = open(filename, 'r')
	reader = csv.DictReader(infile)

	# Leemos la cabecera del csv
	if csv.Sniffer().has_header(filename):
		ind = 0
		head_data = infile.readline()[:-1].split(",")

		# Creamos el diccionario con los valores y posicion de cada atributo
		for hd in head_data:
			attrib_dic[hd] = (ind, [])
			ind += 1
		
		# Movemos la posicion de lectura al principio del fichero
		infile.seek(0 ,0)

		# Recorremos el diccionario del fichero leido...
		for data in reader:

			# ... la lista de instancias
			l = []
			for hd in head_data:
	
				l.append(data[hd])
				if data[hd] not in attrib_dic[hd][1]:
					attrib_dic[hd][1].append(data[hd])
					
			inst.append(l)

		# Rellenamos las clases y borramos la clase de la lista de atributos
		classes = attrib_dic["class"][1]
		del attrib_dic["class"]

	else:
		print "Error en el formato del csv: falta la cabecera"
		infile.close()
		return None
		
	infile.close()
	return (inst, attrib_dic, classes)


# Devuelve un diccionario donde para cada clase indica el numero de veces que aparece en el conjunto y la clase mas comun
def class_ocurrences(inst, classes):
	classes_counter = {}
	class_candidate = None
	ocu_class_candidate = 0

	for value in classes:
		classes_counter[value] = 0

	for ins in inst:
		classes_counter[ins[-1]] += 1
		if classes_counter[ins[-1]] > ocu_class_candidate:
			ocu_class_candidate = classes_counter[ins[-1]]
			class_candidate = ins[-1]
			
	return (classes_counter, class_candidate)



# Crea un nuevo conjunto a partir de inst en el cual el atributo attr es igual a value_attr.
def partition_set(inst, attrib_dic, attr, value_attr):
	new_inst = []
	for ins in inst:
		if ins[attrib_dic[attr][0]] == value_attr:
			new_inst.append(ins)
	return new_inst


# Devuelve el mejor atributo (atributo con mayor ganancia -> menor entropia).
def select_attribute(inst, attrib_dic, classes, candidates):
	attr_candidate = None
	entropia_attr_c = float("Inf")

	for attr in candidates:
		entropia_total = 0.0

		for value_attr in attrib_dic[attr][1]:
			entropia_partial = 0.0
			partition = partition_set(inst, attrib_dic, attr, value_attr)
			(classes_counter, class_mc) = class_ocurrences(partition, classes)

			for class_c in classes:
				if classes_counter[class_c] != 0:
					o = (1.0 * classes_counter[class_c]) / len(partition)
					entropia_partial -= o * math.log(o, 2)

			entropia_total += entropia_partial * len(partition)/len(inst)

		if entropia_total < entropia_attr_c:
			attr_candidate = attr
			entropia_attr_c = entropia_total

	return attr_candidate


# Algoritmo TDIDT (ID3 para la seleccion del atributo)
# return:
#		 [ Valor, [ (arista1, hijo1) , (arista2, hijo2), ... (aristaN, hijoN) ] ]
def id3(inst, attrib_dic, classes, candidates):
	(classes_counter, class_mc) = class_ocurrences(inst, classes)

	# Si todas las instancias del conjunto inst son de una clase en particular o
	# Si la lista de candidatos candidates es vacia...
	if len(candidates) == 0 or classes_counter[class_mc] == len(inst):
		# Creamos una hoja (un nodo sin hijos)
		return (class_mc, [])

	# Seleccionamos el mejor atributo segun id3 (menor entropia)
	attr_selected = select_attribute(inst, attrib_dic, classes, candidates);
	node = (attr_selected, [])

	# Para cada valor del atributo seleccionado...
	for attr_value in attrib_dic[attr_selected][1]:	

		partition = partition_set(inst, attrib_dic, attr_selected, attr_value)
		sheet = None

		if len(partition) == 0:
			sheet = (class_mc, [])

		else:
			candidates_aux = candidates[:]
			candidates_aux.remove(attr_selected)
			sheet = id3( partition, attrib_dic, classes, candidates_aux )

		node[1].append( (attr_value, sheet ) )

	return node


# Recibe un arbol de clasificacion id3_tree como lo genera la funcion
# id3 y crea un fichero .dot de nombre filename.
def write_dot_tree( id3_tree, filename ):
	node_list = []
	arist_list = []
	write_dot_tree_aux(id3_tree, "0", node_list, arist_list)
	
	# Escribimos la informacion en el fichero.
	outfile = open(filename, 'w')
	outfile.write("digraph tree {\n\t//nodos\n")
	for node_str in node_list:
		outfile.write("\t" + node_str + "\n")
	outfile.write("\n\t//aristas\n")
	for arist_str in arist_list:
		outfile.write("\t" + arist_str + "\n")
	outfile.write("}\n")
	outfile.close()


def write_dot_tree_aux(node, id_father, node_list, arist_list):

	if len(node[1]) == 0:
		node_str = "{0}{1} [label=\"{0}\"];".format(node[0], id_father)
		if node_str not in node_list:
			node_list.append(node_str)

	else:
		id_son = 0
		node_str = "{0}{1} [label=\"{0}\" shape=\"box\"];".format(node[0], id_father)
		if node_str not in node_list:
			node_list.append(node_str)
		for data in node[1]: # data[0] -> Arista, data[1] -> Nodo hijo
			arist_str = "{0}{1} -> {2}{1}{3} [label=\"{4}\"];".format(node[0], id_father, data[1][0], id_son, data[0])
			if arist_str not in arist_list:
				arist_list.append(arist_str)
			write_dot_tree_aux(data[1], id_father + str(id_son), node_list, arist_list)
			id_son += 1

if len(sys.argv) != 3:
	print "Usage: " + sys.argv[0] + " <input csv file> <output dot file>"
else:
	input_data = read_file(sys.argv[1]) 
	id3_tree = id3(input_data[0], input_data[1], input_data[2], input_data[1].keys())
	write_dot_tree(id3_tree, sys.argv[2])
