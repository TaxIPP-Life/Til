# -*- coding:utf-8 -*-
''' 
Ce programme compare la base Destinie et la base générée directement à partir de l'enquête patrimoine.

Les tables issues de Destinie et de Patrimoine doivent déjà avoir été créées 
    - data//data//Destinie//Patrimoine.py et data//data//Destinie//Destinie.py
    Pour Patrimoine, ça ne sert à rien de duppliquer pour comparer donc on part de la version Patrimoine_0
'''

import pdb

from pgm.CONFIG import path_til
from pandas import read_hdf, HDFStore
import tables
path_data = path_til + 'Model\\'

# retourne une ficher stat qui permet la comparaison entre deux tables
# d'abord on récupéère les stat de chaque table, 
# ensuite on fait la différence. 

list_var_num = ['agem']

list_subtable = {'':'sexe == [0,1,2]', '_f':'sexe == [1]', '_h':'sexe != [1]'}

stat = {}

pat = read_hdf(path_data + 'Patrimoine_300.h5', 'entities//person')
dest = read_hdf(path_data + 'Destinie.h5', 'entities//person')
table = pat

table1 = 'Patrimoine_400'
table2 = 'Destinie'

def get_stat_on_numeric(name, table):
    var = table[name]
    return [var.mean(), var.max(), var.min(), var.std(), var.quantile(0.1), var.quantile(0.5), var.quantile(0.75)]


for table_name in [table1, table2]:
    stat[table_name] = {}
    table = read_hdf(path_data + table_name + '.h5', 'entities//person')
    for suffix, cond in list_subtable.iteritems():
        subtable = table.query(cond)
        for var in list_var_num:
            stat[table_name][var + suffix] = get_stat_on_numeric(var, subtable)
    


### diff

pdb.set_trace()
diff = {}
for var in stat[table1].keys():
    diff[var] = [y - x for x,y in zip(stat[table2][var], stat[table1][var])]

pdb.set_trace()
#     
#     
#     age = table['agem'].value_counts()*100/taille
# 
# for table in [pat, dest]:
#     taille = len(table)
#     # age 
#     age = table['agem'].value_counts()*100/taille
#     