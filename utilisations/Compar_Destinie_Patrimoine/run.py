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

pdb.set_trace()
test = read_hdf(path_data + 'Cohort_10000.h5', 'entities//person')


pat = read_hdf(path_data + 'Patrimoine_0.h5', 'entities//person')
dest = read_hdf(path_data + 'Destinie.h5', 'entities//person')

for table in [pat, dest]:
    taille = len(table)
    # age 
    age = table['agem'].value_counts()*100/taille
    