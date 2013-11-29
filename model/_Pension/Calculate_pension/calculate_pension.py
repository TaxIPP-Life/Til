# -*- coding:utf-8 -*-
'''
Ce programme appel les fonctions de calcul des droits pour les différents régimes
'''
import pandas as pd
import numpy as np
from pgm.CONFIG import path_data_destinie

# Table des deroulés de carrières pour mise en place du programme (pas mensuel artificiel)
# Rq : emp_Destinie est le déroulé des carrières de Destinie mis en forme par Destinie.py
ind = np.genfromtxt(path_data_destinie + 'emp_Destinie.csv', delimiter =',')
#ind = pd.read_csv(path_data_destinie + 'emp_Destinie.csv', delimiter =';')

# Pour l'instant on va tester le calcul des droits à retraite sur 2013
ind_month = np.repeat(ind, 12, axis=0)
ind = pd.DataFrame(ind_month, columns = ['index', 'id', 'period', 'workstate', 'sali'])
ind = ind[['id', 'period', 'workstate', 'sali']]
id_list = ind.loc[ind['period'] == 2013, 'id'].drop_duplicates('id')
ind = ind[(ind['id'].isin(id_list))] 
ind = ind[ind['period'] < 2014 ]
ind = ind.astype(int)
ind = ind.sort(['id', 'workstate'])
ind = ind.loc[~(ind['workstate'].isin([0,1])), :]

len_ind = len(ind)

ind = ind.sort(['id','period'])
months = np.repeat(range(1,13), len_ind/12, axis =0)

ind['period'] = ind['period']*100 + months
print len(ind)
ind.sort(['id', 'period']).to_csv('testind.csv')
# 1- Calculs des durées de cotisation par régimes + durée tot
# TO DO : Le but de cette étape à terme est de réduire la table ind à une table pension
# ne contenant que l'information strictement nécessaire pour le calcul des droits à retraite, tous régimes confondus

trim = ind.groupby(['id', 'workstate']).size()
trim = trim.reset_index()
trim[0] = trim[0].values /3 
pension = trim.drop_duplicates('id')
print "Nombre de personnes pour lesquelles on calcule un pension : ", len(pension)
regimes = {'chom': 2, 'ncadre':3, 'cadre' : 4, 'FPA': 5, 'FPS': 6, 'indep': 7, 'avpf': 8, 'preret':9 }

for reg in regimes:
    code = regimes[reg]
    id_reg = trim.loc[trim['workstate'] == code, 'id']
    trim_reg = trim.loc[trim['workstate'] == code, 0]
    pension['trim_'+ reg] = 0
    pension.loc[pension['id'].isin(id_reg), 'trim_' + reg] = trim_reg
    

# 2 - Appel des fonctions de calcul de droits à la retraite pour individus avec durées non nulles pour régime
# TO DO : faire tourner les fonctions avec conditions sur les trim_reg != 0 