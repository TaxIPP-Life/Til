# -*- coding:utf-8 -*-
'''
Created on 11 septembre 2013

Ce programme :
- 

Input : 
Output :

'''

# 1- Importation des classes/librairies/tables nécessaires à l'importation des données de l'enquête Patrimoine

# Recup de ce dont on a besoin dans Patrimoine

 
from path_config import path_data_patr, path_til, path_data_des
from DataTil import DataTil_d

import pandas as pd
import numpy as np
import pandas.rpy.common as com
import rpy2.rpy_classic as rpy

from pandas import merge, notnull, DataFrame, Series, HDFStore
from numpy.lib.stride_tricks import as_strided
import pdb
import gc

#data = path_data_des+'\\BiosDestinie.RData'
#bio = rpy.r.load(data)
#bio = com.convert_robj(data) 
#bio  = np.asarray(bio)
#bio = pd.DataFrame(bio)


class Destinie(DataTil_d):  
      
    def __init__(self):
        DataTil_d.__init__(self)
        self.name = 'Destinie'
        self.survey_date = 200901
        
        #TODO: Faire une fonction qui check où on en est, si les précédent on bien été fait, etc.
        #TODO: Dans la même veine, on devrait définir la suppression des variables en fonction des étapes à venir.
        self.done = []
        self.methods_order = ['lecture']
       
    def lecture(self):
        print "début de l'importation des données"
        col= list(xrange(105)) + ['id']
        BioEmp = pd.read_table(path_data_des +'BioEmp2.txt', names=col,sep=',')
        BioFam = pd.read_csv(path_data_des + 'BioFam.csv', sep=';',
                          header=0, names=['noi','pere','mere','statut',
                                           'conj','enf1',"enf2",
                                           "enf3",'enf4','enf5','enf6']) 
        print "fin de l'importation des données"
        
    #def built_BioEmp(self):

        # 1 - Mise en forme de BioEmp
        BioEmp = pd.DataFrame(BioEmp,index=range(0,len(BioEmp),1))
        BioEmp['index'] = BioEmp.index
        BioEmp['id'] = BioEmp['index']/3
        BioEmp['id'] = BioEmp['id'].astype(int)
        #BioEmp['test'] = BioEmp[1] -> on appelle les colonnes avec un format numérique du coup
        
        BioEmp['nb_ligne'] = BioEmp['index'] - 3*BioEmp['id']

        # 2 - Construction de la Table ind : table avec toutes les info de bases sur les individus (appariement entre enfants et parents avec BioFam sur cette table) et car avec les infos sur carrières
        ind = BioEmp[BioEmp['nb_ligne']==0]
        ind = ind.rename(columns={1 :'sexe', 2 : 'naiss', 3 : 'findet'})
        ind = ind[['id','sexe','naiss','findet']]

        '''
        Le programme met pas mal de temps à tourner : le premier merge pourrait ertainement être évité en travaillant conjointement sur statut/salaire
        car = BioEmp[BioEmp['nb_ligne']>0]
        car['index'] = car.index
        car.to_csv('test1.csv')
        car = car.set_index('id').stack().reset_index()
        car1 = car['index','']
        car = pd.melt(car, id_vars=['index'])
        car = car.sort('id')
        car = car.set_index('id') 
        car = car.rename(columns={'variable': 'code_année'})
        car.to_csv('test.csv')
        '''
        
        # 3 - Construction de la table stat (table intermédiaire) avec les status d'emploi
        stat = BioEmp[BioEmp['nb_ligne']==1][col]
        stat = stat.set_index('id').stack().reset_index()
        stat['index'] =stat.index
        stat = stat.rename(columns={ 'level_1': 'annee', 0:'statut'})
        stat = stat[['id','annee','statut','index']]
        #stat.to_csv('test_stat.csv') 
        
        # 4 - Construction de la table sal (table intermédiare) avec les salaires en emploi
        sal = BioEmp[BioEmp['nb_ligne']==2][col]
        sal = sal.set_index('id').stack().reset_index()
        sal['index'] =sal.index
        sal = sal.rename(columns={'level_1': 'annee', 0:'salaire'})
        sal = sal[['salaire','index']]
        #sal.to_csv('test_sal.csv')

        # 5 - Sortie de la table agrégée contenant les infos de BioEmp -> pers
        m1 = merge(stat, sal, on = 'index')
        m1 = m1[['id','annee','statut', 'salaire']]
        pers = merge(m1, ind, on='id')
        # pers.to_csv('test_merge.csv')
        # print np.max(pers['id']) -> Donne bien 71937 correpondant aux 71938 personnes de l'échantillon initiale
        pers = pers[['id','annee','statut','salaire','sexe','naiss','findet']]
        pers.to_csv('test_finish.csv')

        
import time
start = time.clock()
data = Destinie()
data.lecture()
#data.built_BioEmp()
