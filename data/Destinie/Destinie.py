# -*- coding:utf-8 -*-
'''
Created on 11 septembre 2013

Ce programme :
- 

Input : 
Output :

'''

# 1- Importation des classes/librairies/tables nécessaires à l'importation des données de Destinie -> Recup des infos dans Patrimoine
import sys
sys.path.append(  "C:\\TaxIPP-Life\\Til")

#from pgm.CONFIG import path_data_patr, path_til, path_data_destinie
from data.DataTil import DataTil
from pgm.CONFIG import path_data_destinie, path_til_liam, path_til, path_of

import pandas as pd
import numpy as np
import pandas.rpy.common as com
import rpy2.rpy_classic as rpy

from pandas import merge, notnull, DataFrame, Series, HDFStore
from numpy.lib.stride_tricks import as_strided
import pdb
import gc



class Destinie(DataTil):  
      
    def __init__(self):
        DataTil.__init__(self)
        self.name = 'Destinie'
        self.survey_date = 200901
        
        #TODO: Faire une fonction qui check où on en est, si les précédent on bien été fait, etc.
        #TODO: Dans la même veine, on devrait définir la suppression des variables en fonction des étapes à venir.
        self.done = []
        self.methods_order = ['lecture']
       
    def lecture(self):
        longueur_carriere = 106
        print "début de l'importation des données"
        # TODO: revoir le colnames de BioEmp : le retirer ?
        colnames = list(xrange(longueur_carriere)) 
        BioEmp = pd.read_table(path_data_destinie +'BioEmp.txt', 
                               names=colnames, header=None, sep=';')
        BioFam = pd.read_table(path_data_destinie + 'BioFam.txt', sep=';',
                          header=None, names=['id','pere','mere','statut_mar',
                                           'conj','enf1',"enf2",
                                           "enf3",'enf4','enf5','enf6']) 
        print "fin de l'importation des données"
        
    #def built_BioEmp(self):
        print "Début mise en forme BioEmp"
        
        # 1 - Division de BioEmpen trois tables
        
        taille = len(BioEmp)/3
        index = BioEmp.index  # BioEmp['index'] crée une variable, alors qu'on en a pas besoin
        BioEmp['id'] = index/3
        BioEmp['id'] = BioEmp['id'].astype(int)
        #BioEmp['test'] = BioEmp[1] -> on appelle les colonnes avec un format numérique du coup
        
        # selection0 : informations atemporelles  sur les individus (identifiant, sexe, date de naissance et âge de fin d'étude)
        selection0 = [3*x for x in range(taille)]
        ind = BioEmp.iloc[selection0]
        ind = ind.reset_index()
        ind = ind.rename(columns={ 1:'sexe', 2:'naiss', 3:'findet'})
        ind = ind[['id','sexe','naiss','findet']]
        ind.to_csv('test_ind.csv') 
        
        # selection1 : information sur les statuts d'emploi
        selection1 = [3*x+1 for x in range(taille)]
        stat = BioEmp.iloc[selection1]
        stat = stat.set_index('id').stack().reset_index()
        stat = stat.rename(columns={ 'level_1': 'annee', 0:'statut_emp'})
        stat = stat[['id','annee','statut_emp']]
        #stat.to_csv('test_stat.csv') 
        
        # selection2 : informations sue les salaires
        selection2 = [3*x+2 for x in range(taille)]
        sal = BioEmp.iloc[selection2]
        sal = sal.set_index('id').stack().reset_index()
        sal = sal.rename(columns={'level_1': 'annee', 0:'salaire'})
        sal = sal[['salaire']]
        #sal.to_csv('test_sal.csv')
 
        # 2 - Sortie de la table agrégée contenant les infos de BioEmp -> pers
        m1 = merge(stat,sal,left_index = True, right_index = True, sort=False)  #  on ='index', sort = False)
        m1 = m1[['id','annee','statut_emp', 'salaire']]
        pers = merge(m1, ind, on='id', sort=False)
        # print np.max(pers['id']) -> Donne bien 71937 correpondant aux 71938 personnes de l'échantillon initiale
        
        #pers = pers.iloc['id','annee','statut','salaire','sexe','naiss','findet']
        pers['annee'] = pers['annee'] + pers['naiss']
        #pers.to_csv('test_carriere.csv')
        print "fin de la mise en forme de BioEmp"
 
    #def add_link(self):
        print "Début traitement BioFam"

        # 1 - Variable 'date de mise à jour'
        
        # Index limites pour changement de date
        fin = BioFam[BioFam['id'].str.contains('Fin')] # donne tous les index limites
        fin = fin.reset_index()
        fin['annee'] = fin.index  + 2009 
        fin.to_csv('test_fin.csv')
        fin = fin[['index','annee']] # colonne 0 = index et colonne 1 = année
        
        # Actualisation des dates de mise à jour        
        BioFam['annee'] = 2060 # colonne 11 de la table
        BioFam.iloc[: fin.iloc[0,0],11] = fin.iloc[0,1]
        
        for k in range(1,len(fin)): 
            BioFam.iloc[fin.iloc[k-1,0] + 1 : fin.iloc[k,0],11] = fin.iloc[k,1]
            
        # Efface les lignes '*** Fin annee ...'
        to_drop = fin['index']
        BioFam = BioFam.drop(BioFam.index[to_drop])
        #BioFam.to_csv('test_fin.csv', sep=',')-> Toutes les années de changement sont OK
        
        # Identifiants cohérents avec les identifiants pere/mere/enfants
        BioFam['id'] = BioFam['id'].astype(int)
        pers['id'] = pers['id'] + 1     
           
        # 2 - Fusion avec les informations sur déroulés des carrières
        pers = pd.merge(pers,BioFam, on = ['id','annee'], how='left') #, how='left', sort=False)
        pers = pers.fillna(method='pad') # problème des transitions entre individus
        pers['pere' : ][pers['annee']<2009] = Nan

  
        # Bon format pour les dates
        pers['annee'] = pers['annee'].astype(int)
        pers['annee'] = pers['annee'].astype(str) + '01' # Pour conserver un format similaire au format date de Til
        pers['annee'] = pers['annee'].astype(float) # Plus facile pour manip
        
        pers.to_csv('test_finish.csv')
        # list_val = [1,33312,33313, 2,3369,30783]
        # strange = pers[pers['id'].isin(list_val)]
        # strange.to_csv('strange.csv')
    #def creation_menage(self) : 
        '''
        nb_ind = 71938
        list_dup = []
        for k in range(1,nb_ind +1) :
            wrk = list (BioFam['annee'][BioFam['id']==k])
            for i in xrange(len(wrk)-1) : 
                wrk[i] = wrk[i+1] - wrk[i]
            wrk[len(wrk)-1] = 2061 - wrk[len(wrk)-1]
    
            list_dup = list_dup + wrk
        print list_dup
        np.repeat(BioFam, list_dup, axis=0)
       
        #creation des ménages 
               # faire une méthode plutôt
        men = pd.Series(range(len(ind)))  # ne marche pas je pense, je
               # séléctionner les gens qui ont un conjoint, puis un père, puis une mère avec un noi près du leur (moins de 10 disons). A chaque fois leur mettre l'ident de la personne concernée. Ca peut foirer s'il y a des cas vicieux (on vit avec sa mère mais aussi avec son conjoint) et il faudra faire une autre boucle mais ça m'interesse de savoir si ce cas existe. 
        ''' 
        
        print "Fin mise en forme BioFam"

import time
start = time.clock()
data = Destinie()
data.lecture()
#data.built_BioEmp()
