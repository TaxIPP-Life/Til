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
import os.path

path_add =  os.path.split(sys.path[0])
path_add = os.path.split(path_add[0])[0]
sys.path.append(path_add)

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
        stat = stat.rename(columns={ 'level_1': 'period', 0:'statut_emp'})
        stat = stat[['id','period','statut_emp']]
        #stat.to_csv('test_stat.csv') 
        
        # selection2 : informations sue les salaires
        selection2 = [3*x+2 for x in range(taille)]
        sal = BioEmp.iloc[selection2]
        sal = sal.set_index('id').stack().reset_index()
        sal = sal.rename(columns={'level_1': 'period', 0:'salaire'})
        sal = sal[['salaire']]
        #sal.to_csv('test_sal.csv')
 
        # 2 - Sortie de la table agrégée contenant les infos de BioEmp -> pers
        m1 = merge(stat,sal,left_index = True, right_index = True, sort=False)  #  on ='index', sort = False)
        m1 = m1[['id','period','statut_emp', 'salaire']]
        pers = merge(m1, ind, on='id', sort=False)
        # print np.max(pers['id']) -> Donne bien 71937 correpondant aux 71938 personnes de l'échantillon initiale
        
        #pers = pers.iloc['id','annee','statut','salaire','sexe','naiss','findet']
        pers['period'] = pers['period'] + pers['naiss']
        #pers.to_csv('test_carriere.csv')
        print "fin de la mise en forme de BioEmp"
 
    #def add_link(self):
        print "Début traitement BioFam"

        # 1 - Variable 'date de mise à jour'
        
        # Index limites pour changement de date
        fin = BioFam[BioFam['id'].str.contains('Fin')] # donne tous les index limites
        fin = fin.reset_index()
        fin['period'] = fin.index  + 2009 
        #fin.to_csv('test_fin.csv')
        fin = fin[['index','period']] # colonne 0 = index et colonne 1 = année
        
        # Actualisation des dates de mise à jour        
        BioFam['period'] = 2060 # colonne 11 de la table
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
 

        '''        
        Indiquer si aucune info sur les parents (nécessaire pour la suite) dans BioFam
        BioFam.sort(['id', 'period'], ascending=[True, False])
        BioFam.set_index(['id', 'period'], ascending=[True, False])
        BioFam.loc[0,'pere':] = np.nan
        '''
        
        
        # 2 - Fusion avec les informations sur déroulés des carrières
        '''
        Informations sur BioFam qu'à partir de 2009 
        -> on identifie père/mère avec les infos de 2060 + un moins indique leur mort donc reviennent à 0.
        -> situation maritale : la même qu'en 2009 et après l'âge de fin d'étude, avant = célib et pas de conjoint.
        -> info sur enfants : abandon.
        '''
        
        # sélection des informations d'intéret et création d'un délimiteur (année fictive)
        pers = pd.merge(pers,BioFam, on = ['id','period'], how='left') #, how='left', sort=False) 
        pers = pers[['period','id','sexe','naiss','pere','mere','conj','statut_mar','findet','statut_emp','salaire']]
               
                # Création d'une ligne fictive 2061 pour délimiter les fillna dans la partie suivante
        index = range(0,pers['id'].max()+1)
        Delimit = pd.DataFrame(index=index, columns=['period','id','sexe','naiss', 'pere','mere','conj','statut_mar','findet','statut_emp','salaire'])
        Delimit['period'] = 2061
        Delimit['id'] = Delimit.index
        Delimit.loc[:,'sexe' : ] = -1
        pers = pers.append(Delimit,ignore_index=True)
        pers.set_index('id')
        # pers[pers['id']==1].to_csv('index.csv') -> ligne 2061 apparait bien mais pb d'index
 
        
        '''
        # problème des transitions entre individus : on commence par faire un "barrage" de zéro puis on propage les infos (infos de 2009 copiés pour 2010, 2011 ... jusqu'à ce qu'une nouvelle ligne apparaisse)
        pers['period'] = pers['period'].astype(int)
        pers.loc[pers.loc[:,'period']<2009,'pere':] = 0
        pers = pers.fillna(method='pad')
        pers.loc[pers.loc[:,'period']<2009,'pere':] = np.nan # on rétablit les missings
        '''
        
        # Traitement particulier des parents : 
        parents = ['pere','mere']
        for parent in parents : 
            pers[parent] = pers[parent].astype(float)
            pers[parent][pers[parent] == 0] = np.nan # On remplace les 0 par des missings
            parent_viv = ((pers[parent] <0) == False ) # indicatrice du parent vivant : pas d'info sur sa mort ou identifiant positif : nécessaire étape suivante
            pers[parent] = pers[parent].fillna(method = 'backfill') # rempli avec les infos précédentes
            pers[parent] = abs(pers[parent]*parent_viv) # identifant du parent seulement lorsqu'il est vivant (sinon 0)

        pers = pers.fillna(method = 'backfill') 
            
        # Création des variables d'âge/situation maritale (avant la fin des étude : personne célib pour les états antérieurs à 2009
        pers['age'] = pers['period'] - pers['naiss']
        pers['agem'] = pers['age'] * 12
        
        
        pers.loc[(pers['age']<pers['findet'] ) & (pers['period']<2009) ,'statut_mar'] = 1
        pers.loc[(pers['age']<pers['findet'] ) & (pers['period']<2009) ,'conj'] = np.nan 
        
        print "Fin traitement BioFam"       

    #def creation_tables(self) : 
    
        # 0 - Non prise en compte des mouvements migratoires
        #pers = pers.loc(pers['statut_emp' != 0])
        
        
        # 1 -Table pers au format Liam et Til : traitement des variables
        
        # Situation maritale :  1:célib / 2 : marié / 3 : veuf / 4 : divorcé / 5 : Pacsé : Même code dans les deux, c'est ok!
        
        # Workstate : pas de retraité car on va simuler le départ à la retraite!
        '''
         0 -> ? : décès, ou immigré pas encore arrivé en France./ 1-> 3 : privé non cadre /2->4 : privé cadre/31-> 5 : fonctionnaire actif /32-> 6 : fonctionnaire sédentaire
        4-> 7 : indépendant / 5->2 : chômeur / 6-> 1: inactif, y compris scolaire / 7->9 : préretraite (uniquement en rétrospectif) / 9->8 : AVPF 
        '''
        
        pers['statut_emp'] = pers['statut_emp'].astype(int)
        pers['statut_emp'].replace([0,1,2,31,32,4,5,6,7,9],[np.nan,3,4,5,6,7,2,1,9,8])

        
        # Bon format pour les dates
        pers['period'] = pers['period'].astype(str) + '01' # Pour conserver un format similaire au format date de Til
        pers['period'] = pers['period'].astype(float) # Plus facile pour manip
        
        # Noms adéquates pour les variables :
        pers = pers.rename(columns = {'statut_mar' : 'civilstate', 'statut_emp' : 'workstate', 'salaire' : 'Sali'})
        pers = pers[['period','id','agem', 'age','sexe','pere','mere','conj','civilstate','findet','workstate','Sali']]

        pers[pers['workstate'] == 0 ].to_csv('test_migrant.csv')
        pers.to_csv('test_finish2.csv')
        # list_val = [1,33312,33313, 2,3369,30783]
        # strange = pers[pers['id'].isin(list_val)]
        # strange.to_csv('strange.csv')
        
        #creation des ménages 
               # faire une méthode plutôt
        men = pd.Series(range(len(ind)))  # ne marche pas je pense, je
               # séléctionner les gens qui ont un conjoint, puis un père, puis une mère avec un noi près du leur (moins de 10 disons). A chaque fois leur mettre l'ident de la personne concernée. Ca peut foirer s'il y a des cas vicieux (on vit avec sa mère mais aussi avec son conjoint) et il faudra faire une autre boucle mais ça m'interesse de savoir si ce cas existe. 
     
        print "Fin de la mise au format"


import time
start = time.clock()
data = Destinie()
data.lecture()
#data.built_BioEmp()