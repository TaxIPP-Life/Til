# -*- coding:utf-8 -*-
'''
Created on 11 septembre 2013

Ce programme :
- 

Input : 
Output :

'''

# 1- Importation des classes/librairies/tables nécessaires à l'importation des données de Destinie -> Recup des infos dans Patrimoine

#from pgm.CONFIG import path_data_patr, path_til, path_data_destinie
from data.DataTil import DataTil
from pgm.CONFIG import path_data_destinie, path_til_liam, path_til, path_of

import pandas as pd
import numpy as np

from pandas import merge, notnull, DataFrame, Series, HDFStore
from numpy.lib.stride_tricks import as_strided
import pdb
import gc
import time

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
        start_time = time.time()
        # TODO: revoir le colnames de BioEmp : le retirer ?
        colnames = list(xrange(longueur_carriere)) 

        
        BioEmp = pd.read_table(path_data_destinie +'BioEmp.txt', sep=';',  
                               header=None, names=colnames)

        BioFam = pd.read_table(path_data_destinie + 'BioFam.txt', sep=';',
                               header=None, names=['id','pere','mere','statut_mar',
                                                   'conj','enf1',"enf2",
                                                   "enf3",'enf4','enf5','enf6']) 

        print "fin de l'importation des données"
        

    #def built_BioEmp(self):

        print "Début mise en forme BioEmp"
        start_time = time.time()
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
        ind = ind.rename(columns={1:'sexe', 2:'naiss', 3:'findet'})
        ind = ind[['id','sexe','naiss','findet']]
        #ind.to_csv('test_ind.csv') 
        
        # selection1 : information sur les statuts d'emploi
        selection1 = [3*x+1 for x in range(taille)]
        stat = BioEmp.iloc[selection1]
        stat = stat.set_index('id').stack().reset_index()
        stat = stat.rename(columns={'level_1':'period', 0:'statut_emp'})
        stat = stat[['id','period','statut_emp']]
        #stat.to_csv('test_stat.csv') 
        
        # selection2 : informations sue les salaires
        selection2 = [3*x+2 for x in range(taille)]
        sal = BioEmp.iloc[selection2]
        sal = sal.set_index('id').stack().reset_index()
        sal = sal.rename(columns={'level_1':'period', 0:'salaire'})
        sal = sal[['salaire']]
        #sal.to_csv('test_sal.csv')
 
        # 2 - Sortie de la table agrégée contenant les infos de BioEmp -> pers
        m1 = merge(stat,sal,left_index = True, right_index = True, sort=False)  #  on ='index', sort = False)
        m1 = m1[['id', 'period', 'statut_emp', 'salaire']]
        pers = merge(m1, ind, on='id', sort=False)
        # print np.max(pers['id']) -> Donne bien 71937 correpondant aux 71938 personnes de l'échantillon initiale
        
        #pers = pers.iloc['id','annee','statut','salaire','sexe','naiss','findet']
        pers['period'] = pers['period'] + pers['naiss']
        #pers.to_csv('test_carriere.csv')
        print "fin de la mise en forme de BioEmp"
        print "temps ecoule pour BioEmp : " + str(time.time() - start_time) + "s"
        
        #def add_link(self):
        print "Début traitement BioFam"
        start_time = time.time()
        # 1 - Variable 'date de mise à jour'
        
        # Index limites pour changement de date
        annee = BioFam[BioFam['id'].str.contains('Fin')] # donne tous les index limites
        annee = annee.reset_index()
        annee['period'] = annee.index + 2009 
        #annee.to_csv('test_fin.csv')
        annee = annee[['index','period']] # colonne 0 = index et colonne 1 = année
        
        # Actualisation des dates de mise à jour        
        BioFam['period'] = 2060 # colonne 11 de la table

        BioFam.loc[:annee.loc[0, 'index'], 'period'] = annee.loc[0, 'period']

        for k in range(1, len(annee)): 
            BioFam.loc[ 1 + annee.loc[k-1,'index']:annee.loc[k,'index'], 'period'] = annee.loc[k, 'period']
            
        # Efface les lignes '*** annee annee ...'
        to_drop = annee['index']
        BioFam = BioFam.drop(BioFam.index[to_drop])
        #BioFam.to_csv('test_annee.csv', sep=',')-> Toutes les années de changement sont OK

        # Identifiants cohérents avec les identifiants pere/mere/enfants
        BioFam['id'] = BioFam['id'].astype(int)
        pers['id'] = pers['id'] + 1     
      

        # 2 - Fusion avec les informations sur déroulés des carrières
        #Informations sur BioFam qu'à partir de 2009 
        #-> on identifie père/mère avec les infos de 2060 + un moins indique leur mort donc reviennent à 0.
        #-> situation maritale : la même qu'en 2009 et après l'âge de fin d'étude, avant = célib et pas de conjoint.
        #-> info sur enfants : abandon.

        pers= pers.astype(int)
        # sélection des informations d'intéret 
        pers = merge(pers,BioFam, on = ['id','period'], how='left') 
        pers = pers[['period','id','sexe','naiss', 'findet','statut_emp','salaire','pere','mere','conj','statut_mar','enf1', 'enf2', 'enf3', 'enf4','enf5','enf6']]
        # Création d'une ligne fictive 2061 pour délimiter les fillna dans la partie suivante
        index_del = range(0, pers['id'].max() + 1)
        Delimit = pd.DataFrame(index=index_del, 
                               columns=['period', 'id', 'sexe', 'naiss', 'findet',
                                         'statut_emp', 'salaire', 'pere', 'mere', 
                                         'conj', 'statut_mar','enf1', 'enf2', 'enf3', 'enf4','enf5','enf6'])
        Delimit['period'] = 2061
        Delimit['id'] = Delimit.index + 1
        Delimit.loc[:,'sexe':] = - 99999999
        Delimit = Delimit.astype(int) # A remplacer par la suite 
        pers = pers.append(Delimit)
        pers = pers.sort(['id', 'period'])
        #pers[ pers['id'] <4 ].to_csv('index.csv')  #-> lignes 2061 bien ordonné grâce au sort mais pb d'index
        
        # Propagation des infos (infos de 2009 copiés pour 2010, 2011 ... jusqu'à ce qu'une nouvelle ligne apparaisse)
        pers = pers.fillna(method='pad')
        pers.loc[pers['period'] < 2009, 'pere':] = np.nan # on rétablit les missings
        pers.loc[:, ['pere','mere']] = pers.loc[:, ['pere','mere']].astype(float)
        #pers.to_csv('tet0.csv')        
        # Traitement particulier des parents : 
        for parent in ['pere','mere'] : 
            pers.loc[pers[parent] == 0, parent] = np.nan 
            # indicatrice du parent vivant : 0 si identifiant négatif
            parent_vivant = (pers[parent] > 0) | pers[parent].isnull()
            pers[parent] = pers[parent].fillna(method='backfill') # rempli avec les infos précédentes
            pers[parent] = abs(pers[parent]*parent_vivant) # identifant du parent seulement lorsqu'il est vivant (sinon 0)
            
        pers = pers.fillna(method = 'backfill') 
 
        # Création des variables d'âge/situation maritale (avant la fin des étude : personne célib pour les états antérieurs à 2009
        pers['age'] = pers['period'] - pers['naiss']
        pers['agem'] = 12*pers['age']
        
        pers.loc[(pers['age'] < pers['findet']) & (pers['period'] < 2009), 'statut_mar'] = 1
        pers.loc[(pers['age'] < pers['findet']) & (pers['period'] < 2009), 'conj'] = np.nan 
        
        # Données inutiles et valeurs manquantes
        pers = pers.loc[pers['period'] != 2061, :]
        to_replace = ['99999999', -99999999, 99999999]
        pers = pers.replace(to_replace, np.nan)
        pers.loc[pers['conj']==0, 'conj'] = np.nan
        print "Fin traitement BioFam"       
        print "temps de BioFam : " + str(time.time() - start_time) + "s"
   
    #def creation_tables(self) : 
        print "Début de la mise au format"
        start_time = time.time()
        # 0 - Non prise en compte des mouvements migratoires -> Peut-être idée à garder car cette modalité regroupe aussi les décédés
        #pers = pers.loc(pers['statut_emp' != 0])
        
        
        # 1 -Table pers au format Liam et Til : traitement des variables
        
        # Situation maritale :  1:célib / 2 : marié / 3 : veuf / 4 : divorcé / 5 : Pacsé : Même code dans les deux, c'est ok!
        pers.loc[pers['conj'] < 0,'conj'] = np.nan
        
        
        # Workstate : pas de retraité car on va simuler le départ à la retraite!
        
        # 0 -> 0 : décès, ou immigré pas encore arrivé en France./ 1-> 3 : privé non cadre /2->4 : privé cadre/31-> 5 : fonctionnaire actif /32-> 6 : fonctionnaire sédentaire
        # 4-> 7 : indépendant / 5->2 : chômeur / 6-> 1: inactif, y compris scolaire / 7->9 : préretraite (uniquement en rétrospectif) / 9->8 : AVPF 
        pers['statut_emp'] = pers['statut_emp'].astype(int)
        pers['statut_emp'].replace([1, 2, 31, 32, 4, 5, 6, 7, 9],
                                   [3, 4, 5, 6, 7, 2, 1, 9, 8])

        # Bon format pour les dates
        pers['period'] = pers['period'].astype(str) + '01' # Pour conserver un format similaire au format date de Til
        pers['period'] = pers['period'].astype(float) # Plus facile pour manip
        
             
        # Noms adéquates pour les variables :
        pers = pers.rename(columns = {'id': 'noi', 'statut_mar': 'civilstate', 'statut_emp': 'workstate', 'salaire': 'Sali'})
        pers_enf = pers[['pere', 'mere','period', 'noi', 'agem', 'age', 'sexe', 
                     'conj', 'civilstate', 'findet', 'workstate', 'Sali', 'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6']]
        pers = pers[['period', 'noi', 'agem', 'age', 'sexe', 'pere', 'mere',
                     'conj', 'civilstate', 'findet', 'workstate', 'Sali']]


        #pers.loc[:, 'noi':] =pers.loc[:, 'noi':].astype(int)
        
        list_val = [1480, 12455, 12454,
                    1481, 33425, 33426,
                    ]
        #strange = pers[pers['noi'].isin(list_val)]
        #strange.to_csv('strange.csv')
        
    # def crea_men(self) :  
          
        # 1- creation des ménages en 2009
        men_init = pers_enf[pers_enf['period'] == 200901]  
        print "Nombre d'individus en 2009 :" + str(len(men_init))
        # Fiabilité des déclarations : 
        decla = men_init[['noi', 'conj']][men_init['civilstate'] == 2]
        verif = merge(decla, decla, left_on ='noi', right_on='conj')
        Pb = verif[ verif['noi_y'] != verif['conj_x'] ]
        print len(Pb), "couples non appariés"
        
        # Pour faciliter la lecture par la suite :
        men = DataFrame(men_init, columns = ['period', 'noi', 'agem', 'age', 'sexe', 'pere', 'mere',
                     'conj', 'civilstate', 'findet', 'workstate', 'Sali', 'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6', 'men', 'quimen'])
        
        # 2- Ménages constitués de couples
        
        # 1ere étape : détermination des têtes de ménage 
        
        # Personne en couple ayant l'identifiant le plus petit  et leur conjoint
        men.loc[(men['conj'] > men['noi']) & men['civilstate'].isin([2,5]), 'quimen']  = 0 
        men.loc[(men['conj'] < men['noi']) & men['civilstate'].isin([2,5]), 'quimen']  = 1         
        print len (men[men['quimen'] == 0]) # 9457
        print len (men[men['quimen'] == 1]) # 9457
        
        
        # Célibataire veuf ou divorcé ayant entre 22 et 75 ans pour les femmes
        men.loc[men['civilstate'].isin([1,3,4])  & (men['age']<75) & (men['age']>21) & (men['sexe']==2),'quimen'] = 0
        print len (men[men['quimen'] == 0]) # 14 478 : +5021
        
        # Célibataire ou veuf ayant entre 25 et 75 ans pour les hommes
        men.loc[men['civilstate'].isin([1,3,4])  & (men['age']<75) & (men['age']>24) & (men['sexe']==1),'quimen'] = 0
        print len (men[men['quimen'] == 0]) # 18 410 : + 3932
        nb_men = 18410
        
        # 2eme étape : attribution du numéro de ménage grâce à la tête de ménage 
        men['men'][men['quimen']==0] = range(0,nb_men)        
        
        # 3eme étape : attribution du numéro de ménages aux autres personnes du ménage (enfants de moins de 21 ans si fille et de moins de 25 ans si garçon et conjoints) 
        men_link = men.loc[men['quimen']==0,['men', 'noi', 'pere', 'mere','conj', 'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6']]
        men_link = men_link.set_index('men').stack()
        #men_enf = pd.DataFrame(men_enf)
        #men_enf = pd.DataFrame(men_enf.split(','), columns = ['1','2','3'])
        #men_enf.columns = ['1','2','3']
        #men_enf['first'] = splits.str[0]
        #men_enf['last'] = splits.str[1]
        #men_enf = pd.DataFrame(men_enf.str.split(','), columns = ['1','2','3'])
        #men_enf.columns = ['men','link', 'id']
        #men_enf['dup'] =  men_enf.duplicated('id')
        #men = merge(men, men_enf, on = 'noi')
        men_link.to_csv('test_enf.csv')
        men_link = pd.read_csv('test_enf.csv', sep=',', header= None, names=['men','link', 'noi'])
        men = merge(men, men_link, how='left', on ='noi', 
                    left_index=False, right_index=False,
                    suffixes=('_x', '_y'), copy=True)
        
        # On attribue le numéro de ménage aux chefs de ménage et à leur conjoint 
        men['men'] = -1
        men.loc[men['link'].isin(['conj','noi']),'men'] = men.loc[men['link'].isin(['conj','noi']), 'men_y']
        
        # Attribution aux enfants et aux parents à charge 
        men.loc[(men['men'] == -1) & (men['quimen'] != 0) & (men['quimen'] != 1) , 'men'] = men.loc[(men['men'] == -1) & (men['quimen'] != 0) & (men['quimen'] != 1), 'men_y']
        men = men.loc[men['men'] != -1, 'noi' :]
        
        # A ce stade deux types de parents à charge : 
        # + ceux ayant plusieurs enfants (attribués au ménage de l'enfant ayant l'identifiant le plus proche avec une proba à def sinon maison de retraite)
        # + ceux ayant aucun enfant : attribués au ménage '-4' équivalent de la maison de retraite
        men.loc[men['men'].isnull() & men['enf1'].isnull() & (men['age']>74), 'men'] = -4
        strange = [34327,15205,34328,1029,8399,23374,1349,20501,37213]
        men_init = men
        men_s = men.loc[men['noi'].isin(strange),:]
        men_s.to_csv('test_men_strange.csv')
        men.to_csv('test_men.csv')
        men_nodup = men_init.groupby(by='noi').first()
        print "Vérification du nombre d'individus en 2009 : " + str(len(men_nodup))
        print "temps de la mise au format : " + str(time.time() - start_time) + "s"
        print "Fin de la mise au format"



data = Destinie()
data.lecture()
