# -*- coding:utf-8 -*-
'''
Created on 11 septembre 2013

Ce programme :
- 

Input : 
Output :

'''

# 1- Importation des classes/librairies/tables nécessaires à l'importation des données de Destinie -> Recup des infos dans Patrimoine

from data.DataTil import DataTil
from data.utils import minimal_dtype
from pgm.CONFIG import path_data_destinie

import pandas as pd
import numpy as np
from pandas import merge, notnull, DataFrame, Series, HDFStore

import pdb
import gc
import time

class Destinie(DataTil):  
      
    def __init__(self):
        DataTil.__init__(self)
        self.name = 'Destinie'
        self.survey_year = 2009
        self.last_year = 2060
        self.survey_date = 100*self.survey_year + 1
        
        # TODO: Faire une fonction qui check où on en est, si les précédent on bien été fait, etc.
        # TODO: Dans la même veine, on devrait définir la suppression des variables en fonction des étapes à venir.
        self.done = []
        self.methods_order = ['lecture']
       
    def lecture(self):
        longueur_carriere = self.last_year - self.survey_year #106
        
        print "début de l'importation des données"
        start_time = time.time()
        # TODO: revoir le colnames de BioEmp : le retirer ?
        colnames = list(xrange(longueur_carriere)) 

        BioEmp = pd.read_table(path_data_destinie + 'BioEmp.txt', sep=';',
                               header=None, names=colnames)
        BioFam = pd.read_table(path_data_destinie + 'BioFam.txt', sep=';',
                               header=None, names=['noi', 'pere', 'mere', 'civilstate',
                                                   'conj', 'enf1', 'enf2',
                                                   'enf3', 'enf4', 'enf5', 'enf6']) 
            
        def _correction_fam():
            # Ambiguité sur Pacs/marié (ex : 8669 et 8668 se déclarent en couple mais l'un en marié l'autre en pacsé)
            BioFam.loc[BioFam['civilstate'] == 5, 'civilstate'] = 2
            BioFam.loc[((BioFam['civilstate'] == 2) | BioFam['civilstate'].isnull()) & (BioFam['conj'].isnull()| (BioFam['conj'] == 0)), 'civilstate'] = 1
            # Indice more Pythonic

        def _BioEmp_in_3():
            ''' Division de BioEmpen trois tables '''
            taille = len(BioEmp)/3
            BioEmp['noi'] = BioEmp.index/3
            
            # selection0 : informations atemporelles  sur les individus (identifiant, sexe, date de naissance et âge de fin d'étude)
            selection0 = [3*x for x in range(taille)]
            ind = BioEmp.iloc[selection0]
            ind = ind.reset_index()
            ind = ind.rename(columns={1:'sexe', 2:'naiss', 3:'findet', 4:'tx_prime_fct'})
            ind = ind[['sexe', 'naiss', 'findet', 'tx_prime_fct']]
            ind = minimal_dtype(ind)
            
            # selection1 : information sur les statuts d'emploi
            selection1 = [3*x + 1 for x in range(taille)]
            statut = BioEmp.iloc[selection1]
            statut = statut.set_index('noi').stack().reset_index()
            statut = statut.rename(columns={'level_1':'period', 0:'workstate'})
            statut = statut[['noi', 'period', 'workstate']]
            
            # selection2 : informations sue les salis
            selection2 = [3*x + 2 for x in range(taille)]
            sal = BioEmp.iloc[selection2]
            sal = sal.set_index('noi').stack().reset_index()
            sal = sal.rename(columns={'level_1':'period', 0:'sali'})
            sal = sal[['sali']]       
            return ind, statut, sal
              
        _correction_fam()
        print "fin de l'importation des données"
        start_time = time.time()
        ind, statut, sal = _BioEmp_in_3()
        print "temps ecoule pour BioEmp : " + str(time.time() - start_time) + "s"
        
        # def add_link(self):
        print "Début traitement BioFam"
        start_time = time.time()
        
        # 1 - Variable 'date de mise à jour'
    
        # Index limites pour changement de date
        delimiters = BioFam['noi'].str.contains('Fin')
        annee = BioFam[delimiters].index.tolist()  # donne tous les index limites
        annee = [-1] + annee # to simplify loops later
        # create a series period
        period = []
        for k in range(len(annee)-1):
            period = period + [2009+k]*(annee[k+1]-1-annee[k])

        BioFam = BioFam[~delimiters]
        BioFam['period'] = period
        list_enf = ['enf1','enf2','enf3','enf4','enf5','enf6']
        BioFam[list_enf + ['pere','mere', 'conj']] -= 1
        print "Fin traitement BioFam"
        #BioFam = BioFam.fillna(-1).astype(int)
        
        print "Début de l'initialisation des données pour 2009"
        
    # 1-  Sélection des individus présents en 2009 et vérifications des liens de parentés
        year_ini = self.survey_year  = 2009 # pourquoi besoin de rajouter cette égalité?
        ind = merge(ind.loc[ind['naiss'] <= year_ini], BioFam[BioFam['period']==year_ini], 
                    left_index=True, right_index=True, how='left')
        ind = ind.loc[:, :'enf6']
        ind.loc[((ind['civilstate'] == 2) | ind['civilstate'].isnull()) & (ind['conj'].isnull()| (ind['conj'] == -1)), 'civilstate'] = 1
        ind['noi'] = ind.index
        ind.loc[ind['enf1']<0,'enf1'] = np.nan        
        print "Nombre d'individus dans la base initiale de 2009 : " + str(len(ind))

        pere_ini = ind['pere']
        ind['pere'] = -1 
        mere_ini = ind['mere']
        ind['mere'] = -1        
        for var in list_enf:
            pere = ind.loc[ (~ind['sexe']) & (ind[var].notnull()), var]
            mere = ind.loc[ (ind['sexe']) & (ind[var].notnull()), var]
            ind['pere'][pere.values] = pere.index.values
            ind['mere'][mere.values] = mere.index.values
        ind.loc[(ind['pere']== -1), 'pere'] = pere_ini
        ind.loc[(ind['mere']== -1), 'mere'] = mere_ini
        ind.loc[ind['conj'] < 0, 'conj'] = np.nan
        ind.loc[ind['pere'] < 0,'pere'] = np.nan
        ind.loc[ind['mere'] < 0,'mere'] = np.nan
        ind = ind[['sexe', 'naiss', 'findet', 'tx_prime_fct', 'noi', 'pere', 'mere', 'civilstate', 'conj']]
        # valeurs négatives à np.nan pour la fonction minimal_type 
        # ind = minimal_dtype(ind)
    
    # 2- Constitution des ménages de 2009
        ind['quimen'] = -1
        ind['men'] = -1
        ind['age'] = year_ini - ind['naiss']
          
        
        # ind['pere'] == pere_ini : vrai que pour les parents de la même famille -> utilisé pour les ménages !! 
        
        # 1ere étape : détermination des têtes de ménage 
        # Personne en couple ayant l'identifiant le plus petit  et leur conjoint
        ind.loc[( ind['conj'] > ind['noi'] ) & ( ind['civilstate'] == 2 ), 'quimen'] = 0 
        ind.loc[(ind['conj'] < ind['noi']) & ( ind['civilstate'] == 2 ), 'quimen'] = 1         
        print len (ind[ind['quimen'] == 0])  # 9457
        print len (ind[ind['quimen'] == 1])  # 9457
        ind.to_csv('ind.csv')
        
        # Célibataires veuves ou divorcées ayant entre 22 et 75 ans pour les femmes
        ind.loc[ind['civilstate'].isin([1, 3, 4]) & (ind['age'] < 76) & (ind['age'] > 21) & ind['sexe'], 'quimen'] = 0
        print len (ind[ind['quimen'] == 0])  # 14 807 : +5350
        
        # Célibataires ou veufs ayant entre 25 et 75 ans pour les hommes
        ind.loc[ind['civilstate'].isin([1, 3, 4]) & (ind['age'] < 76) & (ind['age'] > 24) & ~ind['sexe'], 'quimen'] = 0
        print len (ind[ind['quimen'] == 0])  # 18 410 : + 4231
 
        # Cas particuliers
        # a - Fille de plus de 75 ans ayant identifiants très proches de la mère
        value = [1536, 1538, 1540, 1542]
        ind.loc[ind['noi'].isin(value), 'quimen'] = 0
        # b - Majeurs n'ayant aucun parent spécifié
        ind.loc[ind['pere'].isnull() & ind['mere'].isnull() & (25>ind['age']) & (ind['age']>17), 'quimen'] = 0
        
        # 2eme étape : attribution du numéro de ménage grâce à la tête de ménage 
        nb_men = len (ind[ind['quimen'] == 0]) # 19 471
        print "Le nombre de ménages constitués est :" + str(nb_men)
        ind['men'][ind['quimen'] == 0] = range(0, nb_men)  

        # 3eme étape : attribution du numéro de ménage aux conjoints
        conj = ind.loc[ind['conj'].notnull() & (ind['quimen'] == 0), ['conj','men']].astype(int)
        ind['men'][conj['conj'].values] = conj['men'].values

        # 4eme étape : attribution du numéro de ménages aux enfants n'ayant pas déjà constitués un ménage
        # -> enfants de moins de 21 ans si fille et de moins de 25 ans si garçon 
        # -> Les enfants sont prioritairement attribués au ménage de leur mère
        mere = ind.loc[ind['mere'].notnull() & (ind['men'] == -1), 'mere']
        ind['men'][mere.index.values] = ind['men'][mere.values]
                
        pere = ind.loc[ind['pere'].notnull() & (ind['men'] == -1), 'pere']
        ind['men'][pere.index.values] = ind['men'][pere.values]

        # 5eme étape : attribution du numéro de ménages pour parents à charge
        # -> On considère que les parents à charge sont ceux déclarés par leur enfant lors de l'enquête
        # Parents vivant dans le même ménage qu'un de leurs enfants :
        without_men = ind.loc[ind['men'] == -1,'noi']
        care_pere = ind.loc[(ind['pere'] == pere_ini) & ind['pere'].isin(without_men), 'pere']
        is_duplicated = care_pere.duplicated('pere')
        care_pere = care_pere[~care_pere.duplicated('pere')]
        ind['men'][care_pere.values] = ind['men'][care_pere.index.values]
        
        care_mere = ind.loc[(ind['mere'] == mere_ini) & ind['mere'].isin(without_men), 'mere']
        is_duplicated = care_mere.duplicated('mere')
        care_mere = care_mere[~care_mere.duplicated('mere')]
        ind['men'][care_mere.values] = ind['men'][care_mere.index.values]
        
        # 6eme étape : création de deux ménages fictifs résiduels :
        # Enfants sans parents :  dans un foyer fictif équivalent à la DASS = -4
        ind.loc[ ind['pere'].isnull() &  ind['mere'].isnull() & (ind['age']<18), 'men' ] = -4
        
        # Personnes âgées non prises en charge par l'un de leur enfant : maison de retraite = -5
        ind.loc[ (ind['men'] == -1) & (ind['age']> 74), 'men' ] = -5
        ind.to_csv('ind.csv')
        print len(ind[ind['men']==-1])
        pdb.set_trace() 
        
    # 3- Constitutions des foyers fiscaux de 2009
        
        ## Note importante, on suppose que l'on repère parfaitement les décès avec BioEmp (on oublie du coup les valeurs négatives)
        BioFam[['noi', 'civilstate', 'period']] = BioFam[['noi', 'civilstate', 'period']].astype(int)
        pdb.set_trace()
        '''
        # 2 - Sortie de la table agrégée contenant les infos de BioEmp -> pers
        m1 = merge(statut, sal, left_index=True, right_index=True, sort=False)  #  on ='index', sort = False)
        m1 = m1[['noi', 'period', 'workstate', 'sali']]
        pers = merge(m1, ind, on='noi', sort=False)
        pers.to_csv('pers.csv')
        pdb.set_trace()
        # pers = pers.iloc['noi','annee','statut','sali','sexe','naiss','findet']
        pers['period'] = pers['period'] + pers['naiss']

        '''
      
        
        # 2 - Fusion avec les informations sur déroulés des carrières
        # Informations sur BioFam qu'à partir de 2009 
        # -> on identifie père/mère avec les infos de 2060 + un moins indique leur mort donc reviennent à 0.
        # -> situation maritale : la même qu'en 2009 et après l'âge de fin d'étude, avant = célib et pas de conjoint.
        # -> info sur enfants : abandon.
        pers = pers.astype(int)
        
        # sélection des informations d'intéret 
        pers = merge(pers, BioFam, on=['noi', 'period'], how='left') 
        pers = pers[['period', 'noi', 'sexe', 'naiss', 'findet', 'workstate', 'sali', 
                     'pere', 'mere', 'conj', 'civilstate', 'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6', 'enf7', 'enf8', 'enf9']]
        # Création d'une ligne fictive 2061 pour délimiter les fillna dans la partie suivante
        index_del = range(0, pers['noi'].max() + 1)
        Delimit = pd.DataFrame(index=index_del,
                               columns=['period', 'noi', 'sexe', 'naiss', 'findet',
                                         'workstate', 'sali', 'pere', 'mere',
                                         'conj', 'civilstate', 'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6',
                                         'enf7', 'enf8', 'enf9'])
        Delimit['period'] = self.last_year + 1
        Delimit['noi'] = Delimit.index + 1
        Delimit.loc[:, 'sexe':] = -99999999
        Delimit = Delimit.astype(int)  # A remplacer par la suite 
        pers = pers.append(Delimit)
        pers = pers.sort(['noi', 'period'])
        # pers[ pers['noi'] <4 ].to_csv('index.csv')  #-> lignes 2061 bien ordonné grâce au sort mais pb d'index
        
        # Propagation des infos (infos de 2009 copiés pour 2010, 2011 ... jusqu'à ce qu'une nouvelle ligne apparaisse)
        pers = pers.fillna(method='pad')
        pers.loc[pers['period'] < self.survey_year, 'pere':] = np.nan  # on rétablit les missings
        pers[['pere', 'mere']] = pers[['pere', 'mere']].astype(float)   
           
        # Traitement particulier des parents : 
        for parent in ['pere', 'mere'] : 
            pers.loc[pers[parent] == 0, parent] = np.nan 
            # indicatrice du parent vivant : 0 si identifiant négatif
            parent_vivant = (pers[parent] > 0) | pers[parent].isnull()
            pers[parent] = pers[parent].fillna(method='backfill')  # rempli avec les infos précédentes
            pers[parent] = abs(pers[parent] * parent_vivant)  # identifant du parent seulement lorsqu'il est vivant (sinon 0)
            
        pers = pers.fillna(method='backfill')
        to_replace = [ -99999999, 99999999, '99999999', '-99999999']
        pers = pers.replace(to_replace, np.nan) 
        strange = [377, 35877, 522, 12224, 34327, 15205, 34328, 1029, 8399, 23374, 1349, 20501, 37213, 35877, 3379, 5986, 19328, 31635]
#        pers.loc[pers['noi'].isin(strange), :].to_csv('pers_test.csv')

        # Création des variables d'âge/situation maritale (avant la fin des étude : personne célib pour les états antérieurs à 2009
        pers['age'] = pers['period'] - pers['naiss']
        pers['agem'] = 12 * pers['age']
        
        pers.loc[(pers['age'] < pers['findet']) & (pers['period'] < self.survey_year), 'civilstate'] = 1
        pers.loc[(pers['age'] < pers['findet']) & (pers['period'] < self.survey_year), 'conj'] = np.nan 
        
        # Données inutiles et valeurs manquantes
        pers = pers.loc[pers['period'] != self.last_year + 1, :]
        pers[['conj', 'pere', 'mere']] = pers[['conj', 'pere', 'mere']].replace(0, np.nan)
        pers.loc[((pers['civilstate'] == 2) | pers['civilstate'].isnull()) & (pers['conj'].isnull()), 'civilstate'] = 1
        print "Fin traitement BioFam"       
        print "temps de BioFam : " + str(time.time() - start_time) + "s"
   
    # def creation_tables(self) : 
        print "Début de la mise au format"
        start_time = time.time()
        # 0 - Non prise en compte des mouvements migratoires -> Peut-être idée à garder car cette modalité regroupe aussi les décédés
        # pers = pers.loc(pers['workstate' != 0])
        
        # 1 -Table pers au format Liam et Til : traitement des variables
        
        # Situation maritale :  1:célib / 2 : marié / 3 : veuf / 4 : divorcé / 5 : Pacsé : Même code dans les deux, c'est ok!
        pers.loc[pers['conj'] < 0, 'conj'] = np.nan
        
        # Workstate : pas de retraité car on va simuler le départ à la retraite!
        # 0 -> 0 : décès, ou immigré pas encore arrivé en France./ 1-> 3 : privé non cadre /2->4 : privé cadre/31-> 5 : fonctionnaire actif /32-> 6 : fonctionnaire sédentaire
        # 4-> 7 : indépendant / 5->2 : chômeur / 6-> 1: inactif, y compris scolaire / 7->9 : préretraite (uniquement en rétrospectif) / 9->8 : AVPF 
        pers['workstate'] = pers['workstate'].astype(int)
        pers['workstate'].replace([1, 2, 31, 32, 4, 5, 6, 7, 9],
                                   [3, 4, 5, 6, 7, 2, 1, 9, 8])

        # Bon format pour les dates
        pers['period'] = 100*pers['period'] + 1
             
        # Noms adéquates pour les variables :
        pers = pers.rename(columns={ 'workstate': 'workstate', 'sali': 'Sali'})
        pers = pers[['period', 'noi', 'agem', 'age', 'sexe', 'pere', 'mere',
                     'conj', 'civilstate', 'findet', 'workstate', 'Sali']]


        # pers.loc[:, 'noi':] =pers.loc[:, 'noi':].astype(int)
        
        list_val = [1480, 12455, 12454,
                    1481, 33425, 33426,
                    ]
        # strange = pers[pers['noi'].isin(list_val)]
        # strange.to_csv('strange.csv')
        
    # def crea_men(self) :  
          
        # 1- creation des ménages en 2009
        men_init = pers[pers['period'] == self.survey_date]  
        print "Nombre d'individus en 2009 :" + str(len(men_init))
        # Fiabilité des déclarations : 
        decla = men_init[['noi', 'conj']][men_init['civilstate'] == 2]
        verif = merge(decla, decla, left_on='noi', right_on='conj')
        Pb = verif[ verif['noi_y'] != verif['conj_x'] ]
        print len(Pb), "couples non appariés"
        
        # Pour faciliter la lecture par la suite :
        var = ['period', 'noi', 'agem', 'age', 'sexe', 'pere', 'mere',
                     'conj', 'civilstate', 'findet', 'workstate', 'Sali', 'men', 'quimen',
                     'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6', 'enf7', 'enf8', 'enf9']
        men = DataFrame(men_init, columns= var)
        
        # 2- Ménages constitués de couples
        
        # 1ere étape : détermination des têtes de ménage 
        # Personne en couple ayant l'identifiant le plus petit  et leur conjoint
        men.loc[(men['conj'] > men['noi']) & men['civilstate'].isin([2, 5]), 'quimen'] = 0 
        men.loc[(men['conj'] < men['noi']) & men['civilstate'].isin([2, 5]), 'quimen'] = 1         
        print len (men[men['quimen'] == 0])  # 9457
        print len (men[men['quimen'] == 1])  # 9457
        
        # Célibataires veuves ou divorcées ayant entre 22 et 75 ans pour les femmes
        men.loc[men['civilstate'].isin([1, 3, 4]) & (men['age'] < 76) & (men['age'] > 21) & (men['sexe'] == 2), 'quimen'] = 0
        print len (men[men['quimen'] == 0])  # 14 478 : +5021
        
        # Célibataires ou veufs ayant entre 25 et 75 ans pour les hommes
        men.loc[men['civilstate'].isin([1, 3, 4]) & (men['age'] < 76) & (men['age'] > 24) & (men['sexe'] == 1), 'quimen'] = 0
        print len (men[men['quimen'] == 0])  # 18 410 : + 3932
                        
        # Cas particuliers
        # a - Fille de plus de 75 ans ayant identifiants très proches de la mère
        value = [1537, 1539, 1541, 1543]
        men.loc[men['noi'].isin(value), 'quimen'] = 0
        # b - Majeurs n'ayant aucun parent spécifié
        men.loc[men['pere'].isnull() & men['mere'].isnull() & (25>men['age']) & (men['age']>17), 'quimen'] = 0
        # c- jeunes ayant déjà commencé à travailler
        men.loc[ (men['Sali'] != 0) & (76>men['age']) & men['quimen'].isnull(), 'quimen'] = 0
        
        # 2eme étape : attribution du numéro de ménage grâce à la tête de ménage 
        nb_men = len (men[men['quimen'] == 0])
        print "Le nombre de ménages constitués est :" + str(nb_men)
        men['men'][men['quimen'] == 0] = range(0, nb_men)  
        
        # 3eme étape : attribution du numéro de ménage  aux conjoints
        men_conj = men[['men', 'conj']]
        men_conj.rename(columns={'conj': 'noi'}, inplace=True)
        men = merge(men, men_conj, how='left', on='noi',
                    left_index=False, right_index=False,
                    suffixes=('', '_conj'), copy=True)        
        men.loc[men['men'].isnull(), 'men'] = men.loc[men['men'].isnull(), 'men_conj']
        men = men.loc[:, :'quimen']

        # 4eme étape : attribution du numéro de ménages aux autres personnes du ménage 
        # -> enfants de moins de 21 ans si fille et de moins de 25 ans si garçon et conjoints 
        men_link = men.loc[~men['men'].isnull(), ['men', 'noi', 'pere', 'mere', 'conj', 
                                                  'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6', 'enf7', 'enf8', 'enf9']]
        men_link = men_link.set_index('men').stack().reset_index()
        men_link = men_link.rename(columns={'level_1': 'link', 0:'noi'}) 
        men = merge(men, men_link, how='left', on='noi',
                    left_index=False, right_index=False,
                    suffixes=('_x', '_y'), copy=True)
        
        # On attribue le numéro de ménage aux chefs de ménage 
        men['men'] = -1
        men.loc[men['link'].isin(['conj', 'noi']), 'men'] = men.loc[men['link'].isin(['conj', 'noi']), 'men_y']

        # Attribution aux enfants et aux parents à charge 
        men.loc[((men['men'] == -1) | men['men'].isnull()) & ~men['quimen'].isin([0, 1]), 'men'] = men.loc[((men['men'] == -1) | men['men'].isnull()) & ~men['quimen'].isin([0, 1]), 'men_y']
        # men = men.loc[men['men'] != -1, 'noi' :]
        
        # Enfants sans parents :  dans un foyer fictif équivalent à la DASS = -5
        men.loc[ men['pere'].isnull() &  men['mere'].isnull() & (men['age']<18), 'men' ] = -5
       
        # Problème des parents non reconnus par les enfants (235 parents) : deux tables réutilisées plus haut
        par_lost = men.loc[men['men'].isnull() & ~men['enf1'].isnull(), ['sexe', 'age', 'noi', 
                                                                         'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6','enf7', 'enf8', 'enf9' ]]
        pere_lost = par_lost.loc[par_lost['sexe'] == 1, 'noi' : ]
        pere_lost = pere_lost.set_index('noi').stack().reset_index()
        pere_lost.rename(columns={'noi': 'pere', 'level_1': 'link', 0:'noi'}, inplace=True)
        
        mere_lost = par_lost.loc[par_lost['sexe'] == 2, 'noi' : ]
        mere_lost = mere_lost.set_index('noi').stack().reset_index()
        mere_lost.rename(columns={'noi': 'mere', 'level_1': 'link', 0:'noi'}, inplace=True)       
        pere_lost[['pere', 'noi']].astype(int).to_csv('pere_sup.csv')
        mere_lost[['mere', 'noi']].astype(int).to_csv('mere_sup.csv')
        
        # A ce stade deux types de parents à charge :         
                # + ceux ayant aucun enfant : attribués au ménage '-4' équivalent de la maison de retraite
                # + ceux ayant plusieurs enfants: quel enfant?
                
        men.loc[men['men'].isnull() & men['enf1'].isnull() & (men['age'] > 74), 'men'] = -4
        men = men.loc[men['men'] != -1, var]
        men = men.loc[~men.duplicated(var),:]
        # TO DO : associer les parents dépendants à leurs enfants 
        # idée : construire index de récurrence par 'noi' + aléa avec proba association enf1, proba asso enf2 .. proba maison de retraite 
        # dup = men.groupby(var)
        # men_ind = men.set_index(var)
        # men_ind.to_csv('men_ind.csv')
        # men['dup_men'] = men_ind.index.map(lambda ind: dup.indices[ind][0])
#        men.to_csv('testmen.csv')

        # Sorties
        men_init = men
        men_nodup = men_init.groupby(by='noi').first()
        print "Vérification du nombre d'individus en 2009 : " + str(len(men_nodup))
        print "temps de la mise au format : " + str(time.time() - start_time) + "s"
        print "Fin de la mise au format"

data = Destinie()
data.lecture()