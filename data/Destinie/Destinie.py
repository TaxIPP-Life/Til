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
from data.utils import minimal_dtype, drop_simult_row
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
        # self.max_dur = 
        self.survey_year = 2009
        self.last_year = 2060
        self.survey_date = 100*self.survey_year + 1
        
        # TODO: Faire une fonction qui check où on en est, si les précédent on bien été fait, etc.
        # TODO: Dans la même veine, on devrait définir la suppression des variables en fonction des étapes à venir.
        self.done = []
        self.methods_order = ['load']
       
    def lecture_BioEmp(self):
        longueur_carriere = 106 #self.max_dur
        print "début de l'importation des données"
        start_time = time.time()
        # TODO: revoir le colnames de BioEmp : le retirer ?
        colnames = list(xrange(longueur_carriere)) 

        BioEmp = pd.read_table(path_data_destinie + 'BioEmp.txt', sep=';',
                               header=None, names=colnames)
            
        #def _correction_fam():
            # Ambiguité sur Pacs/marié (ex : 8669 et 8668 se déclarent en couple mais l'un en marié l'autre en pacsé)
            #BioFam.loc[BioFam['civilstate'] == 5, 'civilstate'] = 2
            #BioFam.loc[((BioFam['civilstate'] == 2) | BioFam['civilstate'].isnull()) & (BioFam['conj'].isnull()| (BioFam['conj'] == 0)), 'civilstate'] = 1
            
        def _BioEmp_in_3():
            ''' Division de BioEmpen trois tables '''
            taille = len(BioEmp)/3
            BioEmp['id'] = BioEmp.index/3
            
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
            statut = statut.set_index('id').stack().reset_index()
            statut = statut.rename(columns={'level_1':'period', 0:'workstate'})
            statut = statut[['id', 'period', 'workstate']]
            
            # selection2 : informations sur les salaires
            selection2 = [3*x + 2 for x in range(taille)]
            sal = BioEmp.iloc[selection2]
            sal = sal.set_index('id').stack().reset_index()
            sal = sal.rename(columns={'level_1':'period', 0:'sali'})
            sal = sal[['sali']].astype(int) 

            return ind, statut, sal
        
        def _Emp_format():
            ''' Mise en forme des données sur carrières'''
            emp_tot = merge(statut, sal, left_index=True, right_index=True, sort=False)  
            emp_tot = emp_tot[['id', 'period', 'workstate', 'sali']]
            emp_tot = merge(emp_tot, ind[['naiss']], left_on = 'id', right_on = ind[['naiss']].index)
            emp_tot['period'] = emp_tot['period'] + emp_tot['naiss']
            emp_tot =  emp_tot[['id','period', 'workstate', 'sali']]
            # Mise au format minimal
            emp_tot = emp_tot.fillna(np.nan).replace(-1, np.nan)
            emp_tot = minimal_dtype(emp_tot)
            return emp_tot
                  
        print "fin de l'importation de BioEmp"
    
        start_time = time.time()
        ind, statut, sal = _BioEmp_in_3()
        emp_tot = _Emp_format()
        emp_tot_mini = drop_simult_row(emp_tot, ['id', 'workstate','sali']) # prend énormément de temps! A AMELIORER
        print "temps ecoule pour BioEmp : " + str(time.time() - start_time) + "s" 
        self.ind = ind
        self.emp_tot = emp_tot
        self.emp_tot_mini = emp_tot_mini
        
    def lecture_BioFam(self):
        
        print "Début importation BioFam"
        start_time = time.time()
        BioFam = pd.read_table(path_data_destinie + 'BioFam.txt', sep=';',
                               header=None, names=['id', 'pere', 'mere', 'civilstate',
                                                   'conj', 'enf1', 'enf2',
                                                   'enf3', 'enf4', 'enf5', 'enf6']) 
        # 1 - Variable 'date de mise à jour'
        # Index limites pour changement de date
        delimiters = BioFam['id'].str.contains('Fin')
        annee = BioFam[delimiters].index.tolist()  # donne tous les index limites
        annee = [-1] + annee # to simplify loops later
        # create a series period
        period = []
        for k in range(len(annee)-1):
            period = period + [2009+k]*(annee[k+1]-1-annee[k])

        BioFam = BioFam[~delimiters]
        BioFam['period'] = period
        BioFam[['id', 'pere']] = BioFam[['id', 'pere']].fillna(0).astype(int)
        list_enf = ['enf1','enf2','enf3','enf4','enf5','enf6']
        BioFam[list_enf + ['id','pere','mere', 'conj']] -= 1
        BioFam = BioFam.fillna(-1)
        # Minimal format
        #BioFam = BioFam.replace('-1', np.nan)
        #BioFam = BioFam.replace(-1, np.nan)
        #BioFam = minimal_dtype(BioFam)

        self.BioFam = BioFam
        print "Fin traitement BioFam"
        
       
    def Tables_ini(self):
        ind = self.ind
        BioFam = self.BioFam
        emp_tot = self.emp_tot
        print "Début de l'initialisation des données pour 2009"
        
    # 1-  Sélection des individus présents en 2009 et vérifications des liens de parentés
        year_ini = self.survey_year # = 2009 
        ind = merge(ind.loc[ind['naiss'] <= year_ini], BioFam[BioFam['period']==year_ini], 
                    left_index=True, right_index=True, how='left')
        
        print "Nombre d'individus dans la base initiale de 2009 : " + str(len(ind))
        ind = ind.replace(-1, np.nan)
        ind = ind.fillna(np.nan)
        # Déclarations initiales des enfants
        pere_ini = ind[['pere']].fillna(-1).astype(int)
        mere_ini = ind[['mere']].fillna(-1).astype(int)
        list_enf = ['enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6']
        # Comparaison avec les déclarations initiales des parents      
        for par in ['pere', 'mere'] :   
            #a -Définition des tables initiales:  
            if par == 'pere':
                par_ini = pere_ini
                sexe = False
            else :
                par_ini = mere_ini
                sexe = True     
            # b -> construction d'une table a trois entrées : 
            #     par_decla = identifiant du parent déclarant l'enfant
            #     par_ini = identifiant du parent déclaré par l'enfant
            #     enf = identifiant de l'enfant (déclaré ou déclarant)
            par_ini = par_ini[par_ini[par] != -1]
            link = ind.loc[ind['enf1'].notnull()  & (ind['sexe'] == sexe),  list_enf]
            link = link.stack().reset_index().rename(columns={'level_0': par, 'level_1': 'link', 0 :'enf'})[[par,'enf']].astype(int)
            link = merge(link, par_ini, left_on = 'enf', right_on = par_ini.index, 
                                suffixes=('_decla', '_ini'), how = 'outer').fillna(-1)
            link = link[ (link[ par + '_decla'] != -1 ) | (link[par + '_ini'] != -1)]
            ind['men_' + par] = 0
            
            # c- Comparaisons et détermination des liens
            # Cas 1 : enfants et parents déclarent le même lien : ils vivent ensembles
            parents = link[link[par +'_decla'] == link[par + '_ini'] ] ['enf']
            ind['men_' + par][parents.values] = 1

            # Cas 2 : enfants déclarant un parent mais ce parent ne les déclare pas (rattachés au ménage du parent)
            # Remarques : 9 cas pour les pères, 10 pour les mères
            parents = link[(link[par + '_decla'] != link[ par +'_ini']) & (link[par +'_decla'] == -1) ] ['enf']
            ind['men_'+ par][parents.values] = 1
            print str(sum(ind['men_' + par]==1)) + " vivent avec leur " + par
            
            # Cas 3 : parent déclarant un enfant mais non déclaré par l'enfant (car hors ménage)
            # Aucune utilisation pour l'instant (men_par = 0) mais pourra servir pour la dépendance
            parents = link[(link[par +'_decla'] != link[par +'_ini']) & (link[par +'_ini'] == -1) ] [['enf', par +'_decla']]
            ind[par][parents['enf'].values] = parents[par + '_decla'].values
            print str(sum((ind[par].notnull() & (ind[par] != -1 )))) + " enfants connaissent leur " + par

        ind = ind[['sexe', 'naiss', 'findet', 'tx_prime_fct', 'pere', 'mere', 'civilstate', 'conj', 'men_pere', 'men_mere']]
        
        # valeurs négatives à np.nan pour la fonction minimal_type     
        ind.loc[ind['conj'] < 0, 'conj'] = np.nan
        ind.loc[ind['pere'] < 0,'pere'] = np.nan
        ind.loc[ind['mere'] < 0,'mere'] = np.nan
        ind = minimal_dtype(ind)

        
    # 2- Constitution des ménages de 2009
        ind['quimen'] = -1
        ind['men'] = -1
        ind['age'] = year_ini - ind['naiss']

        
        # 1ere étape : Détermination des têtes de ménages
        ind['id'] = ind.index 
        
        # (a) - Majeurs ne déclarant ni père, ni mère dans le même ménage (+ un cas de 17 ans indep.financièrement)
        maj = ind.loc[(ind['men_pere'] == 0)&(ind['men_mere'] == 0) & (ind['age']>16)].index
        ind['quimen'][maj] = 0
        
        # (b) - Personnes prenant en charge d'autres individus
            # Mères avec enfants à charge : (ne rajoute aucun ménage)
        enf_mere = ind.loc[(ind['men_pere'] == 0)&(ind['men_mere'] == 1) & (ind['age']<26), 'mere'].astype(int)
        ind['quimen'][enf_mere.values] = 0
            # Pères avec enfants à charge :(ne rajoute aucun ménage)
        enf_pere = ind.loc[(ind['men_mere'] == 0)&(ind['men_pere'] == 1) & (ind['age']<26), 'pere'].astype(int)
        ind['quimen'][enf_pere.values] = 0
            # Personnes ayant un parent à charge de plus de 70 ans : (rajoute 387 ménages)
        for par in ['mere', 'pere']:
            care_par = ind.loc[(ind['men_' + par] == 1), ['id', par]].astype(int)
            par_care = ind.loc[(ind['age'] >69) & (ind['id'].isin(care_par[par].values)), ['id']]
            care_par = merge(care_par, par_care, left_on = par, 
                             right_on='id', how = 'inner', 
                             suffixes = ('_enf', '_'+par))[['id_enf', 'id_'+par]]
            # Enfant ayant des parents à charge deviennent tête de ménage, parents à charge n'ont pas de foyers
            ind['quimen'][care_par['id_enf']] = 0
            ind['quimen'][care_par['id_'+par]] = -2 # pour identifier les couples à charge
            print str(len(care_par)) +" " + par + "s à charge"
        
        # (c) - Correction pour les personnes en couple non à charge [identifiant le plus petit = tête de ménage]
        ind.loc[( ind['conj'] > ind['id'] ) & ( ind['civilstate'] == 2 ) & (ind['quimen']!=-2), 'quimen'] = 0 
        ind.loc[(ind['conj'] < ind['id']) & ( ind['civilstate'] == 2 )& (ind['quimen']!=-2), 'quimen'] = 1         
        print str(len (ind[ind['quimen'] == 0])) + " ménages ont été constitués " # 20873
        print "   dont " + str(len (ind[ind['quimen'] == 1])) +  " couples "   # 9380
        
        # 2eme étape : attribution du numéro de ménage grâce aux têtes de ménage
        nb_men = len(ind[ind['quimen'] == 0]) 
        ind['men'][ind['quimen'] == 0] = range(0, nb_men)
        
        # 3eme étape : Rattachement des autres membres du ménage
        
        # (a) - Rattachements des conjoints des personnes en couples 
        conj = ind.loc[ind['conj'].notnull() & (ind['quimen'] == 0), ['conj','men']].astype(int)
        ind['men'][conj['conj'].values] = conj['men'].values
        
        # (b) - Rattachements de leurs enfants (d'abord ménage de la mère, puis celui du père)
        for par in ['mere', 'pere']:
            enf_par = ind.loc[(ind['men_' + par] == 1) & (ind['men'] == -1), par].astype(int)
            ind['men'][enf_par.index.values] = ind['men'][enf_par.values]
            #print str(sum((ind['men']!= -1)))  + " personnes ayant un ménage attribué"
            
        # (c) - Rattachements des éventuels parents à charge
        for par in ['mere', 'pere']:
            care_par = ind.loc[(ind['men_' + par] == 1) & (ind['men'] != -1), par].astype(int)
            ind['men'][care_par.values] = ind['men'][care_par.index.values]
            #print str(sum((ind['men']!= -1)))  + " personnes ayant un ménage attribué"

        # 4eme étape : création de deux ménages fictifs résiduels :
        # Enfants sans parents :  dans un foyer fictif équivalent à la DASS = -4
        ind.loc[ (ind['men']== -1) & (ind['age']<18), 'men' ] = -4
        
        # TO DO ( Quand on sera à l'étape gestion de la dépendance ) :
        # créer un ménage fictif maison de retraite + comportement d'affectation.
        
        # 5eme étape : attribution des quimen pour les personnes non référentes
        df = ind.groupby('men').size()
        ind.loc[~ind['quimen'].isin([0,1]), 'quimen'] = 2
        
        # 6eme étape : création de la table ménage
        # TO DO
        
        # TO DROP : Petit check sur mes cas particuliers
        
        ind[ind['id'].isin([1536, 1537, 1538, 1539, 1540, 1541, 1542])].to_csv('idd.csv')
        # print str(sum((ind['men']== -1))) # Tous le monde à un ménage : on est content!
        
    
    # 3- Opérations préalables à la création des foyers
        # Ajouts des information sur les statuts + salaires sur le marché du travail (tjs sur table 2009)
        ind = merge(ind, emp_tot[emp_tot['period'] == self.survey_year],  on='id', how ='left', sort=False)

        # Création de la variable 'nb_enf'
        ## nb d'enfant
        nb_enf_mere= ind.groupby('mere').size()
        nb_enf_pere = ind.groupby('pere').size()
        enf_tot = pd.concat([nb_enf_mere, nb_enf_pere], axis=1)
        enf_tot = enf_tot.sum(axis=1)
        # Comme enf_tot a le bon index on fait
        ind['nb_enf'] = enf_tot
        ind['nb_enf'] = ind['nb_enf'].fillna(0)
        
        #ind = ind.replace('-1', np.nan)
        #ind = ind.replace(-1, np.nan)
        #ind = minimal_dtype(ind)
        
        ind.to_csv('pers.csv')
        
        self.ind = ind
    
    def add_change(self):
        print "Début de l'actualisation des changements jusqu'en 2060"
        ind = self.ind
        emp_tot_mini = self.emp_tot_mini
        BioFam = self.BioFam
        
        # On ne garde pour l'instant que les informations sur l'emploi postérieures à 2009
        emp = emp_tot_mini[emp_tot_mini['period'] > 2009]
        
        # Changements familiaux (postérieurs à 2009 par défaut)
        
        
        # On merge 
        
        
        self.ind = ind
        print "Fin de l'actualisation des changements jusqu'en 2060"
    
data = Destinie()
# Importation des données et corrections préliminaires
data.lecture_BioEmp()
data.lecture_BioFam()
data.correction_civilstate()
# Création de la table ind de 2009 (infos de BioFam + BioEmp pour les personnes présentes en 2009) et men en 2009
data.Tables_ini()
# Constitutions des foyers fiscaux de 2009
data.creation_foy()
# Actualisations des changements : Une ligne par changement

        
pdb.set_trace() 
        
        
        
        
        
'''
        
        ## Note importante, on suppose que l'on repère parfaitement les décès avec BioEmp (on oublie du coup les valeurs négatives)
        BioFam[['id', 'civilstate', 'period']] = BioFam[['noi', 'civilstate', 'period']].astype(int)

        
        # 2 - Sortie de la table agrégée contenant les infos de BioEmp -> pers
        m1 = merge(statut, sal, left_index=True, right_index=True, sort=False)  #  on ='index', sort = False)
        m1 = m1[['noi', 'period', 'workstate', 'sali']]
        pers = merge(m1, ind, on='noi', sort=False)
        pers.to_csv('pers.csv')
        pdb.set_trace()
        # pers = pers.iloc['noi','annee','statut','sali','sexe','naiss','findet']
        pers['period'] = pers['period'] + pers['naiss']

        
      
        
        # 2 - Fusion avec les informations sur déroulés des carrières
        # Informations sur BioFam qu'à partir de 2009 
        # -> on identifie père/mère avec les infos de 2060 + un moins indique leur mort donc reviennent à 0.
        # -> situation maritale : la même qu'en 2009 et après l'âge de fin d'étude, avant = célib et pas de conjoint.
        # -> info sur enfants : abandon.
        pers = pers.astype(int)
        
        # sélection des informations d'intéret 
        pers = merge(pers, BioFam, on=['noi', 'period'], how='left') 
        pers = pers[['period', 'id', 'sexe', 'naiss', 'findet', 'workstate', 'sali', 
                     'pere', 'mere', 'conj', 'civilstate', 'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6', 'enf7', 'enf8', 'enf9']]
        # Création d'une ligne fictive 2061 pour délimiter les fillna dans la partie suivante
        index_del = range(0, pers['id'].max() + 1)
        Delimit = pd.DataFrame(index=index_del,
                               columns=['period', 'id', 'sexe', 'naiss', 'findet',
                                         'workstate', 'sali', 'pere', 'mere',
                                         'conj', 'civilstate', 'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6',
                                         'enf7', 'enf8', 'enf9'])
        Delimit['period'] = self.last_year + 1
        Delimit['id'] = Delimit.index + 1
        Delimit.loc[:, 'sexe':] = -99999999
        Delimit = Delimit.astype(int)  # A remplacer par la suite 
        pers = pers.append(Delimit)
        pers = pers.sort(['id', 'period'])
        # pers[ pers['id'] <4 ].to_csv('index.csv')  #-> lignes 2061 bien ordonné grâce au sort mais pb d'index
        
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
#        pers.loc[pers['id'].isin(strange), :].to_csv('pers_test.csv')

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
        pers = pers[['period', 'id', 'agem', 'age', 'sexe', 'pere', 'mere',
                     'conj', 'civilstate', 'findet', 'workstate', 'Sali']]


        # pers.loc[:, 'id':] =pers.loc[:, 'id':].astype(int)
        
        list_val = [1480, 12455, 12454,
                    1481, 33425, 33426,
                    ]
        # strange = pers[pers['id'].isin(list_val)]
        # strange.to_csv('strange.csv')
        
    # def crea_men(self) :  
          
        # 1- creation des ménages en 2009
        men_init = pers[pers['period'] == self.survey_date]  
        print "Nombre d'individus en 2009 :" + str(len(men_init))
        # Fiabilité des déclarations : 
        decla = men_init[['id', 'conj']][men_init['civilstate'] == 2]
        verif = merge(decla, decla, left_on='id', right_on='conj')
        Pb = verif[ verif['id_y'] != verif['conj_x'] ]
        print len(Pb), "couples non appariés"
        
        # Pour faciliter la lecture par la suite :
        var = ['period', 'id', 'agem', 'age', 'sexe', 'pere', 'mere',
                     'conj', 'civilstate', 'findet', 'workstate', 'Sali', 'men', 'quimen',
                     'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6', 'enf7', 'enf8', 'enf9']
        men = DataFrame(men_init, columns= var)
        
        # 2- Ménages constitués de couples
        
        # 1ere étape : détermination des têtes de ménage 
        # Personne en couple ayant l'identifiant le plus petit  et leur conjoint
        men.loc[(men['conj'] > men['id']) & men['civilstate'].isin([2, 5]), 'quimen'] = 0 
        men.loc[(men['conj'] < men['id']) & men['civilstate'].isin([2, 5]), 'quimen'] = 1         
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
        men.loc[men['id'].isin(value), 'quimen'] = 0
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
        men_conj.rename(columns={'conj': 'id'}, inplace=True)
        men = merge(men, men_conj, how='left', on='id',
                    left_index=False, right_index=False,
                    suffixes=('', '_conj'), copy=True)        
        men.loc[men['men'].isnull(), 'men'] = men.loc[men['men'].isnull(), 'men_conj']
        men = men.loc[:, :'quimen']

        # 4eme étape : attribution du numéro de ménages aux autres personnes du ménage 
        # -> enfants de moins de 21 ans si fille et de moins de 25 ans si garçon et conjoints 
        men_link = men.loc[~men['men'].isnull(), ['men', 'id', 'pere', 'mere', 'conj', 
                                                  'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6', 'enf7', 'enf8', 'enf9']]
        men_link = men_link.set_index('men').stack().reset_index()
        men_link = men_link.rename(columns={'level_1': 'link', 0:'id'}) 
        men = merge(men, men_link, how='left', on='id',
                    left_index=False, right_index=False,
                    suffixes=('_x', '_y'), copy=True)
        
        # On attribue le numéro de ménage aux chefs de ménage 
        men['men'] = -1
        men.loc[men['link'].isin(['conj', 'id']), 'men'] = men.loc[men['link'].isin(['conj', 'id']), 'men_y']

        # Attribution aux enfants et aux parents à charge 
        men.loc[((men['men'] == -1) | men['men'].isnull()) & ~men['quimen'].isin([0, 1]), 'men'] = men.loc[((men['men'] == -1) | men['men'].isnull()) & ~men['quimen'].isin([0, 1]), 'men_y']
        # men = men.loc[men['men'] != -1, 'id' :]
        
        # Enfants sans parents :  dans un foyer fictif équivalent à la DASS = -5
        men.loc[ men['pere'].isnull() &  men['mere'].isnull() & (men['age']<18), 'men' ] = -5
       
        # Problème des parents non reconnus par les enfants (235 parents) : deux tables réutilisées plus haut
        par_lost = men.loc[men['men'].isnull() & ~men['enf1'].isnull(), ['sexe', 'age', 'id', 
                                                                         'enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6','enf7', 'enf8', 'enf9' ]]
        pere_lost = par_lost.loc[par_lost['sexe'] == 1, 'id' : ]
        pere_lost = pere_lost.set_index('id').stack().reset_index()
        pere_lost.rename(columns={'id': 'pere', 'level_1': 'link', 0:'id'}, inplace=True)
        
        mere_lost = par_lost.loc[par_lost['sexe'] == 2, 'id' : ]
        mere_lost = mere_lost.set_index('id').stack().reset_index()
        mere_lost.rename(columns={'id': 'mere', 'level_1': 'link', 0:'id'}, inplace=True)       
        pere_lost[['pere', 'id']].astype(int).to_csv('pere_sup.csv')
        mere_lost[['mere', 'id']].astype(int).to_csv('mere_sup.csv')
        
        # A ce stade deux types de parents à charge :         
                # + ceux ayant aucun enfant : attribués au ménage '-4' équivalent de la maison de retraite
                # + ceux ayant plusieurs enfants: quel enfant?
                
        men.loc[men['men'].isnull() & men['enf1'].isnull() & (men['age'] > 74), 'men'] = -4
        men = men.loc[men['men'] != -1, var]
        men = men.loc[~men.duplicated(var),:]
        # TO DO : associer les parents dépendants à leurs enfants 
        # idée : construire index de récurrence par 'id' + aléa avec proba association enf1, proba asso enf2 .. proba maison de retraite 
        # dup = men.groupby(var)
        # men_ind = men.set_index(var)
        # men_ind.to_csv('men_ind.csv')
        # men['dup_men'] = men_ind.index.map(lambda ind: dup.indices[ind][0])
#        men.to_csv('testmen.csv')

        # Sorties
        men_init = men
        men_nodup = men_init.groupby(by='id').first()
        print "Vérification du nombre d'individus en 2009 : " + str(len(men_nodup))
        print "temps de la mise au format : " + str(time.time() - start_time) + "s"
        print "Fin de la mise au format"

'''