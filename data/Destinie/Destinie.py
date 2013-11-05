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
from data.utils import minimal_dtype, drop_consecutive_row
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
       
    def load(self):
        def _BioEmp_in_3():
            ''' Division de BioEmpen trois tables '''
            longueur_carriere = 106 #self.max_dur
            start_time = time.time()
            # TODO: revoir le colnames de BioEmp : le retirer ?
            colnames = list(xrange(longueur_carriere)) 
    
            BioEmp = pd.read_table(path_data_destinie + 'BioEmp.txt', sep=';',
                                   header=None, names=colnames)
    
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
            statut = statut[['id', 'period', 'workstate']] #.fillna(np.nan)
            #statut.loc[statut['workstate']<0, 'workstate'] = np.nan
            statut = minimal_dtype(statut)
            
            # selection2 : informations sur les salaires
            selection2 = [3*x + 2 for x in range(taille)]
            sal = BioEmp.iloc[selection2]
            sal = sal.set_index('id').stack().reset_index()
            sal = sal.rename(columns={'level_1':'period', 0:'sali'})
            sal = sal[['sali']].fillna(np.nan)
            sal = minimal_dtype(sal)
            return ind, statut, sal
        
        def _lecture_BioFam():
            BioFam = pd.read_table(path_data_destinie + 'BioFam.txt', sep=';',
                                   header=None, names=['id', 'pere', 'mere', 'civilstate',
                                                       'conj', 'enf1', 'enf2',
                                                       'enf3', 'enf4', 'enf5', 'enf6']) 
            
            #TODO: remonter ici le code de _Bio_Format? 
            
            return BioFam_ini, BioFam
                  
        print "Début de l'importation des données"
        start_time = time.time()
        #BioEmp = _lecture_BioEmp()
        ind, statut, sal = _BioEmp_in_3()
        BioFam = _lecture_BioFam()
        
        self.ind = ind
        self.statut = statut
        self.sal = sal
        print "Temps d'importation des données : " + str(time.time() - start_time) + "s" 
        print "fin de l'importation des données"

        
    def format_initial(self): 
        def _Bio_format() :  
            BioFam = self.BioFam
            # 1 - Variable 'date de mise à jour'
            # Index limites pour changement de date
            delimiters = BioFam['id'].str.contains('Fin')
            annee = BioFam[delimiters].index.tolist()  # donne tous les index limites
            annee = [-1] + annee # in order to simplify loops later
            # create a series period
            period = []
            for k in range(len(annee)-1):
                period = period + [2009+k]*(annee[k+1]-1-annee[k])
    
            BioFam = BioFam[~delimiters]
            
            BioFam['period'] = period
            #BioFam[['id', 'pere']] = BioFam[['id', 'pere']].fillna(0).astype(int)
            list_enf = ['enf1','enf2','enf3','enf4','enf5','enf6']
            BioFam[list_enf + ['pere','mere', 'conj']] -= 1
            BioFam['id'] = BioFam['id'].astype(int) - 1
            for var in ['pere','mere', 'conj'] + list_enf:
                BioFam.loc[BioFam[var] < 0 , var] = np.nan
            BioFam = BioFam.fillna(np.nan)
            self.BioFam = minimal_dtype(BioFam)
             
        def _Emp_format(statut, sal, ind):
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

        print "Début de la mise en forme initiale"
        start_time = time.time()
        _Bio_format()
        self.emp_tot = _Emp_format(self.statut, self.sal, self.ind)
        emp_tot_mini = drop_consecutive_row(self.emp_tot, ['id', 'workstate','sali'])
        self.emp_tot_mini = emp_tot_mini
        print "Temps de la mise en forme initiale : " + str(time.time() - start_time) + "s" 
        print "Fin de la mise en forme initiale"

    def table_initial(self):
        ind = self.ind
        BioFam = self.BioFam
        emp_tot = self.emp_tot
        print "Début de l'initialisation des données pour 2009"

    # 1-  Sélection des individus présents en 2009 et vérifications des liens de parentés
        year_ini = self.survey_year # = 2009 
        ind = merge(ind.loc[ind['naiss'] <= year_ini], BioFam[BioFam['period']==year_ini], 
                    left_index=True, right_index=True, how='left').fillna(-1)
        
        print "Nombre d'individus dans la base initiale de 2009 : " + str(len(ind))
        #Déclarations initiales des enfants
        pere_ini = ind['pere']
        mere_ini = ind['mere']
        list_enf = ['enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6']
        # Comparaison avec les déclarations initiales des parents      
        for par in ['pere', 'mere'] :   
            #a -Définition des tables initiales:  
            if par == 'pere':
                par_ini = pere_ini
                sexe = 0
            else :
                par_ini = mere_ini
                sexe = 1    
            # b -> construction d'une table a trois entrées : 
            #     par_decla = identifiant du parent déclarant l'enfant
            #     par_ini = identifiant du parent déclaré par l'enfant
            #     enf = identifiant de l'enfant (déclaré ou déclarant)
            par_ini = par_ini[par_ini[par] != -1]
            link = ind.loc[(ind['enf1'] != -1 )& (ind['sexe'] == sexe),  list_enf]
            link = link.stack().reset_index().rename(columns={'level_0': par, 'level_1': 'link', 0 :'enf'})[[par,'enf']].astype(int)
            link = link[link['enf'] != -1]
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
        print str(len (ind[ind['quimen'] == 0])) + " ménages ont été constitués " # 21166
        print "   dont " + str(len (ind[ind['quimen'] == 1])) +  " couples "   # 9087
        
        # 2eme étape : attribution du numéro de ménage grâce aux têtes de ménage
        nb_men = len(ind[ind['quimen'] == 0]) 
        ind['men'][ind['quimen'] == 0] = range(0, nb_men)
        
        # 3eme étape : Rattachement des autres membres du ménage
        # (a) - Rattachements des conjoints des personnes en couples 
        conj = ind.loc[(ind['conj'] != -1) & (ind['quimen'] == 0), ['conj','men']].astype(int)
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
        ind.loc[~ind['quimen'].isin([0,1]), 'quimen'] = 2
        
        # 6eme étape : création de la table men
        men = ind[['id', 'men']]
        men = men.rename(columns= {'id': 'pref', 'men': 'id'})
        for var in ['loyer', 'tu', 'zeat', 'surface', 'resage', 'restype', 'reshlm', 'zcsgcrds','zfoncier','zimpot', 'zpenaliv','zpenalir','zpsocm','zrevfin']:
            men[var] = np.nan
        men['pond'] = 1
        assert sum(ind['men']==-1) == 0 # Tout le monde a un ménage : on est content!
    
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
        
        self.ind = ind.fillna(-1)
        self.men = men
    
    def add_change(self):
        print "Début de l'actualisation des changements jusqu'en 2060"
        ind = self.ind
        emp_tot_mini = self.emp_tot_mini
        BioFam = self.BioFam
        
        # On ne garde pour l'instant que les informations sur l'emploi postérieures à 2009
        emp = emp_tot_mini[emp_tot_mini['period'] > self.survey_year]
        # On merge (changements familiaux (postérieurs à 2009 par défaut))
        Bio = BioFam[['id', 'period', 'pere', 'mere', 'civilstate', 'conj']]
        Bio = drop_consecutive_row(Bio.sort(['id', 'period']), ['id', 'pere', 'mere', 'civilstate', 'conj'])
        Bio = Bio[Bio['period'] > self.survey_year]
        actu = merge(emp, Bio, on = ['id', 'period'], how = 'outer').fillna(-1)
        actu['period'] = actu['period']*100 + 1
        
        # On ajoute ces données aux informations de 2009
        ind = ind.append(actu, ignore_index = True)
        ind = ind.replace(-1, np.nan)
        
        # On sort la table au format minimal
        ind = minimal_dtype(ind)
        ind = ind.fillna(-1)
        self.ind = ind
        print "Fin de l'actualisation des changements jusqu'en 2060"
    
if __name__ == '__main__':
    
    data = Destinie()
    start_t = time.time()
    # Importation des données et corrections préliminaires
    data.load()
    data.format_initial()
    data.conjoint()
    data.table_initial()
    data.creation_foy()    
    data.var_sup()
    data.add_change()
    data.store_to_liam()

    print "Temps Destiny.py : " + str(time.time() - start_t) + "s" 