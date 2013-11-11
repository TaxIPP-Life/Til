# -*- coding:utf-8 -*-
'''
Created on 11 septembre 2013

Ce programme :
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
            ind['id'] = ind.index
            ind = minimal_dtype(ind)
            
            # selection1 : information sur les statuts d'emploi
            selection1 = [3*x + 1 for x in range(taille)]
            statut = BioEmp.iloc[selection1]
            statut = statut.set_index('id').stack().reset_index()
            statut = statut.rename(columns={'level_1':'period', 0:'workstate'})
            statut = statut[['id', 'period', 'workstate']] #.fillna(np.nan)
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
                                   header=None, names=['id','pere','mere','civilstate','conj',
                                                       'enf1','enf2','enf3','enf4','enf5','enf6'])   
            # Index limites pour changement de date
            delimiters = BioFam['id'].str.contains('Fin')
            annee = BioFam[delimiters].index.tolist()  # donne tous les index limites
            annee = [-1] + annee # in order to simplify loops later
            # create a series period
            year0 = self.survey_year
            period = []
            for k in range(len(annee)-1):
                period = period + [year0 + k]*(annee[k + 1] - 1 - annee[k])
                
            BioFam = BioFam[~delimiters]
            BioFam['period'] = period
            list_enf = ['enf1','enf2','enf3','enf4','enf5','enf6']
            BioFam[list_enf + ['pere','mere', 'conj']] -= 1
            BioFam['id'] = BioFam['id'].astype(int) - 1
            for var in ['pere','mere', 'conj'] + list_enf:
                BioFam.loc[BioFam[var] < 0 , var] = np.nan
            BioFam = BioFam.fillna(np.nan)
            BioFam = minimal_dtype(BioFam)
            return BioFam 
                  
        print "Début de l'importation des données"
        start_time = time.time()
        ind, statut, sal = _BioEmp_in_3()
        pdb.set_trace()
        self.BioFam = _lecture_BioFam()
        self.ind = ind
        #TODO: join BioFam
        self.futur = statut.join(sal)
        print "Temps d'importation des données : " + str(time.time() - start_time) + "s" 
        print "fin de l'importation des données"

        
    def format_initial(self): 
        '''
        Ensemble des informations dans deux bases : 
            - ind : démographiques + caractéristiques indiv
            - emp_tot : déroulés de carrières et salaires associés
        '''

        def _ind_merge(BioFam, ind, emp_tot):
            ''' fusiempon : BioFam + ind + Emp_tot -> ind
            Rq : pour l'instant on ne garde que les données postérieures à 2009'''
            #TODO: faire past
            survey_year = self.survey_year
            to_ind = merge(emp_tot, BioFam, on=['id','period'], how ='outer', 
                           right_index=False, left_index=False)
            ind = merge(to_ind, ind, on='id', how ='outer', right_index=False, left_index=False)
            #ind = ind.sort('id')
            #ind = DataFrame(ind, index = ind.id.values)
            # L'index donne l'identifiant de l'individu
            ind = ind.loc[ind['period']>=survey_year].fillna(-1).reset_index()
            # Informations atemporelles ne doivent apparaitre que l'année où l'individu apparait dans la base
            ind.loc[ ind.sort(['id', 'period']).duplicated('id') == True, ['sexe', 'naiss', 'findet', 'tx_prime_fct']] = -1
            return ind
            
            print "Début du travail initial sur BioFam"
            start_time = time.time()
            emp_tot = _Emp_format(self.statut, self.sal, self.ind)
            ind = _ind_merge(BioFam, self.ind, emp_tot)
            self.ind = ind
            ind_add = ind[ind['period'] > self.survey_date]
            list_enf = ['enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6']
            ind_add = ind_add.drop(list_enf + ['index'],axis = 1)
            self.ind_add = ind_add
            print "Temps de la mise en forme initiale : " + str(time.time() - start_time) + "s" 
            print "Fin de la mise en forme initiale"
            return BioFam 
        
        #         def _Emp_format(statut, sal, ind):
#             ''' Mise en forme des données sur carrières'''
#             emp_tot = merge(emp_tot, ind[['naiss']], left_on = 'id', right_on = ind[['naiss']].index)
#             emp_tot['period'] = emp_tot['period'] + emp_tot['naiss']
#             emp_tot =  emp_tot[['id','period','workstate','sali']]
#             # Mise au format minimal
#             emp_tot = emp_tot.fillna(np.nan).replace(-1, np.nan)
#             emp_tot = minimal_dtype(emp_tot)
#             return emp_tot
        
        print "Début de la mise en forme initiale"
        start_time = time.time()
        emp_tot = _Emp_format(self.statut, self.sal, self.ind)
        BioFam = _Bio_format()
        ind = _ind_merge(BioFam, self.ind, emp_tot)
        self.ind = ind
        ind_add = ind[ind['period'] > self.survey_date]
        list_enf = ['enf1', 'enf2', 'enf3', 'enf4', 'enf5', 'enf6']
        ind_add = ind_add.drop(list_enf + ['index'],axis = 1)
        self.ind_add = ind_add
        print "Temps de la mise en forme initiale : " + str(time.time() - start_time) + "s" 
        print "Fin de la mise en forme initiale"

    def format_initial(self):
        '''Sélection des individus présents en 2009 et vérifications des liens de parentés '''
        
        ind = self.ind
        ind = ind.set_index('id')
        ind['id'] = ind.index
        year_ini = self.survey_year # = 2009 
        print "Début de l'initialisation des données pour " + str(year_ini)
        
        def _enf_to_par(ind):
            ind= ind[ind['period'] == year_ini]
            print "Nombre d'individus dans la base initiale de " + str(year_ini) +" : " + str(len(ind))
            #Déclarations initiales des enfants
            pere_ini = ind[['id', 'pere']]
            mere_ini = ind[['id', 'mere']]
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
                #     id = identifiant de l'enfant (déclaré ou déclarant)
                par_ini = par_ini[par_ini[par] != -1]
                link = ind.loc[(ind['enf1'] != -1 )& (ind['sexe'] == sexe),  list_enf]
                link = link.stack().reset_index().rename(columns={'id': par, 'level_1': 'link', 0 :'id'})[[par,'id']].astype(int)
                link = link[link['id'] != -1]
                link = merge(link, par_ini, on = 'id', suffixes=('_decla', '_ini'), 
                             how = 'outer').fillna(-1)
                link = link[(link[ par + '_decla'] != -1 ) | (link[par + '_ini'] != -1)]
                ind['men_' + par] = 0
                
                # c- Comparaisons et détermination des liens
                # Cas 1 : enfants et parents déclarent le même lien : ils vivent ensembles
                parents = link.loc[(link[par + '_decla'] == link[ par + '_ini']), 'id']
                ind['men_' + par][parents.values] = 1
                
                # Cas 2 : enfants déclarant un parent mais ce parent ne les déclare pas (rattachés au ménage du parent)
                # Remarques : 8 cas pour les pères, 10 pour les mères
                parents = link[(link[par + '_decla'] != link[ par +'_ini']) & (link[par +'_decla'] == -1) ] ['id']
                ind['men_'+ par][parents.values] = 1
                print str(sum(ind['men_' + par]==1)) + " vivent avec leur " + par
                
                # Cas 3 : parent déclarant un enfant mais non déclaré par l'enfant (car hors ménage)
                # Aucune utilisation pour l'instant (men_par = 0) mais pourra servir pour la dépendance
                parents = link.loc[(link[par +'_decla'] != link[par +'_ini']) & (link[par +'_ini'] == -1), ['id', par +'_decla']].astype(int)
                ind[par][parents['id'].values] = parents[par + '_decla'].values
                print str(sum((ind[par].notnull() & (ind[par] != -1 )))) + " enfants connaissent leur " + par
                return ind
        ind = _enf_to_par(ind)        
        self.ind = ind.drop(list_enf + ['index'],axis = 1)
        
    def constitution_menage(self):
        ind = self.ind
        ind['quimen'] = -1
        ind['men'] = -1
        ind['age'] = year_ini - ind['naiss']

        # 1ere étape : Détermination des têtes de ménages
        
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
        self.ind = ind.fillna(-1)
        self.men = men
    
    def add_change(self):
        print "Début de l'actualisation des changements jusqu'en 2060"
        ind = self.ind
        self.ind_ini = ind
        ind_add = self.ind_add
        
        # On ajoute ces données aux informations de 2009
        ind = ind.append(ind_add, ignore_index = True)
        ind = ind.replace(-1, np.nan)
        
        # On sort la table au format minimal
        ind = minimal_dtype(ind)
        ind = ind.fillna(-1)
        self.ind = ind
        ind.to_csv('indfinal.csv')
        print "Fin de l'actualisation des changements jusqu'en 2060"
    
if __name__ == '__main__':
    
    data = Destinie()
    start_t = time.time()
    # Importation des données et corrections préliminaires
    data.load()
    data.format_initial()
    data.conjoint()
#     data.check_conjoint()
    data.constitution_menage()
    data.table_initial()
    data.creation_foy()    
    data.var_sup()
    data.add_change()
    
    data.store_to_liam()

    print "Temps Destiny.py : " + str(time.time() - start_t) + "s" 