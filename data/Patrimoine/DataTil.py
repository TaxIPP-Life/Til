# -*- coding:utf-8 -*-
'''
Created on 22 juil. 2013
Alexis Eidelman
'''

#TODO: duppliquer la table avant le matching parent enfant pour ne pas se trimbaler les valeur de hod dans la duplication.

from matching import Matching
from utils import recode, index_repeated, replicate, new_link_with_men
from pgm.CONFIG import path_data_patr, path_til
import pandas as pd
import numpy as np
from pandas import merge, notnull, DataFrame, Series
from numpy.lib.stride_tricks import as_strided
import pdb
import gc
from utils import of_name_to_til


class DataTil(object):
    """
    La classe qui permet de lancer le travail sur les données
    La structure de classe n'est peut-être pas nécessaire pour l'instant 
    """
    def __init__(self):
        self.name = None
        self.survey_date = None
        self.ind = None
        self.men = None
        self.foy = None
        self.par_look_enf = None
        
        #TODO: Faire une fonction qui chexk où on en est, si les précédent on bien été fait, etc.
        self.done = []
        self.order = []
        
    def lecture(self):
        print "début de l'importation des données"
        raise NotImplementedError()
        print "fin de l'importation des données"

                
    def drop_variable(self, dict_to_drop=None, option='white'):
        '''
        - Si on dict_to_drop is not None, il doit avoir la forme table: [liste de variables],
        on retire alors les variable de la liste de la table nommée.
        - Sinon, on se sert de cette méthode pour faire la première épuration des données, on
         a deux options:
             - passer par la liste blanche ce que l'on recommande pour l'instant 
             - passer par  liste noire. 
        '''
        
        if 'ind' in dict_to_drop.keys():
            self.ind = self.ind.drop(dict_to_drop['ind'], axis=1)
        if 'men' in dict_to_drop.keys():
            self.men = self.men.drop(dict_to_drop['men'], axis=1)
        if 'foy' in dict_to_drop.keys():
            self.foy = self.foy.drop(dict_to_drop['foy'], axis=1)            
            
        
    def format_initial(self):
        raise NotImplementedError()
        
    def conjoint(self):
        '''
        Calcule l'identifiant du conjoint et vérifie que les conjoint sont bien reciproques 
        '''     
        print ("travail sur les conjoints")
        raise NotImplementedError()
        print ("fin du travail sur les conjoints")
        

    def enfants(self):   
        '''
        Calcule l'identifiant des parents 
        '''    
        raise NotImplementedError()


    def creation_foy(self):
        '''
        Créer les déclarations fiscale. Il s'agit principalement de regrouper certains individus entre eux.
        Ce n'est qu'ici qu'on s'occupe de verifier que les individus mariés ou pacsé ont le même statut matrimonial
        que leur partenaire légal. On ne peut pas le faire dès le début parce qu'on a besoin du numéro du conjoint.
        '''
        raise NotImplementedError()
        
    def creation_par_look_enf(self):
        '''
        Travail sur les liens parents-enfants. 
        On regarde d'abord les variables utiles pour le matching
        '''
        raise NotImplementedError()
        
    def matching_par_enf(self):
        '''
        Matching des parents et des enfants hors du domicile
        '''
        raise NotImplementedError()
    
    def match_couple_hdom(self):
        '''
        Certaines personnes se déclarent en couple avec quelqu'un ne vivant pas au domicile, on les reconstruit ici. 
        Cette étape peut s'assimiler à de la fermeture de l'échantillon.
        On séléctionne les individus qui se déclare en couple avec quelqu'un hors du domicile.
        On match mariés,pacsé d'un côté et sans contrat de l'autre. Dit autrement, si on ne trouve pas de partenaire à une personne mariée ou pacsé on change son statut de couple.
        Comme pour les liens parents-enfants, on néglige ici la possibilité que le conjoint soit hors champ (étrange, prison, casernes, etc).
        Calcul aussi la variable ind['nb_enf']
        '''
        raise NotImplementedError()
        
    def expand_data(self, seuil=150, nb_ligne=None):
        '''
        Note: ne doit pas tourner après lien parent_enfant
        '''
        if seuil!=0 and nb_ligne is not None:
            raise Exception("On ne peut pas à la fois avoir un nombre de ligne désiré et une valeur" \
            "qui va determiner le nombre de ligne")
        #TODO: on peut prendre le min des deux quand même...
        
        all = self.men.columns.tolist()
        enfants_hdom = [x for x in all if x[:3]=='hod']
        self.drop_variable({'men':enfants_hdom})
        
        men = self.men      
        ind = self.ind        
        foy = self.foy
        par = self.par_look_enf
        
        if par is None: 
            print("Notez qu'il est plus malin d'étendre l'échantillon après avoir fait les tables " \
            "par_look_enf plutôt que de les faire à partir des tables déjà étendue")
            
        if foy is None: 
            print("C'est en principe plus efficace d'étendre après la création de la table foyer" \
                  " mais si on veut rattacher les enfants (par exemple de 22 ans) qui ne vivent pas au" \
                  " domicile des parents sur leur déclaration, il faut faire l'extension et la " \
                  " fermeture de l'échantillon d'abord. Pareil pour les couples. ")
        
        min_pond = min(men['pond'])
        target_pond = max(min_pond, seuil)
    
        men['nb_rep'] = 1 + men['pond'].div(target_pond).astype(int)
        men['pond'] = men['pond'].div(men['nb_rep'])
        men_exp = replicate(men) 
       

        if foy is not None:
            foy = merge(men.ix[:,['id','nb_rep']],foy, left_on='id', right_on='men', how='right', suffixes=('_men',''))
            foy_exp= replicate(foy)
            foy_exp['men'] = new_link_with_men(foy, men_exp, 'men')
        else: 
            foy_exp = None

        if par is not None:
            par = merge(men.ix[:,['id','nb_rep']], par, left_on='id', right_on='men', how='right', suffixes=('_men',''))
            par_exp= replicate(par)
            par_exp['men'] = new_link_with_men(par, men_exp, 'men')         
        else: 
            par_exp = None
                        
                        
        ind = merge(men.ix[:,['id','nb_rep']],ind, left_on='id', right_on='men', how='right', suffixes = ('_men',''))
        ind_exp= replicate(ind)
                
        # liens entre individus
        ind_exp[['pere','id_rep']]
        tableA = ind_exp[['pere','mere','conj','id_rep']].reset_index()
        tableB = ind_exp[['id_rep','id_ini']]
        tableB['id_index'] = tableB.index
        ind_exp = ind_exp.drop(['pere', 'mere','conj'], axis=1)
        print("debut travail sur identifiant")
        pere = tableA.merge(tableB,left_on=['pere','id_rep'], right_on=['id_ini','id_rep'], how='inner').set_index('index')
        pere = pere.drop(['pere','mere','conj','id_ini','id_rep'], axis=1).rename(columns={'id_index':'pere'})
        ind_exp = ind_exp.merge(pere, left_index=True,right_index=True, how='left', copy=False) 
        
        mere = tableA.merge(tableB,left_on=['mere','id_rep'], right_on=['id_ini','id_rep'], how='inner').set_index('index')
        mere = mere.drop(['pere','mere','conj','id_ini','id_rep'], axis=1).rename(columns={'id_index':'mere'})
        ind_exp = ind_exp.merge(mere, left_index=True,right_index=True, how='left', copy=False) 
        
        conj = tableA.merge(tableB,left_on=['conj','id_rep'], right_on=['id_ini','id_rep'], how='inner').set_index('index')
        conj = conj.drop(['pere','mere','conj','id_ini','id_rep'], axis=1).rename(columns={'id_index':'conj'})
        ind_exp = ind_exp.merge(conj, left_index=True,right_index=True, how='left', copy=False) 
        print("fin travail sur identifiant")
        
        # lien indiv - entités supérieures
        ind_exp['men'] = new_link_with_men(ind, men_exp, 'men')  
        if foy is not None:
            #le plus simple est de repartir des quifoy, cela change du men
            # la vérité c'est que ça ne marche pas avec ind_exp['foy'] = new_link_with_men(ind, foy_exp, 'foy')
            vous = (ind['quifoy'] == 0)
            conj = (ind['quifoy'] == 1)
            pac = (ind['quifoy'] == 2)
            ind.loc[vous,'foy']= range(sum(vous))
            ind.loc[conj,'foy'] = ind.ix[ind['conj'][conj],['foy']]
            pac_pere = pac & notnull(ind['pere'])
            ind.loc[pac_pere,'foy'] = ind.loc[ind.loc[pac_pere,'pere'],['foy']]       
            pac_mere = pac & ~notnull(ind['foy'])
            ind.loc[pac_mere,'foy'] = ind.loc[ind.loc[pac_mere,'mere'],['foy']]  
            
        self.par_look_enf = par
        self.men = men_exp
        self.ind = ind_exp
        self.foy = foy_exp
        self.drop_variable({'men':['id_rep','nb_rep','index'], 'ind':['id_rep','id_men',]})    
        
    def mise_au_format(self):
        '''
        On met ici les variables avec les bons codes pour achever le travail de DataTil
        On crée aussi les variables utiles pour la simulation
        '''
        men = self.men      
        ind = self.ind 

        ind['quimen'] = ind['lienpref']
        ind['quimen'][ind['quimen'] >1 ] = 2
        ind['age'] = self.survey_date/100 - ind['anais']
        ind['agem'] = 12*ind['age'] + 11 - ind['mnais']
        ind['period'] = self.survey_date
        men['period'] = self.survey_date
        # a changer avec values quand le probleme d'identifiant et résolu .values
        men['pref'] = ind.ix[ ind['lienpref']==0,'id'].values
        
        self.men = men
        self.ind = ind
        self.drop_variable({'ind':['lienpref','age','anais','mnais']})  
            
    def store_to_liam(self):
        import tables
        path = path_til +'model\\' + self.name + '.h5' # + syrvey_date
        h5file = tables.openFile( path, mode="w")
        ent_node = h5file.createGroup("/", "entities", "Entities")
        
        for ent_name in ['ind','foy','men']:
            entity = eval('self.'+ent_name)
            
            ent_table = entity.to_records(index=False)
            dtypes = ent_table.dtype                
            try:
                table = h5file.createTable(ent_node, of_name_to_til[ent_name], dtypes, title="%s table" % ent_name)         
            except:
                pdb.set_trace()
            table.append(entity.to_records(index=False))
            table.flush()    


    def store(self):
        self.men.to_hdf(path_til + 'model\\patrimoine.h5', 'entites/men')
        self.ind.to_hdf(path_til + 'model\\patrimoine.h5', 'entites/ind')
        self.foy.to_hdf(path_til + 'model\\patrimoine.h5', 'entites/foy')
