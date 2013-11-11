# -*- coding:utf-8 -*-
'''
Created on 22 juil. 2013
Alexis Eidelman
'''

#TODO: duppliquer la table avant le matching parent enfant pour ne pas se trimbaler les valeur de hod dans la duplication.

from matching import Matching
from utils import recode, index_repeated, replicate, new_link_with_men, of_name_to_til, minimal_dtype
from pgm.CONFIG import path_data_patr, path_til, path_til_liam

import pandas as pd
import numpy as np
import tables

from pandas import merge, notnull, DataFrame, Series
from numpy.lib.stride_tricks import as_strided

import pdb
import gc

import sys 
sys.path.append(path_til_liam)
import src_liam.importer as imp


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
        self.futur = None
        self.past = None
        self.par_look_enf = None
        self.seuil= None
        
        #TODO: Faire une fonction qui chexk où on en est, si les précédent on bien été fait, etc.
        self.done = []
        self.order = []
        
    def load(self):
        print "début de l'importation des données"
        raise NotImplementedError()
        print "fin de l'importation des données"

    #def rename_var(self, [pe1e, me1e]):
        # TODO : fonction qui renomme les variables pour qu'elles soient au format liam
        # period, id, agem, age, sexe, men, quimen, foy quifoy pere, mere, conj, dur_in_couple, civilstate, workstate, sali, findet
        
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
        
    def check_conjoint(self):
        '''
        Vérifications de la réciprocité des conjoints déclarés + des états civils
        Correction si nécessaires
        '''     
        ind = self.ind
        start_year = self.survey_year
        final_year = self.survey_year +1        
        print ("Début du travail sur les conjoints")

        for year in xrange(start_year, final_year): 
            # TODO: faire une fonction qui s'adapte à ind pour check si les unions/désunions sont bien signifiées 
            # pour les deux personnses concernées
        #1 -Vérifie que les conjoints sont bien reciproques 
            tab = ind[ind['period'] == year].reset_index()
#            tab.to_csv('testtab0.csv')
            test = tab.loc[(tab['conj'] != -1) | tab['civilstate'].isin([2,5]),['id','conj','civilstate']]
            test = merge(test,test,left_on='id', right_on='conj', how='outer').fillna(-1)
            try: 
                assert((test['conj_x'] == test['id_y']).all())
            except :
                test = test[test['conj_x'] != test['id_y']]
                print "Le nombre d'époux non réciproques pour l'année" + str(year) + " est : " + str(len(test)) 
                # (a) - l'un des deux se déclare célibataire -> le second le devient
                celib_y = test.loc[test['civilstate_x'].isin([2,5]) & ~test['civilstate_y'].isin([2,5,-1]) & (test['id_x']< test['conj_x']),
                                            ['id_x', 'civilstate_y']]
                if celib_y:
                    tab['civilstate'][celib_y['id_x'].values]= celib_y['civilstate_y']
                    tab['conj'][celib_y['id_x'].values] = np.nan

                celib_x = test.loc[test['civilstate_y'].isin([2,5]) & ~test['civilstate_x'].isin([2,5,-1]) & (test['id_x']< test['conj_x']), 
                                            ['id_y','civilstate_x']]
                if len(celib_x) != 0:
                    tab['civilstate'][celib_x['id_y'].values]= celib_x['civilstate_x']
                    tab['conj'][celib_x['id_y'].values] = np.nan

                # (b) - les deux se déclarent mariés mais conjoint non spécifié dans un des deux cas
                # -> Conjoint réattribué à qui de droit
                no_conj = test[test['civilstate_x'].isin([2,5]) & test['civilstate_y'].isin([2,5]) & (test['conj_x']==-1)][['id_y', 'id_x']]
                if no_conj:
                    tab['conj'][no_conj['id_x'].values] = no_conj['id_y'].values
                
                # (c) - Célibats lorsque conjoint non spécifié ni non retrouvé
                no_conj = (tab['civilstate'].isin([2,5]) | (tab['civilstate'] == -1))  & (tab['conj'] == -1)
                print str(len(tab[no_conj])) + " personnes ne spécifiant pas de conjoint ou d'état civil deviennent célibataires"
                tab.loc[no_conj, 'civilstate'] = 1
            
        #3 - Vérifications des états civils
            test = tab.loc[tab['civilstate'].isin([2,5]), ['conj','id','civilstate', 'sexe']]
            test = merge(test, test, left_on='id', right_on='conj')
            confusion = test['civilstate_x'].isin([2,5]) & test['civilstate_y'].isin([2,5]) & (test['civilstate_y']!= test['civilstate_x'])
            test = test.loc[confusion & (test['id_x'] < test['id_y']),
                            ['id_x', 'id_y', 'civilstate_x', 'civilstate_y']]
            if test:
                print str(len(test)) + " confusions mariages/pacs en " + str(year) + " corrigées"
                # Hypothese: Celui ayant l'identifiant le plus petit dit vrai
                tab['civilstate'][test['id_y'].values] = tab['civilstate'][test['id_x'].values]
            ind[ind['period'] == year] = tab
        self.ind = ind
        print ("fin du travail sur les conjoints")

    def enfants(self):   
        '''
        Calcule l'identifiant des parents 
        '''    
        raise NotImplementedError()
    
    def table_initial(self):
        raise NotImplementedError()
        
    def creation_foy(self):
        '''
        Créer les déclarations fiscale. Il s'agit principalement de regrouper certains individus entre eux.
        Ce n'est qu'ici qu'on s'occupe de verifier que les individus mariés ou pacsé ont le même statut matrimonial
        que leur partenaire légal. On ne peut pas le faire dès le début parce qu'on a besoin du numéro du conjoint.
        '''
        ind = self.ind 
        men = self.men      
        print ("creation des declaration")
        # 0eme étape : création de la variable 'nb_enf' si elle n'existe pas
        if 'nb_enf' not in list(ind.columns):
            nb_enf_mere= ind.groupby('mere').size()
            nb_enf_pere = ind.groupby('pere').size()
            enf_tot = pd.concat([nb_enf_mere, nb_enf_pere], axis=1)
            enf_tot = enf_tot.sum(axis=1)
            # Comme enf_tot a le bon index on fait
            ind['nb_enf'] = enf_tot
            ind['nb_enf'] = ind['nb_enf'].fillna(0)
            
        # 1ere étape : Identification des personnes en couple
        spouse = (ind['conj'] != -1) & ind['civilstate'].isin([2,5]) 
        print str(sum(spouse)) + " personnes en couples"
        
        # 2eme étape : rôles au sein du foyer fiscal
        # selection du conjoint qui va être le vousrant : pas d'incidence en théorie
        decl = spouse & ( ind['conj'] > ind['id'])
        conj = spouse & ~decl
        # Identification des personnes à charge (moins de 21 ans sauf si étudiant, moins de 25 ans )
        # attention, on ne peut être à charge que si on n'est pas soi-même parent
        pac_condition = (ind['civilstate']==1) & (ind['nb_enf']==0) & ( ((ind['age'] <25) & (ind['workstate']==11)) | (ind['age']<21) ) & (ind['men'] != -4) 
        pac = ((ind['pere'] != -1) | (ind['mere'] != -1)) & pac_condition
        print str(sum(pac)) + ' personnes prises en charge'
        # Identifiants associés
        ind['quifoy'] = 0
        ind['quifoy'][conj] = 1
        ind['quifoy'][pac] = 2
        ind['quifoy'][ind['men'] == -4] = -4
        

        # 3eme étape : attribution des identifiants des foyers fiscaux
        ind['foy'] = -1
        ind['foy'][ind['men'] == -4] = -4
        nb_foy = len(ind[ind['quifoy'] == 0]) 
        print "Le nombre de foyers créés est : " + str(nb_foy)
        ind['foy'][ind['quifoy'] == 0] = range(0, nb_foy)
        
        # 3eme étape : Rattachement des autres membres du ménage
        # (a) - Rattachements des conjoints des personnes en couples 
        conj = ind.loc[(ind['conj'] != -1) & (ind['quifoy'] == 0), ['conj','foy']].astype(int)
        ind['foy'][conj['conj'].values] = conj['foy'].values
        
        # (b) - Rattachements de leurs enfants (en priorité sur la décla du père)
        for parent in  ['pere', 'mere']:
            pac_par = ind.loc[ pac_condition & (ind[parent] != -1) & (ind['foy'] == -1), parent]
            ind['foy'][pac_par.index.values] = ind['foy'][pac_par.values]
            print str(len(pac_par)) + " enfants sur la déclaration de leur " + parent

        # 4eme étape : création de la table foy
        vous = (ind['quifoy'] == 0)
        foy = DataFrame({'id':range(nb_foy), 'vous': ind['id'][vous], 'men':ind['men'][vous] })
        var_to_declar = ['zcsgcrds','zfoncier','zimpot', 'zpenaliv','zpenalir','zpsocm','zrevfin','pond']
        foy_men = men[var_to_declar]
        # hypothèse réparartition des élements à égalité entre les déclarations : discutable
        nb_foy_men = ind[vous].groupby('men').size()
        foy_men = foy_men.div(nb_foy_men,axis=0) 
        
        foy = merge(foy,foy_men, left_on='men', right_index=True)
        foy['vous'] = ind['id'][vous]
        foy = DataFrame(foy, index = xrange(0,len(foy)))
        foy['id'] = foy.index
        for table in [men, foy, ind]:
            table['period'] = self.survey_date
            
        #### fin de declar
        self.ind = ind
        self.foy = foy
        print("fin de la creation des declarations")
        
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
        #TODO: add future and past
        '''
        Note: ne doit pas tourner après lien parent_enfant
        Cependant par_look_enfant doit déjà avoir été créé car on s'en sert pour la réplication
        '''
        self.seuil = seuil
        if seuil != 0 and nb_ligne is not None:
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
        target_pond = float(max(min_pond, seuil))

        # 1 - Réhaussement des pondérations inférieures à la pondération cible
        men['pond'] [men ['pond']<target_pond] = target_pond 
        # 2 - Calcul du nombre de réplications à effectuer
        men['nb_rep'] = men['pond'].div(target_pond)
        men['nb_rep'] = men['nb_rep'].round()
        men['nb_rep'] = men['nb_rep'].astype(int)

        # 3- Nouvelles pondérations (qui seront celles associées aux individus après réplication)
        men['pond'] = men['pond'].div(men['nb_rep'])
        men_exp = replicate(men) 
       
        if foy is not None:
            foy = merge(men[['id','nb_rep']],foy, left_on='id', right_on='men', how='right', suffixes=('_men',''))
            foy_exp= replicate(foy)
            foy_exp['men'] = new_link_with_men(foy, men_exp, 'men')
        else: 
            foy_exp = None

        if par is not None:
            par = merge(men[['id','nb_rep']], par, left_on='id', right_on='men', how='right', suffixes=('_men',''))
            par_exp = replicate(par)
            par_exp['men'] = new_link_with_men(par, men_exp, 'men')         
        else: 
            par_exp = None
                        
        ind = merge(men[['id','nb_rep']], ind, left_on='id', right_on='men', how='right', suffixes = ('_men',''))
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
        
        ind = ind.fillna(-1)
        
        #TODO: comprendre pourquoi le type n'est pas bon plus haut, et le changer le plus tôt possible
        #souvent c'est à cause des NA
#         var_to_int = ['anc','conj','findet','foy','mere','pere','workstate','xpr']
#         var_to_float = ['choi','rsti','sali']
#         ind[var_to_int] = ind[var_to_int].astype(int)
#         ind[var_to_float] = ind[var_to_float].astype(float)
        
        self.men = minimal_dtype(men)
        self.ind = minimal_dtype(ind)
        self.drop_variable({'ind':['lienpref','age','anais','mnais']})  
        
    def var_sup(self):
        '''
        Création des variables indiquant le nombre de personnes dans le ménage et dans le foyer
        '''   
        ind = self.ind        
        #ind['nb_men'] = ind.groupby('men').size()
        #ind['nb_foy'] = ind.groupby('foy').size()
        
        ind_men = ind.groupby('men')       
        ind = ind.set_index('men')
        ind['nb_men'] = ind_men.size() 
        ind=ind.reset_index()

        ind_foy = ind.groupby('foy')
        ind = ind.set_index('foy')
        ind['nb_foy'] = ind_foy.size() 
        ind=ind.reset_index()
        self.ind=ind 
        
        
    def store_to_liam(self):
        '''
        Sauvegarde des données au format utilisé ensuite par le modèle Til
        Appelle des fonctions de Liam2
        Le mieux serait que Liam2 puisse tourner sur un h5 en entrée
        '''
        if self.seuil is None:
            path = path_til +'model\\' + self.name + '_' + '0' +'.h5' # + survey_date
        else: 
            path = path_til +'model\\' + self.name + '_' + str(self.seuil) +'.h5' # + survey_date
        h5file = tables.openFile( path, mode="w")
        # 1 - on met d'abord les global en recopiant le code de liam2
        globals_def = {'periodic': {'path': 'param\\globals.csv'}}

        const_node = h5file.createGroup("/", "globals", "Globals")
        localdir = path_til + '\\model'
        for global_name, global_def in globals_def.iteritems():
            print()
            print(" %s" % global_name)
            req_fields = ([('PERIOD', int)] if global_name == 'periodic'
                                            else [])
            kind, info = imp.load_def(localdir, global_name,
                                  global_def, req_fields)
            # comme dans import
#             if kind == 'ndarray':
#                 imp.array_to_disk_array(h5file, const_node, global_name, info,
#                                     title=global_name,
#                                     compression=compression)
#             else:
            assert kind == 'table'
            fields, numlines, datastream, csvfile = info
            imp.stream_to_table(h5file, const_node, global_name, fields,
                            datastream, numlines,
                            title="%s table" % global_name,
                            buffersize=10 * 2 ** 20,
                            compression=None)
        
        # 2 - ensuite on s'occupe des entities
        ent_node = h5file.createGroup("/", "entities", "Entities")
        for ent_name in ['ind','foy','men']:
            entity = eval('self.'+ ent_name)
            entity = entity.fillna(-1)
            ent_table = entity.to_records(index=False)
            dtypes = ent_table.dtype   
            print dtypes      
            table = h5file.createTable(ent_node, of_name_to_til[ent_name], dtypes, title="%s table" % ent_name)         
            table.append(ent_table)
            table.flush()    
            
            if ent_name == 'men':
                ent_table2 = entity[['pond','id','period']].to_records(index=False)
                dtypes2 = ent_table2.dtype 
                table = h5file.createTable(ent_node, 'companies', dtypes2, title="%s table" % ent_name)
                table.append(ent_table2)
                table.flush()  
            if ent_name == 'ind':
                ent_table2 = entity[['agem','sexe','pere','mere','id','findet','period']].to_records(index=False)
                dtypes2 = ent_table2.dtype 
                table = h5file.createTable(ent_node, 'register', dtypes2, title="%s table" % ent_name)
                table.append(ent_table2)
                table.flush()  
        h5file.close()
            
    def store(self):
        self.men.to_hdf(path_til + 'model\\patrimoine.h5', 'entites/men')
        self.ind.to_hdf(path_til + 'model\\patrimoine.h5', 'entites/ind')
        self.foy.to_hdf(path_til + 'model\\patrimoine.h5', 'entites/foy')

    def run_all(self):
        for method in self.methods_order:
            eval('self.'+ method + '()')        
            