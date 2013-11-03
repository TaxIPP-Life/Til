# -*- coding:utf-8 -*-
'''
Created on 22 juil. 2013
Alexis Eidelman
'''

#TODO: duppliquer la table avant le matching parent enfant pour ne pas se trimbaler les valeur de hod dans la duplication.

from matching import Matching
from utils import recode, index_repeated, replicate, new_link_with_men, of_name_to_til
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

    
    def correction_civilstate(self):
        '''
        verification que les états civils des deux membres d'un couple correspondent
        et corrections quand ce n'est pas le cas
        '''
        BioFam = self.BioFam
        BioFam = BioFam.fillna(-1)
        # Corrections préliminaires
        BioFam.loc[ (BioFam['civilstate'].isin([2,5]) | (BioFam['civilstate'] == -1))  & (BioFam['conj'] == -1), 'civilstate'] = 1
        for year in xrange(self.survey_year, self.survey_year): 
            # Rq : A terme, faire une fonction qui s'adapte à BioFam pour check si les unions/désunions sont signifiés 
            # pour les deux personnses concernées
            corr = BioFam[BioFam['period'] == year]
            # Réciprocité des déclarations
            test_spouse = corr.ix[(corr['civilstate'].isin([2,5])),['conj','id','civilstate']]
            test_spouse = merge(test_spouse,test_spouse,
                                 left_on='id',right_on='conj',how='outer')
            prob_spouse = test_spouse['id_x'] != test_spouse['conj_y']
            if sum(prob_spouse) != 0 :
                print "Le nombre d'époux non réciproques est : " + str(sum(prob_spouse)) + " en " + str(year) + " mais on corrige"
                
            
            # Confusion pacs/marriage
            test = corr.ix[(corr['conj'] != -1) & (corr['conj']>corr['id']), ['conj','id','civilstate']]
            test2 = merge(test, test, left_on='id', right_on='conj', suffixes=('','_conj'))
            test2 = test2[(test2['civilstate']==2) & (test2['civilstate_conj']==5)]['conj', 'id']
            if sum(test2) != 0:
                print str(sum((test2['civilstate']==2) & (test2['civilstate_conj']==5))) + " confusions mariages/pacs en " + str(year) + " mais on corrige"
                # Hypothese: Si un des deux dit mariés ou pacsés alors les deux le sont 
                coor['civilstate'][test2['conj'].values] = coor['civilstate'][test2['id'].values]
            BioFam[BioFam['period'] == year] = corr
        self.BioFam = BioFam
        
    def creation_foy(self):
        '''
        Créer les déclarations fiscale. Il s'agit principalement de regrouper certains individus entre eux.
        Ce n'est qu'ici qu'on s'occupe de verifier que les individus mariés ou pacsé ont le même statut matrimonial
        que leur partenaire légal. On ne peut pas le faire dès le début parce qu'on a besoin du numéro du conjoint.
        '''
        ind = self.ind             
        ind[['conj', 'pere']] = ind[['conj', 'pere']].fillna(-1).astype(int)
        ind.to_csv('tetstt.csv')
        print ("creation des declaration")

        # Identification des personnes en couple
        spouse = (ind['conj'] != -1) & ind['civilstate'].isin([2,5]) 
        print len(spouse)
        # selection du conjoint qui va être le declarant : pas d'incidence en théorie
        decl = spouse & ( ind['conj'] > ind['id'])
        conj = spouse & ~decl
        
        #Identification des personnes à charge (moins de 21 ans sauf si étudiant, moins de 25 ans )
        # attention, on ne peut être à charge que si on n'est pas soi-même parent
        pac = ((ind['pere'] != -1) | (ind['mere'] != -1)) & (ind['civilstate']==1) & (ind['nb_enf']==0) & ( ((ind['age'] <25) & (ind['workstate']==11)) |  (ind['age']<21) ) 
        print len(pac)
        # idendifiant foyer, d'abord les déclarants puis rattachement des autres personnes
        ind['quifoy'] = 0
        ind['quifoy'][conj] = 1
        ind['quifoy'][pac] = 2
        vous = (ind['quifoy'] == 0)
        print sum(vous)
        print sum(ind['quifoy'] == 1)
        ind['foy'] = -1
        ind.loc[vous,'foy']= range(sum(vous))
        ind.loc[conj,'foy'] = ind.ix[ind['conj'][conj],'foy']
        
        # Enfants à charge en priorité sur la décla du père
        pac_pere = pac & (ind['pere'] != -1)
        ind.loc[pac_pere,'foy'] = ind.loc[ind.loc[pac_pere,'pere'],'foy']

        pac_mere = pac & (ind['foy'] == -1)   
        ind.loc[pac_mere,'foy'] = ind.loc[ind.loc[pac_mere,'mere'],'foy'] 
        print sum(ind['foy']==-1)
        assert sum(ind['foy']==-1) == 0
        
        foy = DataFrame({'id':range(sum(vous)), 'vous': ind['id'][vous], 'men':ind['men'][vous] })
        #repartition des revenus du ménage par déclaration
        # var_to_declar = ['zcsgcrds','zfoncier','zimpot', 'zpenaliv','zpenalir','zpsocm','zrevfin','pond']
        # foy_men = men[var_to_declar]
        # hypothèse réparartition des élements à égalité entre les déclarations : discutable
        # nb_foy_men = ind[vous].groupby('men').size()
        # foy_men = foy_men.div(nb_foy_men,axis=0) 
        
        # foy = merge(foy,foy_men, left_on='men', right_index=True)
        foy['period'] = self.survey_date
        foy['vous'] = ind['id'][vous]
        foy = foy.reset_index(range(len(foy)))
        foy['id'] = foy.index

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
        '''
        Note: ne doit pas tourner après lien parent_enfant
        Cependant par_look_enfant doit déjà avoir été créé car on s'en sert pour la réplication
        '''
        self.seuil = seuil
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
        target_pond = float(max(min_pond, seuil))

        # 1 - Réhaussement des pondérations inférieures à la pondération cible
        men['pond'] [men ['pond']<target_pond] = target_pond 
        
        # 2 - Calcul du nombre de réplications à effectuer
        men['nb_rep'] = men['pond'].div(target_pond)
        men['nb_rep'] = men['nb_rep'].round()
        men['nb_rep'] = men['nb_rep'].astype(int)

                
        # 3- Nouvelles pondérations (qui seront celles associées aux individus après réplication)
        men['pond'] = men['pond'].div(men['nb_rep'])
        men.to_csv('testcsv2.csv', sep=';')
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
        
        ind = ind.fillna(-1)
        
        #TODO: comprendre pourquoi le type n'est pas bon plus haut, et le changer le plus tôt possible
        #souvent c'est à cause des NA
        var_to_int = ['anc','conj','findet','foy','mere','pere','workstate','xpr']
        var_to_float = ['choi','rsti','sali']
        ind[var_to_int] = ind[var_to_int].astype(int)
        ind[var_to_float] = ind[var_to_float].astype(float)
        ind = ind.rename(columns={'etamatri':'civilstate'})
        
        self.men = men
        self.ind = ind
        self.drop_variable({'ind':['lienpref','age','anais','mnais']})  
        
    def var_sup(self):
        '''
        Création des variables indiquant le nombre de personnes dans le ménage et dans le foyer
        '''   
        ind = self.ind        
        #ind['nb_men'] = ind.groupby('men').size()
        #ind['nb_foy'] = ind.groupby('foy').size()
        

        g = ind.groupby('men')       
        ind= ind.set_index('men')
        ind['nb_men'] = g.size() 
        ind=ind.reset_index()

        h=ind.groupby('foy')
        ind = ind.set_index('foy')
        ind['nb_foy'] = h.size() 
        ind=ind.reset_index()
        
        self.ind=ind 
        
        
    def store_to_liam(self):
        '''
        Sauvegarde des données au format utilisé ensuite par le modèle Til
        Appelle des fonctions de Liam2
        Le mieux serait que Liam2 puisse tourner sur un h5 en entrée
        '''
        
        path = path_til +'model\\' + self.name + '_' + str(self.seuil) +'.h5' # + syrvey_date
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
            entity = eval('self.'+ent_name)
            entity = entity.fillna(0)
            
            ent_table = entity.to_records(index=False)
            dtypes = ent_table.dtype         
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
            