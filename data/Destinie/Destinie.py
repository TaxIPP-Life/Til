# -*- coding:utf-8 -*-
'''
Created on 11 septembre 2013

Ce programme :
- 

Input : 
Output :

'''

# 1- Importation des classes/librairies/tables nécessaires à l'importation des données de l'enquête Patrimoine

# Recup de ce dont on a besoin dans Patrimoine

 
from path_config import path_data_patr, path_til, path_data_des
from DataTil import DataTil_d

import pandas as pd
import numpy as np
import pandas.rpy.common as com
import rpy2.rpy_classic as rpy

from pandas import merge, notnull, DataFrame, Series, HDFStore
from numpy.lib.stride_tricks import as_strided
import pdb
import gc

#data = path_data_des+'\\BiosDestinie.RData'
#bio = rpy.r.load(data)
#bio = com.convert_robj(data) 
#bio  = np.asarray(bio)
#bio = pd.DataFrame(bio)




class Destinie(DataTil_d):   
    

    def __init__(self):
        DataTil_d.__init__(self)
        self.name = 'Destinie'
        self.survey_date = 200901
         
        #TODO: Faire une fonction qui check où on en est, si les précédent on bien été fait, etc.
        #TODO: Dans la même veine, on devrait définir la suppression des variables en fonction des étapes à venir.
        self.done = []
        self.methods_order = ['lecture','drop_variable','format_initial','conjoint','enfants',
                      'creation_par_look_enf','expand_data','matching_par_enf','matching_couple_hdom'
                      'creation_foy','mise_au_format']

    def lecture(self):
        print "début de l'importation des données d'emploi"
    #BioEmp = pd.read_table(path_data_des+'BioEmp.txt')
        BioEmp = pd.read_csv(path_data_des+'BioEmp.csv') 
        Emp_mat = np.asarray(BioEmp)
    #BioEmp.to_hdf(path_data_des + 'Bio.h5','entities/emp')      
        print "fin de l'importation des données d'emploi"
        
        print "début de l'importation des données liens familiaux"
        BioFam = pd.read_csv(path_data_des+'BioFam.csv')
        Fam_mat = np.asarray(BioFam)
        print "fin de l'importation des données liens familiaux"
        
    def built(self):
        
        '''
    def store_to_liam(self):
        
        Sauvegarde des données au format utilisé ensuite par le modèle Til
        Appelle des fonctions de Liam2
        Le mieux serait que Liam2 puisse tourner sur un h5 en entrée
        
        
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
             print "début de l'importation des données d'emploi"
        BioEmp = pd.read_table(path_data_des+'BioEmp.txt')
        print BioEmp.to_csv('test.csv', sep=',')     
        print "fin de l'importation des données d'emploi"
    '''
        
des=Destinie() 
des.lecture()

col= list(xrange(105))
BioEmp = pd.read_csv(path_data_des+'BioEmp2.csv', names=col, header=None) 
BioFam = pd.read_csv(path_data_des+'BioFam.csv')
Fam_mat = np.asarray(BioFam)


BioEmp['index'] = BioEmp.index
BioEmp['id'] = BioEmp['index']/3
#BioEmp['test'] = BioEmp[1] -> on appelle les colonnes avec un format numérique du coup

BioEmp['nb_ligne'] = BioEmp['index'] - 3*BioEmp['id']

# Table ind : table avec toutes les info de bases sur les individus (appariement entre enfants et parents avec BioFam sur cette table) et car avec les infos sur carrières
ind = BioEmp[BioEmp['nb_ligne']==0]
car = BioEmp[BioEmp['nb_ligne']>0]
'''
car['index'] = car.index
car.to_csv('test1.csv')
car = car.set_index('id').stack().reset_index()
car1 = car['index','']
car = pd.melt(car, id_vars=['index'])
car = car.sort('id')
car = car.set_index('id') 
car = car.rename(columns={'variable': 'code_année'})
car.to_csv('test.csv')
'''
stat = BioEmp[BioEmp['nb_ligne']==1][col]
stat = stat.stack()
stat.columns=['id','année','statut']
stat['id'] = stat['id']/3
stat.to_csv('test.csv') # on obtient a peu près ce que l'on veut : col1 = identifiant, col2 = 


sal = BioEmp[BioEmp['nb_ligne']==2][col]
sal = sal.reset_index()


