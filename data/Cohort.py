# -*- coding:utf-8 -*-
'''
Created on 13 december 2013

@author: a.eidelman

Ce programme :
- 

Input : 
Output :

'''

# 1- Importation des classes/librairies/tables nécessaires à l'importation des données de l'enquête Patrimoine
 
 
from data.DataTil import DataTil, variables_til
from data.matching import Matching
from data.utils import recode, index_repeated, replicate, new_link_with_men, minimal_dtype
from pgm.CONFIG import path_data_patr, path_til

import pandas as pd
import numpy as np

from pandas import merge, notnull, DataFrame, Series
from numpy.lib.stride_tricks import as_strided

import pdb
import gc



class Cohort(DataTil):
    """
    La classe qui permet de lancer le travail sur les données
    La structure de classe n'est peut-être pas nécessaire pour l'instant 
    """
    def __init__(self, size=1000):
        DataTil.__init__(self)
        self.survey_date = 100*2009 + 1
        self.size = size
        self.name = 'cohort'


    def load(self):
        print "création de l'importation des données"
        size = self.size
        for name_table in variables_til:
            vars_int, vars_float = variables_til[name_table]
            vars = ['id','period','pond'] + vars_int + vars_float
            
            table = pd.DataFrame(index=range(size), columns=vars)
            for var in vars_int:
                table[var] = 0
            for var in vars_float:
                table[var] = table[var].astype(float)
                
            table['pond'] = 1.0
            table['period'] = self.survey_date
            table['id'] = range(size)
            self.__setattr__(name_table, table)
        print "fin de la créations des données"

    def _output_name(self):
        return 'Cohort_' + str(self.size) + '.h5'
            
    def imputations(self):
        #TODO: findet ? 
        self.ind['sexe'] = np.random.random_integers(1, 2, size = self.size)
        self.ind['civilstate'] = 1
        self.ind['workstate'] = 11
        
    def links(self):
        size = self.size
        rg = range(size)
        self.ind['men'] = rg
        self.ind['foy'] = rg
        self.ind[['pere','mere','conj']] = -1
        
        self.foy['vous'] = rg
        self.men['pref'] = rg

    
if __name__ == '__main__':
    import time
    start_t = time.time()
    data = Cohort(1000)
    data.load()
    data.imputations()
    data.links()
    data.format_to_liam()
    data.final_check()
    data.store_to_liam()
    
