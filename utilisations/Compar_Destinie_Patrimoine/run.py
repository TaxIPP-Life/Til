# -*- coding:utf-8 -*-
''' 
Ce programme compare la base Destinie et la base générée directement à partir de l'enquête patrimoine.

Les tables issues de Destinie et de Patrimoine doivent déjà avoir été créées 
    - data//data//Destinie//Patrimoine.py et data//data//Destinie//Destinie.py
    Pour Patrimoine, ça ne sert à rien de duppliquer pour comparer donc on part de la version Patrimoine_0
'''

import pdb

from pgm.CONFIG import path_til
from pandas import read_hdf, HDFStore
import tables
import numpy as np
from scipy.stats import mstats

path_data = path_til + 'Model\\'

# retourne une ficher stat qui permet la comparaison entre deux tables
# d'abord on récupéère les stat de chaque table, 
# ensuite on fait la différence. 

list_var_num = ['agem']
list_var_qual = ['civilstate']

pat = read_hdf(path_data + 'Patrimoine_300.h5', 'entities//person')
dest = read_hdf(path_data + 'Destinie.h5', 'entities//person')
table = pat

table1 = 'Patrimoine_300'
table2 = 'Destinie'

def quantile10(x):    return x.quantile(0.1)
def quantile25(x):    return x.quantile(0.25)
def quantile50(x):    return x.quantile(0.5)
def quantile75(x):    return x.quantile(0.75)

stat_on_numeric = [np.mean, np.std, np.min, np.max, quantile10, quantile25, quantile50, quantile75] #I don't know how to add some lambda function

class Comparaison_bases(object):
    '''
    Organisation de la comparaison des deux bases de données 
    Les deux bases ont des rôles symétriques, on présente toujours la différence de la seconde moins la premiere
    '''
    def __init__(self, table1='', table2=''):
        self.name_tables = [table1, table2]
        self.tables = None
        self.stat_on_numeric = [np.mean, np.std, np.min, np.max, quantile10, quantile25, quantile50, quantile75]
  
    def load(self):
        self.tables = []
        for name_table in self.name_tables:
            table = read_hdf(path_data + name_table + '.h5', 'entities//person')
            table['Total'] = 'Total'
            self.tables += [table]
                
    def get_stat_num(self, list_var_num, sous_cat=None):
        ''' retourne les stats sur des variables numérique (ou pseudo numériques)
            - les variables sont celle de list_var_num
            - les stats sont celle de la fonction get_stat_on_numeric (que l'on pourrait mettre en attribut de la class)
            - le résultats est une liste de deux series, une pour chaque table (on a une liste parce qu'on veut garder un ordre pour la différence
                - le dictionnaire de chaque table a une entrée par variable et comme valeur le resultat de tous les tests
        '''        
        if sous_cat is None:
            sous_cat = ['Total']

        tab0, tab1 = self.tables

        return [tab0[list_var_num + sous_cat].groupby(sous_cat).agg(stat_on_numeric),
                 tab1[list_var_num + sous_cat].groupby(sous_cat).agg(stat_on_numeric)]
       
    def get_stat_qual(self, list_var_qual, sous_cat=None):
        ''' retourne les stats sur des variables qualitative, en fait la fréquence
            - les variables sont celle de list_var_qual
            - le résultats est un dictionnaire avec une clé pour chaque variable et comme valeur une liste
            de deux tables qui sont les fréquences de la variable.
        '''        
        output = {}
        if sous_cat is None:
            sous_cat = ['Total']

        tab0, tab1 = self.tables  
        gp0 = tab0[list_var_qual+ sous_cat].groupby(sous_cat)
        gp1 = tab1[list_var_qual+ sous_cat].groupby(sous_cat)
        for var_qual in list_var_qual:         
            value0 = gp0[var_qual].value_counts()
            value0 = 100*value0/value0.sum()
            value1 = gp1[var_qual].value_counts()
            value1 = 100*value1/value1.sum()
            output[var_qual] = [value0, value1]
        return output
    
    def get_diff_num(self, list_var_num, sous_cat=None):
        ''' sort dans un dictionnaire la différence entre les statistiques sur les deux tables'''
        stat0, stat1 = self.get_stat_num(list_var_num, sous_cat)
        return stat1 - stat0       
        
    def get_diff_qual(self, list_var_qual, sous_cat=None):
        ''' sort dans un dictionnaire la différence entre les statistiques sur les deux tables'''
        stat = self.get_stat_qual(list_var_qual, sous_cat)
        output = {}
        for var_qual in stat.keys():
            output[var_qual] =  stat[var_qual][1] - stat[var_qual][0]
        return output          

 
            
if __name__ == '__main__':
    cc = Comparaison_bases('Patrimoine_300','Destinie')
    cc.load()
    tab0, tab1 = cc.tables
    print cc.get_diff_num(list_var_num, ['sexe'])
    print cc.get_diff_qual(list_var_qual, ['sexe'])
    self = cc
    pdb.set_trace()
