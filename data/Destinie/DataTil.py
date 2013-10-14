# -*- coding:utf-8 -*-
'''
Created on 22 juil. 2013
Alexis Eidelman
'''

#TODO: duppliquer la table avant le matching parent enfant pour ne pas se trimbaler les valeur de hod dans la duplication.



from path_config import path_data_patr, path_til, path_til_liam
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


class DataTil_d(object):
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
        
    def lecture(self):
        print "début de l'importation des données"
        raise NotImplementedError()
        print "fin de l'importation des données"

  