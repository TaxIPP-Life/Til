# -*- coding:utf-8 -*-
import pandas as pd
from pgm.CONFIG import path_data_destinie
#from Param.paramData import XmlReader

class Pension(object):
    """
    La classe qui permet de lancer le calcul des retraites
    """
    def __init__(self, date_simul, paramFile):
        self.regime = None
        self._date = date_simul
        self.paramFile = paramFile

    def load_param(self): 
        print "Début de l'importation des paramètres de la législation"
        read = XmlReader(self.paramFile, self._date)
        self.param = read.param
        
    def trim_cot(self):
        print "Calcul du nombre de trimestres côtisés"
        raise NotImplementedError()
    
    def sal_ref(self):
        ''' Calcul du salaire de référence '''
        raise NotImplementedError()
    
    def calculate_CP(trim_cot, N_CP):
        ''' Calcul du coefficient de proratisation '''
        CP = min(1, trim_cot/ N_CP)
        print CP

    def calculate_taux(self, taux_plein, decote, surcote, age, age_dec, age_min, N_tau, trim_cot):
        ''' Calcul générique du taux individuel avec décotes et surcotes éventuelles '''
        if self.leg<198301:
            trim_to_taux =0
        else:
            trim_to_taux = 1
        to_decote = max(0, min((age_dec - age)/3, (N_tau - trim_cot)*trim_to_taux))
        to_surcote =  max(0, min((age - age_min)/3, (trim_cot - N_tau) * trim_to_taux))
        taux = taux_plein* (1 - decote*to_decote + surcote*to_surcote)
        return taux
    
def calculate_taux_col(base, decote, surcote, trim_to_taux, taux_plein, age_dec, age_min, N_tau, age, trim_cot):
    ''' Calcul générique du taux individuel avec décotes et surcotes éventuelles '''
    data = np.array(base[['age', 'trim_cot']])
    trim = N_tau - data[1]
    to_decote = max(0, min((age_dec - data[0])/3, trim))
    to_surcote =  max(0, min((data[0] - age_min)/3, - trim))
    taux = taux_plein* (1 - decote*to_decote + surcote*to_surcote)
    return taux
#print calculate_taux(0.75, 0.012, 0.012, 730, 744, 720, 160,  164)
        
import numpy as np
M = np.transpose([[1,2,3],[4,5,6],[7,8,9],[10,11,12]])
print np.triu(M, 1)