# -*- coding:utf-8 -*-
import pandas as pd
from pgm.CONFIG import path_data_destinie

class Pension(object):
    """
    La classe qui permet de lancer le calcul des retraites
    """
    def __init__(self):
        self.regime = None
        self.date = 2060
        self.leg = 201001

    def load_param(self):
        print "Début de l'importation des paramètres de la législation"
        raise NotImplementedError()
        print "Fin de l'importation des paramètres"
        
    def trim_cot(self):
        print "Calcul du nombre de trimestres côtisés"
        raise NotImplementedError()
    
    def sal_ref(self):
        ''' Calcul du salaire de référence '''
        raise NotImplementedError()
    
    def calculate_CP(self):
        ''' Calcul du coefficient de proratisation '''
        raise NotImplementedError()

    def calculate_taux(taux_plein, decote, surcote, age_dec, age_min, N_tau,
                        age, trim_cot):
        ''' Calcul générarique du taux individuel avec décotes et surcotes éventuelles '''
        to_decote = max(0, min((age_dec - age)/3, N_tau - trim_cot))
        to_surcote =  max(0, min((age - age_min)/3, trim_cot - N_tau))
        taux = taux_plein* (1 - decote*to_decote + surcote*to_surcote)
        return taux
    
    #print calculate_taux(0.75, 0.012, 0.012, 744, 720, 160, 730, 164)
        


        