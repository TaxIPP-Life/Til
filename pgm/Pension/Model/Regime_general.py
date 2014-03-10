# -*- coding:utf-8 -*-

#import pandas as pd
#import numpy as np
from pgm.CONFIG import path_data_destinie
from datetime import datetime
from pgm.Pension.SimulPension import PensionSimulation

class Regime_general(PensionSimulation):
    
    def __init__(self, param_regime, param_common):
        PensionSimulation.__init__(self)
        self.regime = 'RG'
        self.ret_base = None
        self.ret_comp = None
        self._P = param_regime
        self._Pcom = param_common
        self.workstate  = None
        self.sali = None

    def _nb_trim(self):
        ''' Nombre de trimestres côtisés pour le régime général 
        ref : code de la sécurité sociale, article R351-9'''
        workstate = self.workstate
        sali = self.sali
        datesim = self.datesim 
        print datesim
        

    ######
    # Déterminations des variables d'input de la simulation
    ####

    def nb_trim_maj(self,P):
        ''' Code sécu : article R. 351-9 '''
        
        print P
            
    def MDA(self, data):
        MDA_naiss = self.valparam['mda_naiss']
        MDA_educ = self.valparam['mda_educ']
        nb_enf = data['nb_enf']
        nb_educ = data['nb_enf_educ']
        if self._date < datetime.strptime("1974-06-01","%Y-%m-%d").date():
            nb_enf = nb_enf.replace(1,0)
            mda = MDA_naiss *nb_enf
        else:
            mda = MDA_naiss*nb_enf + MDA_educ*nb_educ
        return mda
            
    
    def calculate_CP(self, trim_cot, age):
        ''' Calcul du coefficient de proratisation '''
        N_CP =  self.valparam['N_CP']
        date = self._date
        if datetime.strptime("1948-01-01","%Y-%m-%d").date() <= date:
            trim_cot = trim_cot + (120 - trim_cot)/2
        if datetime.strptime("1983-01-01","%Y-%m-%d").date() <= date:
            trim_cot = np.minimum(N_CP, trim_cot* (1 + np.maximum(0, age/ 3 - 260)))
            
        print len(trim_cot / N_CP)
        CP = np.minimum(1, trim_cot / N_CP)
        print len(CP)
        return CP