# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np
from pension import Pension, list_param
from pgm.CONFIG import path_data_destinie
from datetime import datetime


class Regime_gene(Pension):
    
    def __init__(self, date_simul, paramFile):
        Pension.__init__(self, date_simul, paramFile)
        self.regime = 'RG'
        self.ret_base = 'None'
        self.ret_comp = 'None'

    def load_param(self): 
        Pension.load_param(self)
        self.ret_base = self.param.ret_base.RG
        #self.ret_comp = self.param.ret_comp.RG
        del self.param
        
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