# -*- coding:utf-8 -*-

import pandas as pd
from pension import Pension
from pgm.CONFIG import path_data_destinie
import datetime


class Regime_gene(Pension):
    
    def __init__(self, date_simul, paramFile):
        Pension.__init__(self, date_simul, paramFile)
        self.regime = 'RG'
        self.ret_base = 'None'
        self.ret_comp = 'None'
        
    def load_param(self): 
        Pension.load_param(self)
        print self.param.ret_base.RG
        self.ret_base = self.param.ret_base.RG
        #self.ret_comp = self.param.ret_comp.RG
        
        def _check_param_base(param):
            param_to_check = {'tx_plein' : param.tx_plein, 'age_min' : param.age_min, 'age_max' : param.age_max, 'decote' : param.dec}
            mult 
            for param in param_to_check:
                if type(param_to_check[param]) != float :
                    param_to_check[param] = 'Valeurs multiples'
            print "Paramètres de la législation pour", self.regime, ' : ', param_to_check
            
        _check_param_base(self.ret_base)