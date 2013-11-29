# -*- coding:utf-8 -*-

import pandas as pd

from pension import Pension
from pgm.CONFIG import path_data_destinie

class Regime_gene(Pension):

    def __init__(self):
        Pension.__init__(self)
        self.regime = 'RG'
    
    def load_param(self): 
        print "Hello"
        

        
    