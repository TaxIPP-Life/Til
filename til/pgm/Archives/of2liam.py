# -*- coding:utf-8 -*-
'''
Created on 25 Apr 2013

@author: alexis_e
'''

from pandas import HDFStore, merge # DataFrame
import numpy as np
import pdb
import time
from src.lib.simulation import SurveySimulation 
from src.parametres.paramData import XmlReader, Tree2Object
import pandas as pd 
import datetime as dt   
import pandas.rpy.common as com     
from rpy2.robjects import r
import os
import tables



def main(period=None):
    temps = time.clock()   
    input_tab = "C:/openfisca/output/liam/"+"LiamLeg.h5"
    output_tab = "C:/Myliam2/Model/SimulTest.h5" 
    
    store = HDFStore(input_tab)
    goal = HDFStore(output_tab) 
    
    name_convertion = {'ind':'person','foy':'declar','men':'menage', 'fam':'menage'}
    # on travaille d'abord sur l'ensemble des tables puis on selectionne chaque annee
    # step 1
    
    for ent in ('ind','men','foy','fam'):
        dest = name_convertion[ent]
        tab_in = store[ent]
        tab_out = goal['entities/'+dest]
        #on jour sur les variable a garder 
        #TODO: remonter au niveau de of_on_liam mais la c'est pratique du fait de 
        #l'autre table
        ident = 'id'+ent
        if ent=='ind':
            ident='noi'
        # on garde les valeurs de depart
        to_remove = [x for x in tab_in.columns if x in tab_out.columns]
        #on retire les identifiant sauf celui qui deviendra id
        list_id = ['idmen','idfoy','idfam','id','quifoy','quifam','quimen','noi'] 
        list_id.remove(ident)
        to_remove = to_remove + [x for x in tab_in.columns if x in list_id]
        #on n4oublie pas de garder periode
        to_remove.remove('period')
        tab_in = tab_in.drop(to_remove,axis=1)
        tab_in = tab_in.rename(columns={ident:'id'})
        tab_out  = merge(tab_in, tab_out , how='right', on=['id','period'], sort=False)
        goal.remove('entities/'+dest)  
        goal.append('entities/'+dest, tab_out) 
#        new_tab = np.array(tab_out.to_records())

    store.close()
    goal.close()

    

#    output_file = tables.openFile(output_tab)
    
    
if __name__ == "__main__":
    main()