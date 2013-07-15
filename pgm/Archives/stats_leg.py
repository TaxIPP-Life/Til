# -*- coding:utf-8 -*-

'''
Created on 2 May 2013

@author: alexis_e
'''


from pandas import HDFStore, merge # DataFrame
import numpy as np
import pdb
import time
import pandas as pd

output = "C:/openfisca/output/liam/"
get_years =  HDFStore("C:/openfisca/src/countries/france/data/surveyLiam.h5")
input_h5  = HDFStore(output+"LiamLeg.h5")
output_h5 = HDFStore(output+"LiamLeg2.h5")

years = [x[-4:] for x in dir(get_years.root) if x[0]!='_' ]
nb_year = len(years)
get_years.close()

ent ='ind'
list_tab = [ 'survey_'+x+'/'+ent for x in years ]

output_h5['ind']=pd.DataFrame()

for ent in ('ind','men','foy','fam'):
    output_h5[ent]=pd.DataFrame()
    for year in years: 
        name_tab = 'survey_'+year+'/'+ent 
        tab = input_h5[name_tab]
        tab['period'] = pd.Series(np.ones(len(tab)) * int(year))
        output_h5.put(ent, output_h5[ent].append(tab) )




# par entité, lire les tables pour chaque année, ajouter périod, et ajouter tout ça