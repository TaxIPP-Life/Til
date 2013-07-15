# -*- coding:utf-8 -*-
'''
Created on 25 Apr 2013

@author: alexis_e
'''

from pandas import HDFStore, merge # DataFrame
import numpy as np
import pdb
import time

import pandas as pd 
import datetime as dt   
import pandas.rpy.common as com     
from rpy2.robjects import r
import os



def main(simulation, period=None, output=".h5"):
    temps = time.clock()    
    output_tab = "C:/til/output/to_run_leg.h5"
    name_convertion = {'person':'ind','declar':'foy','menage':'men', 'fam':'fam'}


    # on travaille d'abord sur l'ensemble des tables puis on selectionne chaque annee
    # on étudie d'abord la table individu pour pouvoir séléctionner les identifiants
    # step 1
    table = {}
    entities = simulation.entities
    for entity in entities:
        nom = entity.name
        if nom == 'person':
            ent = name_convertion[nom]
            # convert from PyTables to Pandas
            table[ent] = pd.DataFrame(entity.array.columns)
            # rename variables to make them OF ones
            table['ind'] = table['ind'].rename(columns={
                        'men': 'idmen', 'foy': 'idfoy', 'id': 'noi', 'statmarit': 'civilstate'})

    # get years
    years = np.unique(table['ind']['period'].values/100)
    ent = 'ind'
    # création de variable
    
# useless since agem is in simu    
#     table[ent]['agem'] = 12 * table[ent]['age'] 
    
    table[ent]['ageq'] =  table[ent]['age']/5 - 4 
    table[ent]['ageq'] = table[ent]['ageq']*(table[ent]['ageq'] > 0) 
    table[ent]['ageq'] = 12+ (table[ent]['ageq']-12)*(table[ent]['ageq'] < 12) 
    #TODO: modifier pour les jeunes veufs 
    
    # create fam entity
    try:
        table[ent][['idfam','quifam']] = table[ent].loc[:,['idmen','quimen']]
    except:
        pdb.set_trace()
    
    # save information on qui == 0
    foy0 = table[ent].ix[table[ent]['quifoy']==0,['noi','idfoy','idmen','idfam','period']]
    men0 = table[ent].ix[table[ent]['quimen']==0,['noi','idfoy','idmen','idfam','period']]

#    # Travail sur les qui quand on ne controle pas dans la simulation que tout le monde n'est pas qui==2
## inutile car fait maintenant dans la simulation mais peut-être mieux à refaire ici un jour
## parce que ça prend du temps dans la simulation
#    time_qui = time.clock()
#    for ent in ('men','foy'): # 'fam' un jour...
#        print "Deal with qui for ", ent        
#        qui= 'qui'+ent
#        ident = 'id'+ent
#        trav = table['ind'].ix[table['ind'][qui]==2, [ident,qui,'period']]
#        for name, groupfor nom in ('menage','declar','fam'):for nom in ('menage','declar','fam'): in trav.groupby([ident,'period']):
#            to_add = range(len(group))
#            group[qui] = group[qui]+to_add
#            table['ind'].ix[group[qui].index, qui] = group[qui]
#        print "les qui pour ", ent," sont réglés"
#    time_qui = time.clock() - time_qui
#    print "le temps passé à s'occuper des qui a été",time_qui
    

    
    for entity in entities:
        nom = entity.name
        if nom in name_convertion:
            if nom != 'person': 
                pd.DataFrame(entity.array.columns)
                ent = name_convertion[nom]
                # convert from PyTables to Pandas
                table[ent] = pd.DataFrame(entity.array.columns)
                ident = 'id'+ent
                table[ent] = table[ent].rename(columns={'id': ident})
                table[ent] = merge(table[ent], eval(ent +'0'), how='left', left_on=[ident,'period'], right_on=[ident,'period'])
            # traduction de variable en OF pour ces entités
                
            if ent=='men':
                # nbinde est limité à 6 personnes et donc valeur = 5 en python
                table[ent]['nbinde'] = (table[ent]['nb_persons']-1) * (table[ent]['nb_persons']-1 <=5) +5*(table[ent]['nb_persons']-1 >5)

    table['fam'] = men0 
    
    if period is not None:
        years=[period]
        print years
    
    # a comnmenter quand on est sur du nodele pour gagner un peu de temps
#    test = {}
#    for year in years: 
#        for nom in ('menage','declar'):
#            ent = name_convertion[nom] 
##            print ent, base, ident
#            test[ent] = pd.DataFrame(entity.array.columns).rename(columns={'id': ident})
#            test[ent] = test[ent].ix[test[ent]['period']==year,:]
#            
#            test0 = eval(ent +'0')[eval(ent +'0')['period']==year]
#            
#            tab = table[ent].ix[table[ent]['period']==year,['noi','id'+ent,'idfam']]
#            ind = table['ind'].ix[table['ind']['period']==year,['qui'+ent]] 
#            try:
#                list_ind =  ind[ind==0]
#            except:
#                pdb.set_trace()            
#            lidmen = test[ent][ident]
#            lidmenU = np.unique(lidmen)
#            diff1 = set(test0[ident]).symmetric_difference(lidmenU)
#            print year, ent, diff1
#            for k in diff1:           
#    
#                pd.set_printoptions(max_columns=30)
#                listind = table['ind'][table['ind'][ident]==k]
#                print listind
#                for indiv in np.unique(listind['noi']):
#                    print table['ind'].ix[table['ind']['noi']==indiv,['noi','period','sexe','idmen','quimen','idfoy','quifoy','conj','mere','pere']]
#                    pdb.set_trace()   
              
              

    #available_years = sorted([int(x[-4:]) for x in  store.keys()])              
              
    for year in years:    
        if output=='.h5':
            try: 
                os.remove(output_tab)
            except: 
                print("Attention, la table intermediaire n'a pas ete supprimee")
            goal = HDFStore(output_tab)             
            goal.remove('survey_'+str(year))
            for ent in ('ind','men','foy','fam'):
                tab = table[ent].ix[table[ent]['period']/100==year]
                key = 'survey_'+str(year) + '/'+ent     
                goal.put(key, tab) 
            goal.close()
        else:
            for ent in ('ind','men','foy','fam'):
                table[ent] = table[ent].ix[table[ent]['period']/100==year] 
            return table       
                
if __name__ == "__main__":
    main()