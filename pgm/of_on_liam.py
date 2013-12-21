# -*- coding:utf-8 -*-

'''
Created on 25 Apr 2013

@author: alexis_e
'''

from pandas import HDFStore, merge # DataFrame
import numpy as np
import pdb
import tables
import time

from utils import of_name_to_til
import liam2of
from CONFIG import path_of, path_liam, path_til

import sys
sys.path.append(path_of)
sys.path.remove('C:\\liam2')
# sys.path.remove(path__liam)
try:
    sys.modules.pop('src')
    from src.lib.simulation import SurveySimulation
    from src.parametres.paramData import XmlReader, Tree2Object
except:
    pdb.set_trace()
import pandas as pd 
import datetime as dt   
import pandas.rpy.common as com     
from rpy2.robjects import r
import gc

### list des variable que l'on veut conserver
### Note plus vraiment utile
listkeep = {'ind': ["salsuperbrut","cotsoc_noncontrib","cotsal_noncontrib","cotsoc_bar","cotsoc_lib",
                     "cotpat_contrib","cotpat_noncontrib","cotsal_contrib","cotsal","impo","psoc","mini","pfam","logt"],
            'men': ["decile","decile_net", "pauvre60", "revdisp", "revini", "revnet", "typ_men", "uc"],
            'fam': ["aah","caah","aeeh","aefa","af","cf","paje", "al","alf","als","apl","ars","asf",
                     "api","apje","asi","aspa","rmi","rsa","rsa_socle"],
            'foy': ["decote", "irpp", "isf_tot", "avantage_qf"]}
    
def main(simulation, annee_leg=None,annee_base=None, output='array'):
    ''' Send data from the simulation to openfisca
    - annee_base: si rempli alors on tourne sur cette année-là, sinon sur toute la base
     mais à voir
     - annee_leg pour donner les paramètres
     '''
    print "annee base", annee_base
    #TODO: test output is either a simulation either a string
    # if not isinstance(output,SurveySimulation)
#    #### initialisation, si on veut une grosse table de sortie
#    for ent in ('ind','men','foy','fam'):
#        del output_h5[ent]
#        output_h5[ent]=pd.DataFrame() 
  
    ## on recupere la liste des annees en entree
    if annee_base is not None:
        if isinstance(annee_base,int):
            annee_base = [annee_base]
    else:
        #TODO: ? peut-être updater pour qaund
        get_years =  HDFStore(path_til + "/output/to_run_leg.h5")   
        years = [x[-4:] for x in dir(get_years.root) if x[0]!='_' ]
        get_years.close()
    
    country = 'france'    
    for year in annee_base:        
        yr = str(year)
        deb3 =  time.clock()
          
        simu = SurveySimulation()
        simu.set_config(year = year, country = country)
        #_load_parameters(annee_leg):
        date_str = str(annee_leg)+ '-01-01'
        date = dt.datetime.strptime(date_str ,"%Y-%m-%d").date()
        reader = XmlReader(simu.param_file, date)
        rootNode = reader.tree
        simu.P_default = Tree2Object(rootNode, defaut=True)
        simu.P_default.datesim = date
        simu.P = Tree2Object(rootNode, defaut=False)
        simu.P.datesim = date
            
        table = liam2of.table_for_of(simulation, year, check_validity=True, save_tables=False)
        simu.set_config(survey_filename=table, num_table=3, print_missing=False)
        
        tps_charge = time.clock() - deb3
        print tps_charge, time.clock()
        deb_comp =  time.clock()
        simu.compute()
        tps_comp = time.clock() - deb_comp
        print "total", time.clock() - deb3
             
        # save results in the simulation or in a hdf5 table.
        deb_write =  time.clock()        
        if output == '.h5':
            # chemin de sortie
            output = path_til + "/output/"
            output_h5 = tables.openFile(output+"simul_leg.h5",mode='w')
            output_entities = output_h5.createGroup("/", "entities",
                                                              "Entities")              
            for ent in ('ind','men','foy',"fam"):
    #            #TODO: gerer un bon keep pour ne pas avoir trop de variable  
                tab = simu.output_table.table3[ent]
    ###  export par table     
                if ent=='ind':
                    ident = ["idmen","quimen","idfam","quifam","idfoy","quifoy","noi"]
                else:
                    ident = ["idmen","idfam","idfoy"]
                renam ={}
                for nom in ident:
                    renam[nom+'_'+ent] = nom
                tab = tab.rename(columns=renam)
                tab = tab[listkeep[ent]+ident]
                tab['period'] = pd.Series(np.ones(len(tab)) * int(year),dtype=int)
                 
                ident = 'id'+ent
                if ent=='ind':
                    ident='noi'
    
                #on retire les identifiant sauf celui qui deviendra id
                list_id = ['idmen','idfoy','idfam','id','quifoy','quifam','quimen','noi'] 
                list_id.remove(ident)
                to_remove = [x for x in tab.columns if x in list_id]
                #on n4oublie pas de garder periode
                tab = tab.drop(to_remove,axis=1)
                tab = tab.rename(columns={ident:'id'})           
                tab['id'] = tab['id'].astype(int)           
                nom = of_name_to_til[ent]
                output_type = tab.to_records(index=False).dtype
                #TODO: ameliorer pour optimiser tout ca, les type
                to_int = ['id','period']
                for x in output_type.descr: 
                    if x[0] in to_int : 
                        x=(x[0],'<i4')
                        
                if output_entities.__contains__(nom):
                    output_table = getattr(output_entities, nom) 
                else: 
                    output_table = output_h5.createTable('/entities',nom,output_type)
                output_table.append(tab.to_records(index=False))
                output_table.flush() 
            output_h5.close()
            
        else: 
            entities = simulation.entities
            for entity in entities:
                nom = entity.name
                if nom in of_name_to_til:
                    ent = of_name_to_til [nom]
                    vars = [x for x in simu.output_table.table3[ent].columns if x in entity.array.columns]
                    for var in vars:
                        value = simu.output_table.table3[ent][var]
                        #TODO: test the type
                        if len(entity.array[var]) != len(value):
                            print ent, nom, var, len(entity.array[var]),  len(value)
                            pdb.set_trace()
                        entity.array[var] = np.array(value)
                #TODO: change that ad hoc solution
                if nom == 'menage':
                    ent = 'fam'
                    vars = [x for x in simu.output_table.table3[ent].columns if x in entity.array.columns]
                    for var in vars:
                        value = simu.output_table.table3[ent][var]
                        #TODO: test the type
                        if len(entity.array[var]) != len(value):
                            print ent, nom, var, len(entity.array[var]),  len(value)
                            pdb.set_trace()
                        entity.array[var] = np.array(value)  
                                                 
        tps_write = time.clock() - deb_write
        del simu
        gc.collect()
        fin3  = time.clock()
        print ("La législation sur l'année %s vient d'être calculée en %d secondes"
                   " dont %d pour le chargement, %d pour la simul pure et %d pour la sauvegarde") %(year, fin3-deb3,
                                                                        tps_charge, tps_comp, tps_write )          

# [('period', '<i4'), ('id', '<i4'), ('age', '<i4'), ('sexe', '<i4'), ('wprm_init', '<i4'),
# ('men', '<i4'), ('quimen', '<i4'), ('foy', '<i4'), ('quifoy', '<i4'), ('pere', '<i4'), 
# ('mere', '<i4'), ('conj', '<i4'), ('dur_in_couple', '<f8'), ('dur_out_couple', '<i4'), 
# ('civilstate', '<i4'), ('education_level', '<i4'), ('findet', '<i4'), ('workstate', '<i4'), 
# ('sali', '<f8'), ('productivity', '<f8'), ('rsti', '<f8'), ('choi', '<f8'), ('xpr', '<i4'), 
# ('anc', '<i4'), ('dur_rest_ARE', '<i4')]
        
####  export par period       
#            df = simu.output_table.table3[ent]     
##            key = 'survey_'+str(year) + '/'+ent              
##            output_h5.put(key,df)
####     export en R
#            not_bool = df.dtypes[df.dtypes != bool]
#            df = df.ix[:,not_bool.index]
#            r_dataframe = com.convert_to_r_dataframe(df)
#            name = ent+'_'+str(year)
#            r.assign(name, r_dataframe)
#            file_dir = output + name+ ".gzip"
#            phrase = "save("+name+", file='" +file_dir+"', compress=TRUE)"
#            r(phrase) 

if __name__ == "__main__":
    main(2009)