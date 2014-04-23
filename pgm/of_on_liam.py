# -*- coding:utf-8 -*-

'''
Created on 25 Apr 2013

@author: alexis_e
'''

from pandas import HDFStore, DataFrame, Series
import numpy as np
import pdb
import tables
import time
import gc
import datetime as dt   

from utils import of_name_to_til
import liam2of
from CONFIG import path_liam, path_til


import datetime
from openfisca_core import simulations
import openfisca_france
from openfisca_france import surveys

### list des variable que l'on veut conserver
### Note plus vraiment utile
listkeep = {'ind': ["salsuperbrut","cotsoc_noncontrib","cotsal_noncontrib","cotsoc_bar","cotsoc_lib",
                     "cotpat_contrib","cotpat_noncontrib","cotsal_contrib","cotsal","impo","psoc","mini","pfam","logt"],
            'men': ["decile","decile_net", "pauvre60", "revdisp", "revini", "revnet", "typ_men", "uc"],
            'fam': ["aah","caah","aeeh","aefa","af","cf","paje", "al","alf","als","apl","ars","asf",
                     "api","apje","asi","aspa","rmi","rsa","rsa_socle"],
            'foy': ["decote", "irpp", "isf_tot", "avantage_qf"]}

traduc_variables = {'noi':'id', 'idmen':'men', 'idfoy':'foy', 'statmarit':'civilstate'}
# on prend ce sens pour que famille puisse chercher dans menage comme menages
traduc_entities = {'foyers_fiscaux':'declar', 'menages':'menage',
                    'individus':'person', 'familles':'menage'}
input_even_if_formula = ['age', 'agem']
traduction = {}
traduction['person'] = {'men': 'idmen', 'foy': 'idfoy', 'id': 'noi', 'statmarit': 'civilstate'}


def main(liam, annee_leg=None,annee_base=None, output='array'):
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
#        output_h5[ent]=DataFrame() 
    
    ## on recupere la liste des annees en entree
    if annee_base is not None:
        if isinstance(annee_base,int):
            annee_base = [annee_base]
    else:
        #TODO: ? peut-être updater pour qaund
        get_years =  HDFStore(path_til + "/output/to_run_leg.h5")   
        years = [x[-4:] for x in dir(get_years.root) if x[0]!='_' ]
        get_years.close()
    
    
    
    TaxBenefitSystem = openfisca_france.init_country()
    tax_benefit_system = TaxBenefitSystem()
    column_by_name = tax_benefit_system.column_by_name
    
    simulation = simulations.Simulation(
    compact_legislation = None,
    date = datetime.date(2009, 5, 1),
    debug = None,
    debug_all = None,
    tax_benefit_system = tax_benefit_system,
    trace = None,
    )
    
    entities = liam.entities
    entities_name =  map( lambda e: e.name, liam.entities)
    def _get_entity(name):
        position = entities_name.index(name)
        return liam.entities[position]
    input = dict()
    required = dict()  # pour conserver les valeurs que l'on va vouloir sortir de of.
    #pour chaque entité d'open fisca

    for of_ent_name, of_entity in tax_benefit_system.entity_class_by_key_plural.iteritems():
        input[of_ent_name] = []
        required[of_ent_name] = []
        # on cherche l'entité corrspondante dans liam
        til_ent_name = traduc_entities[of_ent_name]
        til_entity =  _get_entity(til_ent_name)
        of_entity.count = of_entity.step_size = sum(til_entity.id_to_rownum > 0)
        of_entity.roles_count = sum(til_entity.id_to_rownum > 0) + 1 
        til_entity = til_entity.array.columns
        # pour toutes les variables de l'entité of
        for column in of_entity.column_by_name:
            # on regarde si on les a dans til sous un nom ou un autre
            if column in traduc_variables or column in til_entity:
                holder = simulation.get_or_new_holder(column)
                #on selectionne les valeurs d'entrée
                if holder.formula is None or column in input_even_if_formula:
                    input[of_ent_name].append(column)
                    if column in til_entity:
                        holder.array = til_entity[column]
                    else: 
                        holder.array = til_entity[traduc_variables[column]]
                # sinon, on conserve la liste des variable pour tout à l'heure
                else: 
                    required[of_ent_name].append(column)
        print(of_ent_name)
        print(required[of_ent_name])
        print(input[of_ent_name])
    
    simulation.calculate('nbF')
    pdb.set_trace()

#         
#     output_of = 'list_de_variable_qui_sortent_du_model'
#     for var in ind.array.columns:
#         if var in output_of:
#             liam.entities.etc.var.values = simulation.calculate(var)
#                                                  
#         tps_write = time.clock() - deb_write
#         del simu
#         gc.collect()
#         fin3  = time.clock()
#         print ("La législation sur l'année %s vient d'être calculée en %d secondes"
#                    " dont %d pour le chargement, %d pour la simul pure et %d pour la sauvegarde") %(year, fin3-deb3,
#                                                                         tps_charge, tps_comp, tps_write )          


if __name__ == "__main__":
    main(2009)