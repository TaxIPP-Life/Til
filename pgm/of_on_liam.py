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

from scipy.stats import rankdata

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

rename_variables = {'statmarit':'civilstate'}
# on prend ce sens pour que famille puisse chercher dans menage comme menages
traduc_entities = {'foyers_fiscaux':'declar', 'menages':'menage',
                    'individus':'person', 'familles':'menage'}

input_even_if_formula = ['age', 'agem']
traduction = {}
traduction['person'] = {'men': 'idmen', 'foy': 'idfoy', 'id': 'noi', 'statmarit': 'civilstate'}

new_ident = {'noi':'id', 'idmen':'men', 'idfoy':'foy', 'idfam':'men'}
id_to_row = {}

def main(liam, annee_leg=None,annee_base=None, mode_output='array'):
    ''' Send data from the simulation to openfisca
    - annee_base: si rempli alors on tourne sur cette année-là, sinon sur toute la base
     mais à voir
     - annee_leg pour donner les paramètres
     '''
    print "annee base", annee_base
    #TODO: test mode_output is either a simulation either a string
    # if not isinstance(mode_output,SurveySimulation)
#    #### initialisation, si on veut une grosse table de sortie
#    for ent in ('ind','men','foy','fam'):
#        del mode_output_h5[ent]
#        mode_output_h5[ent]=DataFrame() 
    
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

    ## load data : 
    
    for of_ent_name, of_entity in tax_benefit_system.entity_class_by_key_plural.iteritems():
        input[of_ent_name] = []
        required[of_ent_name] = []
        # on cherche l'entité corrspondante dans liam
        til_ent_name = traduc_entities[of_ent_name]
        til_entity =  _get_entity(til_ent_name)
        of_entity.count = of_entity.step_size = sum(til_entity.id_to_rownum > 0) + 1
        of_entity.roles_count = 10 #TODO: faire une fonction
        of_entity.is_persons_entity
        
        til_entity = til_entity.array.columns
        # pour toutes les variables de l'entité of
        for column in of_entity.column_by_name:
            # on regarde si on les a dans til sous un nom ou un autre
            if column in rename_variables or column in til_entity or column in new_ident:
                holder = simulation.get_or_new_holder(column)
                #on selectionne les valeurs d'entrée
                if holder.formula is None or column in input_even_if_formula:
                    input[of_ent_name].append(column)
                    if column in til_entity:
                        holder.array = til_entity[column]
                    elif column in new_ident:
                        ident = til_entity[new_ident[column]]
                        holder.array = rankdata( ident, 'dense').astype(int) - 2
                    else: 
                        holder.array = til_entity[rename_variables[column]]
                # sinon, on conserve la liste des variable pour tout à l'heure
                else: 
                    required[of_ent_name].append(column)
#         print(of_ent_name)
#         print(required[of_ent_name])
#         print(input[of_ent_name])
    
    ### load of outputs
    for entity, entity_vars in required.iteritems():
        til_ent_name = traduc_entities[entity]
        til_entity = _get_entity(til_ent_name)
        til_column = til_entity.array.columns
        for var in entity_vars:
            til_column[var] = simulation.calculate(var)

if __name__ == "__main__":
    main(2009)