# -*- coding:utf-8 -*-

'''
Created on 25 Apr 2013

@author: alexis_e
'''
from __future__ import print_function
from future.builtins import range

from pandas import HDFStore, DataFrame, Series
import numpy as np
import pdb
import time
import gc
import datetime as dt
import os

from scipy.stats import rankdata

from til import __path__ as path_til
import openfisca_france
from openfisca_core import simulations

# import check_structure from : ...
# from openfisca_france_data.build_openfisca_survey_data.utils import check_structure
def check_structure(dataframe):
# duplicates = dataframe.noindiv.duplicated().sum()
# assert duplicates == 0, "There are {} duplicated individuals".format(duplicates)
# df.drop_duplicates("noindiv", inplace = True)
    for entity in ["men", "foy"]:
        role = 'qui' + entity
        entity_id = 'id' + entity
        assert not dataframe[role].isnull().any(), "there are NaN in qui{}".format(entity)
        max_entity = dataframe[role].max().astype("int")

        for position in range(0, max_entity + 1):
            test = dataframe[[role, entity_id]].groupby(by = entity_id).agg(lambda x: (x == position).sum())
            if position == 0:
                errors = (test[role] != 1).sum()
                if errors > 0:
                    import pdb
                    pdb.set_trace()
            else:
                errors = (test[role] > 1).sum()
                if errors > 0:
                    import pdb
                    pdb.set_trace()

    for entity in ['foy', 'men']:
        assert len(dataframe['id' + entity].unique()) == (dataframe['qui' + entity] == 0).sum(),\
            "Wronger number of entity/head for {}".format(entity)

### list des variable que l'on veut conserver
### Note plus vraiment utile
# listkeep = {'ind': ["salsuperbrut","cotsoc_noncontrib","cotsal_noncontrib","cotsoc_bar","cotsoc_lib",
#                      "cotpat_contrib","cotpat_noncontrib","cotsal_contrib","cotsal","impo","psoc","mini","pfam","logt"],
#             'men': ["decile","decile_net", "pauvre60", "revdisp", "revini", "revnet", "typ_men", "uc"],
#             'fam': ["aah","caah","aeeh","aefa","af","cf","paje", "al","alf","als","apl","ars","asf",
#                      "api","apje","asi","aspa","rmi","rsa","rsa_socle"],
#             'foy': ["decote", "irpp", "isf_tot", "avantage_qf"]}

rename_variables = {'statmarit':'civilstate', 'quifam':'quimen'}
# on prend ce sens pour que famille puisse chercher dans menage comme menages
traduc_entities = {'foyers_fiscaux':'declar', 'menages':'menage',
                    'individus':'person', 'familles':'menage'}
input_even_if_formula = ['age', 'agem']

new_ident = {'noi':'id', 'idmen':'men', 'idfoy':'foy', 'idfam':'men'}
id_to_row = {}

def deal_with_qui(qui, ident):
    ''' change qui to have a unique qui by ident
        assumes that there is an accumulation (on qui=2) '''
    order = np.lexsort((qui, ident))
    diff_ident = np.ones(qui.shape, qui.dtype)
    diff_ident[1:] = np.diff(ident[order])
    squi = qui[order].copy()
    diff_qui = np.ones(qui.shape, qui.dtype)
    diff_qui[1:] = np.diff(squi)
    cond = (diff_ident == 0) & (diff_qui == 0)
    while sum(cond) > 0:
        squi[cond] += 1
        diff_qui[1:] = np.diff(squi)
        cond = (diff_ident == 0) & (diff_qui == 0)
    qui[order] = squi
    return qui

def main(liam, annee_leg=None,annee_base=None, mode_output='array'):
    ''' Send data from the simulation to openfisca
    - annee_base: si rempli alors on tourne sur cette année-là, sinon sur toute la base
     mais à voir
     - annee_leg pour donner les paramètres
     '''
    print("annee base", annee_base)

    ## on recupere la liste des annees en entree
    if annee_base is not None:
        if isinstance(annee_base,int):
            annee_base = [annee_base]
    else:
        #TODO: ? peut-être updater pour qaund
        output_tab = os.path.join(path_til[0], "output", "to_run_leg.h5" )
        get_years =  HDFStore(output_tab)
        years = [x[-4:] for x in dir(get_years.root) if x[0]!='_' ]
        get_years.close()

    TaxBenefitSystem = openfisca_france.init_country()
    tax_benefit_system = TaxBenefitSystem()
    column_by_name = tax_benefit_system.column_by_name

    simulation = simulations.Simulation(
    compact_legislation = None,
    date = dt.date(2009, 5, 1),
    debug = None,
    debug_all = None,
    tax_benefit_system = tax_benefit_system,
    trace = None,
    )

    entities = liam.entities
    entities_name =  [e.name for e in liam.entities]
    def _get_entity(name):
        position = entities_name.index(name)
        return liam.entities[position]

    input = dict()
    required = dict()  # pour conserver les valeurs que l'on va vouloir sortir de of.
    #pour chaque entité d'open fisca
    ## load data :
    selected_rows = {}
    for of_ent_name, of_entity in tax_benefit_system.entity_class_by_key_plural.items():
        input[of_ent_name] = []
        required[of_ent_name] = []
        # on cherche l'entité corrspondante dans liam
        til_ent_name = traduc_entities[of_ent_name]
        til_entity =  _get_entity(til_ent_name)
        til_entity = til_entity.array.columns

        if of_entity.is_persons_entity:
            selected = (til_entity['men'] >= 10) & (til_entity['foy'] >= 10)
            til_entity['quimen'][selected] = deal_with_qui(til_entity['quimen'][selected],
                                                            til_entity['men'][selected])
            til_entity['quifoy'][selected] = deal_with_qui(til_entity['quifoy'][selected],
                                                            til_entity['foy'][selected])
#             select_var = ('quifoy', 'quimen', 'id', 'men', 'foy')
#             test = dict()
#             for var in select_var:
#                 test[var] = til_entity[var]
#             test['idmen'] = til_entity['men']
#             test['idfoy'] = til_entity['foy']
#             test = DataFrame(test)
#             test = test.loc[selected,:]
#             check_structure(test)
#            assert set(til_entity['civilstate']).issubset(set([1,2,3,4,5])) # TODO error here

        else:
            selected = np.ones(len(til_entity['id']), dtype=bool)
            selected[til_entity['id'] < 10] = False

        selected_rows[of_ent_name] = selected
        of_entity.step_size = sum(selected)
        of_entity.count = sum(selected)

        of_entity.roles_count = 10 #TODO: faire une fonction (max du nombre d'enfant ?

        # pour toutes les variables de l'entité of
        for column in of_entity.column_by_name:
            # on regarde si on les a dans til sous un nom ou un autre
            if column in list(rename_variables.keys()) + list(til_entity.keys()) + list(new_ident.keys()):
                holder = simulation.get_or_new_holder(column)
                #on selectionne les valeurs d'entrée
                if holder.formula is None or column in input_even_if_formula:
                    input[of_ent_name].append(column)
                    if column in til_entity:
                        holder.array = til_entity[column][selected]
                    elif column in new_ident:
                        ident = til_entity[new_ident[column]][selected]
                        holder.array = rankdata(ident, 'dense').astype(int) - 2
                    else:
                        holder.array = til_entity[rename_variables[column]][selected]
                # sinon, on conserve la liste des variable pour tout à l'heure
                else:
                    required[of_ent_name].append(column)
#         print(of_ent_name)
#         print(required[of_ent_name])
#         print(input[of_ent_name])

    ### load of outputs
    for entity, entity_vars in required.items():
        til_ent_name = traduc_entities[entity]
        til_entity = _get_entity(til_ent_name)
        til_column = til_entity.array.columns
        selected = selected_rows[entity]
        test = simulation.calculate('nbptr')
        assert min(test) > 0
        for var in entity_vars:
            til_column[var][selected] = simulation.calculate(var)
            #TODO: check incidence of following line
            til_column[var][~selected] = 0