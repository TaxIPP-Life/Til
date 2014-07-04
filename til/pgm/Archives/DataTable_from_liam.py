# -*- coding:utf-8 -*-

"""
Convert Liam output in OpenFisca Input
"""
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


temps = time.clock()    
input = "C:/Myliam2/Model/simulTest.h5"
output = "C:/openfisca/output/liam/"

name_convertion = {'person':'ind','declar':'foy','menage':'men', 'fam':'fam'}

store = HDFStore(input)
goal = HDFStore("C:/openfisca/src/countries/france/data/surveyLiam.h5")
#available_years = sorted([int(x[-4:]) for x in  store.keys()])


# on travaille d'abord sur l'ensemble des tables puis on selectionne chaque annee

# step 1
table = {}
nom = 'person'
base = 'entities/'+nom
ent = name_convertion[nom]
table[ent] = store[str(base)]
# get years
years = np.unique(table[ent]['period'].values)[:1]
# rename variables to make them OF ones
table[ent] = table[ent].rename(columns={'res': 'idmen', 'quires': 'quimen', 'foy': 'idfoy', 'id': 'noi'})

# travail important sur les qui==2
time_qui = time.clock()
for ent in ('men','foy'): # 'fam' un jour...
    qui= 'qui'+ent
    ident = 'id'+ent
    trav = table['ind'].ix[table['ind'][qui]==2, [ident,qui,'period']]
    for name, group in trav.groupby([ident,'period']):
        to_add = range(len(group))
        group[qui] = group[qui]+to_add
        table['ind'].ix[group[qui].index, qui] = group[qui]
    print "les qui pour ", ent," sont réglés"


time_qui = time.clock() - time_qui
print time_qui


ent = 'ind'
# création de variable
table[ent]['agem'] = 12 * table[ent]['age'] 

table[ent]['ageq'] =  table[ent]['age']/5 - 4 
f = lambda x: min( max(x, 0), 12)
table[ent]['ageq'] = table[ent]['ageq'].map(f)




# menage qu'on élimine pour l'instant
#diff_foy = set([3880, 3916, 4190, 7853, 8658, 9376, 9508, 9717, 12114, 13912, 15260])
#
#temp = table['ind']['idfoy'].isin(diff_foy)
#diff_ind = table['ind'][temp]['id']
#diff_men = table['ind'][temp]['idmen'].copy()
#temp_ent = table[ent]['idmen'].isin(diff_men) 
#table[ent] = table[ent][-temp_ent]
# il faut espérer après qu'avec les ménages, on s'en sort et qu'on n'a pas de 
# pere dans diff_ind pour quelqu'un d'autre, ie, un pere hors du domicile supprimé
# on fait on s'en fout, on fait que de la légisaltion ici

# create fam base
table[ent][['idfam','quifam']] = table[ent][['idmen','quimen']]
# save information on qui == 0
foy0 = table[ent].ix[table[ent]['quifoy']==0,['id','idfoy','idmen','idfam','period']]
men0 = table[ent].ix[table[ent]['quimen']==0,['id','idfoy','idmen','idfam','period']]
fam0 = men0

for nom in ('menage','declar','fam'):
    ent = name_convertion[nom]    
    base = 'entities/'+nom
    ident = 'id'+ent
    if ent == 'fam':
        table[ent] = eval(ent +'0')
    else :
        table[ent] = store[str(base)].rename(columns={'id': ident})
        table[ent] = merge(table[ent], eval(ent +'0'), how='left', left_on=[ident,'period'], right_on=[ident,'period'])
        
    # traduction de variable en OF pour ces entités
    if ent=='men':
        # nbinde est limité à 6 personnes et donc valeur = 5 en python
        table[ent]['nbinde'] = (table[ent]['nb_persons']-1) * (table[ent]['nb_persons']-1 <=5) +5*(table[ent]['nb_persons']-1 >5)
    
    
#    temp_ent = table[ent]['idmen'].isin(diff_men) 
#    print ent
#    table[ent] = table[ent][-temp_ent]
                   
# test sur le nombre de qui ==0
test = {}
for year in years:
    for nom in ('menage','declar'):
        ent = name_convertion[nom] 
        base = 'entities/'+nom
        ident = 'id'+ent
        print ent, base, ident
        test[ent] = store[str(base)].rename(columns={'id': ident})
        test[ent] = test[ent].ix[test[ent]['period']==year,:]
        
        test0 = eval(ent +'0')[eval(ent +'0')['period']==year]
        
        tab = table[ent].ix[table[ent]['period']==year,['id','id'+ent,'idfam']]
        ind = table['ind'].ix[table['ind']['period']==year,['qui'+ent]] 
        list_ind =  ind[ind==0]
        lidmen = test[ent][ident]
        lidmenU = np.unique(lidmen)
        diff1 = set(test0[ident]).symmetric_difference(lidmenU)
        
#voir = store[str(base)][['id','period']].rename(columns={'id': ident})
#voir = store[str(base)].rename(columns={'id': ident})[[ident,'period']]
#voir.ix[ voir['period']==2011,'id']
#
#test[ent][ident][:10]
#test1.ix[table[ent]['period']==year,['idmen']]
# il y a un truc avec les gens qui se marient puis divorcent
# en profiter pour bien gerer les conj = 0 ou conj =-1
        # si on ne s'arrete pas là, c'est qu'on n'a pas de problème !! 
        print year, ent, diff1
        for k in diff1:           

            pd.set_printoptions(max_columns=30)
            listind = table['ind'][table['ind'][ident]==k]
            print listind
            for indiv in np.unique(listind['id']):
                print table['ind'].ix[table['ind']['id']==indiv,['id','period','sexe','idmen','quimen','idfoy','quifoy','conj','mere','pere']]
                pdb.set_trace()   
        
            
for year in years:
    goal.remove('survey_'+str(year))
    for ent in ('ind','men','foy','fam'):
        tab = table[ent].ix[table[ent]['period']==year]
        key = 'survey_'+str(year) + '/'+ent     
        goal.put(key, tab) 
#    if year == 2010:
#        pdb.set_trace()
#        tab = table[ent].ix[table[ent]['period']==year]
#        tab[:5]
#        len(tab['idfam'])
#        len(np.unique(tab['idfam']))
#        list_qui = tab['idfam']
#        double = list_qui.value_counts()[list_qui.value_counts()>1]
#        tabind = table['ind'].ix[table['ind']['period']==year]
        
        
store.close()
goal.close()

# on fais maintenant tourner le modèle OF
country = 'france'    
for year in years:        
    yr = str(year)
    deb3 =  time.clock()
    
    simu = SurveySimulation()
    simu.set_config(year = year, country = country)
    # mettre les paramètres de a législation 2009
    date_str = str(2009)+ '-01-01'
    date = dt.datetime.strptime(date_str ,"%Y-%m-%d").date()
    reader = XmlReader(simu.param_file, date)
    rootNode = reader.tree
    simu.P_default = Tree2Object(rootNode, defaut=True)
    simu.P_default.datesim = date
    simu.P = Tree2Object(rootNode, defaut=False)
    simu.P.datesim = date
    
    simu.set_survey(filename="C:/openfisca/src/countries/france/data/surveyLiam.h5", num_table=3, print_missing=True)
    simu.compute()
    
    for ent in ('ind','men','foy','fam'):
        df = simu.outputs.table3[ent]
        not_bool = df.dtypes[df.dtypes != bool]
        print df.ix[:50,df.dtypes[df.dtypes == bool].index]
        df = df.ix[:,not_bool.index]
        r_dataframe = com.convert_to_r_dataframe(df)
        name = ent+'_'+str(year)
        r.assign(name, r_dataframe)
        file_dir = output + name+ ".gzip"
        phrase = "save("+name+", file='" +file_dir+"', compress=TRUE)"
        r(phrase) 
    fin3  = time.clock()


print time.clock()- temps




