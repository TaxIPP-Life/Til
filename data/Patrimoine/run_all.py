# -*- coding:utf-8 -*-
'''
Created on 17 juil. 2013
Alexis Eidelman
'''

from pgm.CONFIG import path_data_patr, path_til
import pandas as pd
import pdb
import gc

print path_data_patr

# pd.read_stata(path_data_patr + 'individu.dta')
ind = pd.read_csv(path_data_patr + 'individu.csv')
men = pd.read_csv(path_data_patr + 'menage.csv')
print "fin de l'importation des données"

#check parce qu'on a un proble dans l'import au niveau de identmen
#TODO: not solved, see with read_stat in forcoming pandas release
men['identmen'] = men['identmen'].apply(int)
ind['identmen'] = ind['identmen'].apply(int)

# correction après utilisation des programmes verif (en R pour l'instant)
# Note faire attention à la numérotation à partir de 0
ind['cydeb1'] = ind.prodep
liste1 = [6723,7137,10641,21847,30072,31545,33382]
liste1 = [x - 1 for x in liste1]
ind['cydeb1'][liste1] = ind.anais[liste1] + 20
ind['cydeb1'][15206] = 1963
ind['cydeb1'][27800] = 1999
ind['modif'] = pd.Series("", index=ind.index)
ind['modif'].iloc[liste1 +[15206,27800]] =  "cydeb1_manq"

ind['cyact3'][10833] = 4
ind['cyact2'][23584] = 11
ind['cyact3'][27816] = 5
ind['modif'].iloc[[10833,23584,27816]] = "cyact manq"

var = ["cyact","cydeb","cycaus","cytpto"]

#TODO: la solution ne semble pas être parfaite du tout
# cond : gens pour qui on a un probleme de date
cond1 = pd.notnull(ind['cyact2']) & ~pd.notnull(ind['cyact1'])  & \
    ((ind['cydeb1']==ind['cydeb2']) | (ind['cydeb1'] > ind['cydeb2']) | (ind['cydeb1']==(ind['cydeb2']-1)))
cond1[8297] = True

ind['modif'][cond1] = "decal act"
# on decale tout de 1 à gauche en espérant que ça résout le problème
for k in range(1,16):
    var_k = [x + str(k) for x in var]
    var_k1 = [x + str(k+1) for x in var]
    ind.ix[cond1, var_k] = ind.ix[cond1, var_k]

# si le probleme n'est pas resolu, le souci était sur cycact seulement, on met une valeur
cond1 = pd.notnull(ind['cyact2']) & ~pd.notnull(ind['cyact1'])  & \
    ((ind['cydeb1']==ind['cydeb2']) | (ind['cydeb1'] > ind['cydeb2']) | (ind['cydeb1']==(ind['cydeb2']-1)))
ind['modif'][cond1] = "cyact1 manq"
ind.ix[ cond1 & (ind['cyact2'] != 4),'cyact1'] = 4
ind.ix[ cond1 & (ind['cyact2'] == 4),'cyact1'] = 2  

cond2 = ~pd.notnull(ind['cydeb1']) & ( pd.notnull(ind['cyact1']) | pd.notnull(ind['cyact2']))
ind['modif'][cond1] = "jeact ou anfinetu manq"
ind.ix[ cond2,'cydeb1'] =  ind.ix[ cond2,['jeactif','anfinetu']].max(axis=1)

# quand l'ordre des dates n'est pas le bon on fait l'hypothèse que c'est la première date entre
#anfinetu et jeactif qu'il faut prendre en non pas l'autre
cond2 = ind['cydeb1'] > ind['cydeb2']
ind.ix[ cond2,'cydeb1'] = ind.ix[ cond2,['jeactif','anfinetu']].min(axis=1)


############### on supprime les antilles ##################
# Pourquoi ? - elles n'ont pas les memes variables ni l'appariemment
# Pourquoi pas dès le début ? - Des raison historiques : si on remonte ca casse les numeros de
# lignes utilises plus haut mais TODO:
antilles = men.ix[ men['zeat'] == 0,'identmen'].copy()
men = men[~men['identmen'].isin(antilles)]
men = men.reset_index()
# on fusionne ind et men pour ne garder que les ind de men.
#pref inutile pour l'isntant, ajouter d'autre plus tard
ind = pd.merge(men.ix[:,['identmen','pref']],ind, \
                on='identmen', how='left')

# travail sur le 'men'
idmen = pd.Series(ind['identmen'].unique())
idmen = pd.DataFrame(idmen)
idmen['men'] = idmen.index
idmen.columns = ['identmen', 'men']
ind = pd.merge(idmen, ind)
ind['id'] = ind.index

###### Conjoints
conj = ind.ix[ind['couple']==1,['men','lienpref','id']]
conj.ix[conj['lienpref']==31,'lienpref'] = 2
conj.ix[conj['lienpref']==1,'lienpref'] = 0
conj2 = conj.copy()
conj = pd.merge(conj, conj2, on=['men','lienpref'])
conj = conj[conj['id_x'] != conj['id_y']]
couple = pd.groupby(conj, 'id_x')

print ("travail sur les conjoints")
for id, potential in couple:
    if len(potential) == 1:
        conj.loc[ conj['id_x']==id, 'id_y'] = potential['id_y']
    else:
        pdb.set_trace()
# TODO: pas de probleme, bizarre
conj = conj.rename(columns={'id_x': 'id', 'id_y':'conj'})
ind = pd.merge(ind,conj[['id','conj']], on='id', how='left')

## verif sur les conj réciproque
test_conj = pd.merge(ind[['conj','id']],ind[['conj','id']],
                     left_on='id',right_on='conj')
print sum(test_conj['id_x'] != test_conj['conj_y'])

del conj
del couple
del test_conj
gc.collect()
print ("fin du travail sur les conjoints")

###### Enfants
print("travail sur les enfants")
enf = ind.ix[ ind['enf'] != 0 ,['men','lienpref','id','enf']]
enf0 = enf[enf['enf'].isin([1,2])]
enf0['lienpref'] = 0
enf0 = pd.merge(enf0, ind[['men','lienpref','id']], on=['men','lienpref'], how='left', suffixes=('', '_1'))

enf1 = enf[enf['enf'].isin([1,3])]
enf1['lienpref'] = 1
enf1 = pd.merge(enf1, ind[['men','lienpref','id']], on=['men','lienpref'], how='left', suffixes=('', '_2'))

#pour les petits enfants, on renverse, on selectionne, les enfants qui seront des 
#parents pour les petits-enfants
print("cas des petits-enfants")
enf4 = enf[enf['enf'].isin([1,2,3])]
enf4['lienpref'] = 21
enf4 = pd.merge(enf4, ind[['men','lienpref','id']], on=['men','lienpref'], how='inner', suffixes=('_4', ''))
enf4['id_1'] = pd.Series()
enf4['id_2'] = pd.Series()
parents = pd.groupby(enf4, 'id')
for id, parent in parents:
    if len(parent) == 1:
        enf4.loc[ enf4['id']==id, 'id_1'] = parent['id_4']
    elif len(parent) == 2:
        enf4.loc[ enf4['id']==id, 'id_1'] = parent['id_4'].values[0]
        enf4.loc[ enf4['id']==id, 'id_2'] = parent['id_4'].values[1]
    else:
        # cas à résoudre
#         print(ind.ix[ind['men']==parent['men'].values[0],['age','lienpref']])
        enf4.ix[ enf4['id']==id, 'id_1'] = 15043

enf = pd.merge(enf0[['id','id_1']],enf1[['id','id_2']], how='outer')
enf = enf.append(enf4[['id','id_1','id_2']])

enf = pd.merge(enf,ind[['id','sexe']], left_on='id_1', right_on='id', how = 'left', suffixes=('', '_'))
del enf['id_']

enf['pere'] = pd.Series()
enf['pere'][enf['sexe']==1] = enf['id_1'][enf['sexe']==1] 
enf['mere'] = pd.Series()
enf['mere'][enf['sexe']==2] = enf['id_1'][enf['sexe']==2] 

cond_pere = pd.notnull(enf['mere']) & pd.notnull(enf['id_2'])
enf['pere'][cond_pere] = enf['id_2'][cond_pere]
cond_mere = ~pd.notnull(enf['mere']) & pd.notnull(enf['id_2'])
enf['mere'][cond_mere] = enf['id_2'][cond_mere]
#sum(sexe1==sexe2) 6 couples de parents homosexuels
ind = pd.merge(ind,enf[['id','pere','mere']], on='id', how='left')

## nb d'enfant
#TODO au moment où on en a besoin
nb_enf_mere = enf.groupby('mere').size()
nb_enf_pere = enf.groupby('pere').size()
# note, qu'on doit faire la somme en cas de couple homosexuel

del enf0, enf1, enf4, enf
gc.collect()

print("fin du travail sur les enfants")


###### situation sur le marché du travail
print("début du codage de workstate")
# code destinie reproduit ici
# inactif   <-  1
# chomeur   <-  2
# non_cadre <-  3
# cadre     <-  4
# fonct_a   <-  5
# fonct_s   <-  6
# indep     <-  7
# avpf      <-  8
# preret    <-  9
#on travaille avec situa puis avec statut puis avec classif
ind['workstate'] = pd.Series()
ind['workstate'][ ind['situa'].isin([1,2])] = 3
ind['workstate'][ ind['situa']==3] =  11
ind['workstate'][ ind['situa']==4] =  2
ind['workstate'][ ind['situa'].isin([5,6,7])] = 1
ind['workstate'][ ind['situa'].isin([1,2])] = 0
ind['workstate'][ ind['situa']==3] =  0 #remet ce qui était en R, mais étrange TODO: explications
#precision inactif
ind['workstate'][ ind['preret']==1]  = 9
# precision AVPF
#TODO: "vous pouver bénéficier de l'AVPF si vous n'exercer aucune activité 
# professionnelle (ou seulement à temps partiel) et avez 
# la charge d'une personne handicapée (enfant de moins de 20 ans ou adulte).
# Pour l'instant, on fait ça parce que ça colle avec PensIPP mais il faudrait faire mieux.
#en particulier c'est de la législation l'avpf finalement.
cond =  (men['paje']==1) | (men['complfam']==1) | (men['allocpar']==1) | (men['asf']==1)
avpf = men.ix[cond,:].index.values + 1 
ind['workstate'][(ind['men'].isin(avpf)) & (ind['workstate'].isin([1,2]))] = 8
# public, privé, indépendant
ind['workstate'][ ind['statut'].isin([1,2])] = 5
ind['workstate'][ ind['statut']==7] =  7
# cadre, non cadre
ind['workstate'][ (ind['classif']==6)  & (ind['workstate']==5)] = 6
ind['workstate'][ (ind['classif']==7)  & (ind['workstate']==3)] = 4
#retraite
ind['workstate'][ (ind['anais'] < 2009-64)  & (ind['workstate']==1)] = 10
print("fin du codage de workstate")


ind['quires'] = ind['lienpref']
ind['quires'][ind['quires'] >1 ] = 2

ind['age'] = 2009 - ind['anais']
ind['agem'] = 12*ind['age'] + 11 - ind['mnais']
ind['period'] = 200901
men['period'] = 200901
# a changer avec values quand le probleme d'identifiant et résolu .values
men['pref'] = ind.ix[ ind['lienpref']==0,'id'].values

##### declarations
print ("creation des declaration")
children = ((ind['pere']>0) | (ind['mere']>0)) & \
    (ind['etamatri']==1) & (ind['age']<25)  
spouse = (ind['conj']>0) & (ind['etamatri']==2)

## verif sur les époux réciproques
#TODO: comprendre ce qui ne va pas.
test_spouse = ind.ix[spouse,['conj','id','etamatri']]
test_spouse = pd.merge(test_spouse,test_spouse,
                     left_on='id',right_on='conj',how='outer')
prob_conj = test_spouse['id_x'] != test_spouse['conj_y']
print sum(prob_conj)
# note: 14328 marié et 14092 en couple, époux hors du dom ? 

# selection du conjoint qui va être le declarant
decl = spouse & ( ind['conj'] > ind['id'])
#TODO: Partir des données ? si on les a dans l'enquête
ind['quifoy'] = 0
ind['quifoy'][spouse & ~decl] = 1
ind['quifoy'][children] = 2

vous = (ind['quifoy'] == 0)
foy = pd.DataFrame({'id':range(sum(vous)), 'vous': ind['id'][vous], 
                   'res':ind['men'][vous] })

ind['foy'] = pd.Series()
ind['foy'][vous] = range(sum(vous))
pdb.set_trace()
spouse1 = spouse & ~decl & ~pd.notnull(ind['foy'])
ind['foy'][spouse] = ind.ix[spouse,['foy']]
ind['foy'][children] = ind.ix[ind['pere'][children],['foy']]
children = children & ~pd.notnull(ind['foy'])
ind['foy'][children] = ind.ix[ind['mere'][children],['foy']]
print sum(~pd.notnull(ind['foy']))

#repartition des revenus du ménage par déclaration
var_to_declar = ['zcsgcrds','zfoncier','zimpot', \
     'zpenaliv','zpenalir','zpsocm','zrevfin','pond']
foy_men = men[var_to_declar]
nb_foy_men = ind[vous].groupby('men').size()
foy_men = foy_men.div(nb_foy_men,axis=0) 

foy = pd.merge(ind[['foy','men']],foy_men, left_on='men', right_index=True)
foy['period'] = 200901
foy['vous'] = ind['id'][vous]
foy = foy.reset_index()
foy['id'] = foy.index
print("fin de la creation des declarations")
#### fin de declar

####### travail sur les liens parents-enfants. 
inf_pr = [ind['lienpref']==0,["id","res","sexe","anais","per1e","mer1e","cs42"]]
inf_pr = [ind['lienpref']==1,["id","res","sexe","anais","per1e","mer1e","cs42"]]

gpar = ind['per1e'].isin([1,2]) | ind['mer1e'].isin([1,2])

k = str(1)
var_hod = ['hodln','hodsex','hodan','hodco','hodip','hodenf',
           'hodemp','hodcho','hodpri','hodniv']
var_hod_rename=['hodln','sex','anais','couple','dip6',
           'nb_enf']
var_hod_k = [var + k for var in var_hod]
men['id'] = men.index
test = men[['id']+var_hod_k]
dict_rename = 
