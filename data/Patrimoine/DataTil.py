# -*- coding:utf-8 -*-
'''
Created on 22 juil. 2013
Alexis Eidelman
'''

#TODO: duppliquer la table avant le matching parent enfant pour ne pas se trimbaler les valeur de hod dans la duplication.

from matching import Matching
from pgm.CONFIG import path_data_patr, path_til
import pandas as pd
import numpy as np
from pandas import merge, notnull, DataFrame, Series
from numpy.lib.stride_tricks import as_strided
import pdb
import gc

print path_data_patr

def recode(table, var_in, var_out, list, method, dtype=None):
    '''
    code une variable à partir d'une autre
    attention à la liste et à son ordre pour des méthode avec comparaison d'ordre
    '''
    if var_in == var_out:
        raise Exception("Passer par une variable intermédiaire c'est plus safe")
    
    if dtype is None:
        dtype1 = table[var_in].dtype
        # dtype1 = table[var_in].max()
    
    table[var_out] = Series(dtype=dtype)
    for el in list:
        val_in = el[0]
        val_out = el[1]
        if method is 'geq':
            table[var_out][table[var_in]>=val_in] = val_out
        if method is 'eq':
            table[var_out][table[var_in]==val_in] = val_out
        if method is 'leq':
            table[var_out][table[var_in]<=val_in] = val_out                    
        if method is 'lth':
            table[var_out][table[var_in]< val_in] = val_out                      
        if method is 'gth':
            table[var_out][table[var_in]> val_in] = val_out  
        if method is 'isin':
            table[var_out][table[var_in].isin(val_in)] = val_out  

def index_repeated(nb_rep):
    '''
    Fonction qui permet de numeroter les réplications. Si [A,B,C] sont répliqués 3,4 et 2 fois alors la fonction retourne
    [0,1,2,0,1,2,3,0,1] qui permet ensuite d'avoir 
    [[A,A,A,B,B,B,B,C,C],[0,1,2,0,1,2,3,0,1]] et d'identifier les replications
    '''
    id_rep = np.arange(nb_rep.max())
    id_rep = as_strided(id_rep, shape=nb_rep.shape + id_rep.shape, strides=(0,) + id_rep.strides)
    return  id_rep[id_rep < nb_rep[:, None]]  
            
            
class DataTil(object):
    """
    La classe qui permet de lancer le travail sur les données
    La structure de classe n'est peut-être pas nécessaire pour l'instant 
    """
    def __init__(self):
        self.name = 'Patrimoine'
        self.survey_date = 200901
        self.ind = None
        self.men = None
        self.foy = None
        self.par_look_enf = None
        
        #TODO: Faire une fonction qui chexk où on en est, si les précédent on bien été fait, etc.
        self.done = []
        self.order = ['lecture',]
        
    def lecture(self):
        print "début de l'importation des données"
#fonctionne mais est trop long
#         ind = pd.read_stata(path_data_patr + 'individu.dta')
#         men = pd.read_stata(path_data_patr + 'menage.dta')
        ind = pd.read_csv(path_data_patr + 'individu.csv')
        men = pd.read_csv(path_data_patr + 'menage.csv')
        print "fin de l'importation des données"
        
        #check parce qu'on a un proble dans l'import au niveau de identmen
        #TODO: not solved, see with read_stat in forcoming pandas release
        men['identmen'] = men['identmen'].apply(int)
        ind['identmen'] = ind['identmen'].apply(int)

        
        def correction_carriere():
            '''
            Fait des corrections (à partir de vérif écrit en R)
            '''       
            # Note faire attention à la numérotation à partir de 0
            ind['cydeb1'] = ind['prodep']
            liste1 = [6723,7137,10641,21847,30072,31545,33382]
            liste1 = [x - 1 for x in liste1]
            ind['cydeb1'][liste1] = ind.anais[liste1] + 20
            ind['cydeb1'][15206] = 1963
            ind['cydeb1'][27800] = 1999
            ind['modif'] = Series("", index=ind.index)
            ind['modif'].iloc[liste1 +[15206,27800]] =  "cydeb1_manq"
            
            ind['cyact3'][10833] = 4
            ind['cyact2'][23584] = 11
            ind['cyact3'][27816] = 5
            ind['modif'].iloc[[10833,23584,27816]] = "cyact manq"
            
            var = ["cyact","cydeb","cycaus","cytpto"]
            #TODO: la solution ne semble pas être parfaite du tout
            # cond : gens pour qui on a un probleme de date
            cond1 = notnull(ind['cyact2']) & ~notnull(ind['cyact1'])  & \
                ((ind['cydeb1']==ind['cydeb2']) | (ind['cydeb1'] > ind['cydeb2']) | (ind['cydeb1']==(ind['cydeb2']-1)))
            cond1[8297] = True
            
            ind['modif'][cond1] = "decal act"
            # on decale tout de 1 à gauche en espérant que ça résout le problème
            for k in range(1,16):
                var_k = [x + str(k) for x in var]
                var_k1 = [x + str(k+1) for x in var]
                ind.ix[cond1, var_k] = ind.ix[cond1, var_k1]
            
            # si le probleme n'est pas resolu, le souci était sur cycact seulement, on met une valeur
            cond1 = notnull(ind['cyact2']) & ~notnull(ind['cyact1'])  & \
                ((ind['cydeb1']==ind['cydeb2']) | (ind['cydeb1'] > ind['cydeb2']) | (ind['cydeb1']==(ind['cydeb2']-1)))
            ind['modif'][cond1] = "cyact1 manq"
            ind.ix[ cond1 & (ind['cyact2'] != 4),'cyact1'] = 4
            ind.ix[ cond1 & (ind['cyact2'] == 4),'cyact1'] = 2  
            
            cond2 = ~notnull(ind['cydeb1']) & ( notnull(ind['cyact1']) | notnull(ind['cyact2']))
            ind['modif'][cond1] = "jeact ou anfinetu manq"
            ind.ix[ cond2,'cydeb1'] =  ind.ix[ cond2,['jeactif','anfinetu']].max(axis=1)
            # quand l'ordre des dates n'est pas le bon on fait l'hypothèse que c'est la première date entre
            #anfinetu et jeactif qu'il faut prendre en non pas l'autre
            cond2 = ind['cydeb1'] > ind['cydeb2']
            ind.ix[ cond2,'cydeb1'] = ind.ix[ cond2,['jeactif','anfinetu']].min(axis=1)
            
        def champ_metro(men,ind):
            ''' 
            Se place sur le champ France métropolitaine en supprimant les antilles
            Pourquoi ? - elles n'ont pas les memes variables + l'appariemment EIR n'est pas possible
            '''
            antilles = men.ix[ men['zeat'] == 0,'identmen'].copy()
            men = men[~men['identmen'].isin(antilles)]
            ind = ind[~ind['identmen'].isin(antilles)]                    
            return men, ind        
            
        correction_carriere()
        # Remarque: correction_carriere() doit être lancer avant champ_metro à cause des numeros de ligne en dur
        men, ind = champ_metro(men,ind)
        
        self.men = men
        self.ind = ind 
        all = self.ind.columns.tolist()
        carriere =  [x for x in all if x[:2]=='cy' and x not in ['cyder', 'cysubj']] + ['jeactif', 'anfinetu','prodep']
        self.drop_variable(dict_to_drop={'ind':carriere})       
        
    def drop_variable(self, dict_to_drop=None, option='white'):
        if dict_to_drop is None:
            dict_to_drop={}
            
        # travail sur men 
            all = self.men.columns.tolist()
            #liste noire
            pr_or_cj = [x for x in all if (x[-2:]=='pr' or x[-2:]=='cj') and x not in ['indepr','r_dcpr','r_detpr']]
            detention = [x for x in all if len(x)==6 and x[0]=='p' and x[1] in ['0','1']]
            diplom = [x for x in all if x[:6]=='diplom']
            conj_died = [x for x in all if x[:2]=='cj']
            even_rev =  [x for x in all if x[:3]=='eve']
            black_list = pr_or_cj + detention + diplom + conj_died +even_rev #+ enfants_hdom 
            #liste blanche
            var_to_declar = ['zcsgcrds','zfoncier','zimpot', 'zpenaliv','zpenalir','zpsocm','zrevfin']
            var_apjf = ['asf','allocpar','complfam','paje']
            enfants_hdom = [x for x in all if x[:3]=='hod']
            white_list = ['identmen','pond'] + var_apjf + enfants_hdom + var_to_declar 
            if option=='white':
                dict_to_drop['men'] = [x for x in all if x not in white_list]
            else:
                dict_to_drop['men'] = black_list
                            
        # travail sur ind 
            all = self.ind.columns.tolist()
            #liste noire
            parent_prop = [x for x in all if x[:6]=='jepro_']
            jeunesse_grave = [x for x in all if x[:6]=='jepro_']
            jeunesse = [x for x in all if x[:7]=='jegrave']
            black_list = jeunesse_grave + parent_prop  + diplom
            #liste blanche
            info_pers = ['anais','mnais','sexe','dip14']
            famille = ['couple','lienpref','enf','etamatri','pacs','gpar','per1e','mer1e']
            jobmarket = ['statut','situa','preret','classif']
            info_parent = ['jepnais','jemnais','jemprof']
            carriere =  [x for x in all if x[:2]=='cy' and x not in ['cyder', 'cysubj']] + ['jeactif', 'anfinetu','prodep']
            white_list = ['identmen','noi', 'pond'] + info_pers + famille + jobmarket + carriere + info_parent           
            
            if option=='white':
                dict_to_drop['ind'] = [x for x in all if x not in white_list]
            else:
                dict_to_drop['ind'] = black_list            

        if 'ind' in dict_to_drop.keys():
            self.ind = self.ind.drop(dict_to_drop['ind'], axis=1)
        if 'men' in dict_to_drop.keys():
            self.men = self.men.drop(dict_to_drop['men'], axis=1)
        if 'foy' in dict_to_drop.keys():
            self.foy = self.foy.drop(dict_to_drop['foy'], axis=1)            
            
        
    def format_initial(self):
        
        men = self.men      
        ind = self.ind 
        
        men = men.reset_index(range(len(men)))
        men['id'] = men.index
        
        #passage de ind à men, variable ind['men']
        idmen = Series(ind['identmen'].unique())
        idmen = DataFrame(idmen)
        idmen['men'] = idmen.index
        idmen.columns = ['identmen', 'men']
        verif_match = len(ind)
        ind = merge(idmen, ind, how='inner')
        if len(ind) != verif_match:
            raise Exception("On a perdu le lien entre ind et men via identmen")
        ind['id'] = ind.index
        
        dict_rename = {"zsalaires_i":"sali", "zchomage_i":"choi",
        "zpenalir_i":"alr", "zretraites_i":"rsti", "agfinetu":"findet",
        "cyder":"anc", "duree":"xpr"}
        ind = ind.rename(columns=dict_rename)
        
        def work_on_workstate(ind):
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
            list_situa_work = [ [[1,2],3], 
                                  [[4],2], 
                                  [[5,6,7],1], 
                                  [[1,2],3] ]
            recode(ind,'situa','workstate', list_situa_work ,'isin')
#           Note:  ind['workstate'][ ind['situa']==3] =  0 : etudiant -> NA
           
            #precision inactif
            ind['workstate'][ind['preret']==1]  = 9
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
            return ind['workstate']

        ind['workstate'] = work_on_workstate(ind)
        ind['workstate'].dtype = np.int8
        
        self.men = men
        self.ind = ind 
        self.drop_variable({'men':['identmen','paje','complfam','allocpar','asf'], 'ind':['identmen','preret']})       
        
    def conjoint(self):
        '''
        Calcule l'identifiant du conjoint et vérifie que les conjoint sont bien reciproques 
        '''     
        print ("travail sur les conjoints")
        ind = self.ind
        conj = ind.ix[ind['couple']==1,['men','lienpref','id']]
        conj.ix[conj['lienpref']==31,'lienpref'] = 2
        conj.ix[conj['lienpref']==1,'lienpref'] = 0
        conj = merge(conj, conj, on=['men','lienpref'])
        conj = conj[conj['id_x'] != conj['id_y']]
        couple = pd.groupby(conj, 'id_x')
        for id, potential in couple:
            if len(potential) == 1:
                conj.loc[ conj['id_x']==id, 'id_y'] = potential['id_y']
            else:
                pdb.set_trace()
        # TODO: pas de probleme, bizarre
        conj = conj.rename(columns={'id_x': 'id', 'id_y':'conj'})
        ind = merge(ind,conj[['id','conj']], on='id', how='left')
        
        self.ind = ind
        ## verif sur les conj réciproque
        test_conj = merge(ind[['conj','id']],ind[['conj','id']],
                             left_on='id',right_on='conj')
        print "le nombre de couple non réciproque est:", sum(test_conj['id_x'] != test_conj['conj_y'])
        print ("fin du travail sur les conjoints")
        


    def enfants(self):   
        '''
        Calcule l'identifiant des parents 
        '''    
        ind = self.ind
        print("travail sur les enfants")
        enf = ind.ix[ ind['enf'] != 0 ,['men','lienpref','id','enf']]
        enf0 = enf[enf['enf'].isin([1,2])]
        enf0['lienpref'] = 0
        enf0 = merge(enf0, ind[['men','lienpref','id']], on=['men','lienpref'], how='left', suffixes=('', '_1'))
        
        enf1 = enf[enf['enf'].isin([1,3])]
        enf1['lienpref'] = 1
        enf1 = merge(enf1, ind[['men','lienpref','id']], on=['men','lienpref'], how='left', suffixes=('', '_2'))
        
        #pour les petits enfants, on renverse, on selectionne, les enfants qui seront des 
        #parents pour les petits-enfants
        print("cas des petits-enfants")
        enf4 = enf[enf['enf'].isin([1,2,3])]
        enf4['lienpref'] = 21
        enf4 = merge(enf4, ind[['men','lienpref','id']], on=['men','lienpref'], how='inner', suffixes=('_4', ''))
        enf4['id_1'] = Series(-1, dtype=np.int32)
        enf4['id_2'] = Series(-1, dtype=np.int32)
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

        enf = merge(enf0[['id','id_1']],enf1[['id','id_2']], how='outer')
        enf = enf.append(enf4[['id','id_1','id_2']])
        
        enf = merge(enf,ind[['id','sexe']], left_on='id_1', right_on='id', how = 'left', suffixes=('', '_'))
        del enf['id_']
        
        enf['pere'] = Series(-1, dtype=np.int32)
        enf['pere'][enf['sexe']==1] = enf['id_1'][enf['sexe']==1] 
        enf['mere'] = Series(-1, dtype=np.int32)
        enf['mere'][enf['sexe']==2] = enf['id_1'][enf['sexe']==2] 
        
        cond_pere = notnull(enf['mere']) & notnull(enf['id_2'])
        enf['pere'][cond_pere] = enf['id_2'][cond_pere]
        cond_mere = ~notnull(enf['mere']) & notnull(enf['id_2'])
        enf['mere'][cond_mere] = enf['id_2'][cond_mere]
        #sum(sexe1==sexe2) 6 couples de parents homosexuels
        ind = merge(ind,enf[['id','pere','mere']], on='id', how='left')
        print("fin du travail sur les enfants")
        self.ind = ind


    def mise_au_format(self):
        '''
        On met ici les variables avec les bons codes
        On crée aussi les variables utiles pour la simulation
        '''
        men = self.men      
        ind = self.ind 

        ind['quimen'] = ind['lienpref']
        ind['quimen'][ind['quimen'] >1 ] = 2
        ind['age'] = self.survey_date/100 - ind['anais']
        ind['agem'] = 12*ind['age'] + 11 - ind['mnais']
        ind['period'] = self.survey_date
        men['period'] = self.survey_date
        # a changer avec values quand le probleme d'identifiant et résolu .values
        men['pref'] = ind.ix[ ind['lienpref']==0,'id'].values
        
        self.men = men
        self.ind = ind
        self.drop_variable({'ind':['lienpref','age','anais','mnais']})    

    def creation_foy(self):
        men = self.men      
        ind = self.ind
        print ("creation des declaration")
        def correction_etamatri(ind):
            spouse = notnull(ind['conj']) & ((ind['etamatri']==2) | (ind['pacs']==1))
            ## verif sur les époux réciproques
            #TODO: comprendre ce qui ne va pas.
            test_spouse = ind.ix[spouse,['conj','id','etamatri']]
            test_spouse = merge(test_spouse,test_spouse,
                                 left_on='id',right_on='conj',how='outer')
            prob_spouse = test_spouse['id_x'] != test_spouse['conj_y']
            print "Le nombre d'époux non réciproque est: ", sum(prob_spouse), "mais on corrige"
            #correction: 
            # Hypothese: Si le conjoint est marié on marie le couple, et on ne distingue plus maries et pacsés
            ind['etamatri'][ind['conj'][spouse]] = 2
            ind['etamatri'][ind['id'][spouse]] = 2
            pdb.set_trace()
            #TODO: 
            spouse = (notnull(ind['conj'])) & (ind['etamatri']==2)
            test_spouse = ind.ix[spouse,['conj','id','etamatri']]
            test1 = test_spouse['id'].isin( test_spouse['conj'])
            test2 = test_spouse['conj'].isin( test_spouse['id'])
            test_spouse[['id','conj']][~test2]
            return ind['etamatri']
            
        ind['etamatri'] = correction_etamatri(ind)
        # note: 14134 marié et 14092 en couple, époux hors du dom ? 
        spouse = (notnull(ind['conj'])) & (ind['etamatri']==2)    
        children = (notnull(ind['pere']) | notnull(ind['mere'])) & \
            (ind['etamatri']==1) & (ind['age']<25)  
        
        # selection du conjoint qui va être le declarant
        decl = spouse & ( ind['conj'] > ind['id'])
        #TODO: Partir des données ? si on les a dans l'enquête
        ind['quifoy'] = 0
        ind['quifoy'][spouse & ~decl] = 1
        ind['quifoy'][children] = 2
        
        vous = (ind['quifoy'] == 0)
        foy = DataFrame({'id':range(sum(vous)), 'vous': ind['id'][vous], 
                           'men':ind['men'][vous] })
        
        ind['foy'] = Series(dtype=np.int32)
        ind['foy'][vous] = range(sum(vous))
        spouse1 = spouse & ~decl & ~notnull(ind['foy'])
        ind['foy'][spouse] = ind.ix[spouse,['foy']]
        ind['foy'][children] = ind.ix[ind['pere'][children],['foy']]
        children = children & ~notnull(ind['foy'])
        ind['foy'][children] = ind.ix[ind['mere'][children],['foy']]
        print "le nombre de personne sans foyer est: ", sum(~notnull(ind['foy']))
        #repartition des revenus du ménage par déclaration
        var_to_declar = ['zcsgcrds','zfoncier','zimpot', 'zpenaliv','zpenalir','zpsocm','zrevfin','pond']
        foy_men = men[var_to_declar]
        nb_foy_men = ind[vous].groupby('men').size()
        foy_men = foy_men.div(nb_foy_men,axis=0) 
        
        foy = merge(ind[['foy','men']],foy_men, left_on='men', right_index=True)
        foy['period'] = self.survey_date
        foy['vous'] = ind['id'][vous]
        foy = foy.reset_index(range(len(foy)))
        foy['id'] = foy.index
        print("fin de la creation des declarations")
        #### fin de declar
        self.men = men
        self.ind = ind
        self.foy = foy
        
    def creation_par_look_enf(self):
        '''
        Travail sur les liens parents-enfants. 
        On regarde d'abord les variables utiles pour le matching
        '''
        men = self.men      
        ind = self.ind
        
        ## info sur les enfants hors du domicile des parents
        par_look_enf = DataFrame()
        for k in range(1,13):
            k = str(k)
            var_hod = ['hodln','hodsex','hodan','hodco','hodip','hodenf',
                       'hodemp','hodcho','hodpri','hodniv']
            var_hod_rename=['hodln','sexe','anais','couple','dip6','nb_enf',
                            'hodemp','hodcho','hodpri','hodniv']
            var_hod_k = [var + k for var in var_hod]
            temp = men.ix[notnull(men[var_hod_k[0]]), ['id']+var_hod_k]
            dict_rename = {}
            for num_varname in range(len(var_hod_rename)):
                dict_rename[var_hod_k[num_varname]] = var_hod_rename[num_varname]
            temp = temp.rename(columns=dict_rename)
            
            temp['situa'] = Series(dtype=np.int8)
            temp['situa'][temp['hodemp']==1] = 1
            temp['situa'][temp['hodemp']==2] = 5
            temp['situa'][temp['hodcho']==1] = 4
            temp['situa'][temp['hodcho']==2] = 6
            temp['situa'][temp['hodcho']==3] = 3
            temp['situa'][temp['hodcho']==4] = 7
            
            temp['classif'] = Series()
            prive = temp['hodpri'].isin([1,2,3,4])
            temp['classif'][prive] = temp['hodpri'][prive]
            temp['classif'][~prive] = temp['hodniv'][~prive]
        
            par_look_enf = par_look_enf.append(temp)
            
        
        var_parent = ["id","men","sexe","anais","cs42"]
        ind['gpar'] = ind['per1e'].isin([1,2]) | ind['mer1e'].isin([1,2]) 
        info_pr = ind.ix[ind['lienpref']==0,var_parent]
        info_cj = ind.ix[ind['lienpref']==1,var_parent]    
        var_parent_pr = ['id_pr'] + var_parent[1:]
        var_parent_pr[var_parent_pr.index('anais')] = 'anais_pr'
        var_parent_cj = [nom +'_cj' for nom in var_parent]
        
        # d'abord les peres puis les meres
        info_pr_pere = info_pr[info_pr['sexe']==1].rename(columns={'id':'pere', 'anais':'jepnais','gpar':'gparpat','cs42':'jepprof','sexe':'to_delete'})
        info_cj_pere = info_cj[info_cj['sexe']==1].rename(columns={'id':'pere', 'anais':'jepnais','gpar':'gparpat','cs42':'jepprof','sexe':'to_delete'})
        info_pere = info_pr_pere.append(info_cj_pere)
        
        cond1 = par_look_enf['hodln']==1
        cond2 = par_look_enf['hodln']==2
        cond3 = par_look_enf['hodln']==3
        par_look_enf1 = merge(par_look_enf[cond1], info_pere, left_on='id', right_on='men', how = 'left')
        par_look_enf2 = merge(par_look_enf[cond2], info_pr_pere, left_on='id', right_on='men', how = 'left')
        par_look_enf3 = merge(par_look_enf[cond3], info_cj_pere, left_on='id', right_on='men', how = 'left')
         
        # puis les meres
        info_pr_mere = info_pr[info_pr['sexe']==2].rename(columns={'id':'mere', 'anais':'jemnais','gpar':'gparmat','cs42':'jemprof','sexe':'to_delete'}) 
        info_cj_mere = info_cj[info_cj['sexe']==2].rename(columns={'id':'mere', 'anais':'jemnais','gpar':'gparmat','cs42':'jemprof','sexe':'to_delete'}) 
        info_mere = info_pr_mere.append(info_cj_mere)

        par_look_enf1 = merge(par_look_enf1, info_mere, left_on='id', right_on='men', how = 'left')
        par_look_enf2 = merge(par_look_enf2, info_pr_mere, left_on='id', right_on='men', how = 'left')
        par_look_enf3 = merge(par_look_enf3, info_cj_mere, left_on='id', right_on='men', how = 'left')        
             
        par_look_enf =  par_look_enf1.append(par_look_enf2).append(par_look_enf3)  
        par_look_enf.index = range(len(par_look_enf))  
        par_look_enf['men'] = Series(dtype=np.int32)
        par_look_enf['men'][notnull(par_look_enf['men_x'])] = par_look_enf['men_x']*notnull(par_look_enf['men_x'])
        par_look_enf['men'][notnull(par_look_enf['men_y'])] = par_look_enf['men_y']*notnull(par_look_enf['men_y'])
        par_look_enf = par_look_enf.drop(['hodcho','hodemp','hodniv','hodpri','men_x','men_y','to_delete_x','to_delete_y','jepprof'],axis=1)
        self.par_look_enf = par_look_enf
        
    def matching_par_enf(self):
        '''
        Matching des parents et des enfants hors du domicile
        '''
        
        ind = self.ind
        par_look_enf = self.par_look_enf

        ## info sur les parents hors du domicile des enfants
        cond_enf_look_par = (ind['per1e']==2) | (ind['mer1e']==2)
        enf_look_par = ind[cond_enf_look_par]
        # Remarque: avant on mettait à zéro les valeurs quand on ne cherche pas le parent, maintenant
        # on part du principe qu'on fait les choses assez minutieusement                                           
        
        recode(enf_look_par, 'dip14', 'dip6', [[30,5], [41,4], [43,3], [50,2], [60,1]] , method='geq')
        recode(enf_look_par, 'classif', 'classif2', [ [[1,2,3],4], [[4,5],2], [[6,7],1], [[8,9], 3], [[10],0]], method='isin')
        enf_look_par['classif'] = enf_look_par['classif2']

        ## nb d'enfant
        nb_enf_mere_dom = ind.groupby('mere').size()
        nb_enf_pere_dom = ind.groupby('pere').size()
        nb_enf_mere_hdom = par_look_enf.groupby('mere').size()
        nb_enf_pere_hdom = par_look_enf.groupby('pere').size()
        enf_tot = pd.concat([nb_enf_mere_dom, nb_enf_pere_dom, nb_enf_mere_hdom, nb_enf_pere_hdom], axis=1)
        enf_tot = enf_tot.sum(axis=1)
        #comme enf_tot a le bon index on fait
        enf_look_par['nb_enf'] = enf_tot
        enf_look_par['nb_enf'] = enf_look_par['nb_enf'].fillna(0)
        #Note: Attention le score ne peut pas avoir n'importe quelle forme, il faut des espaces devant les mots, à la limite une parenthèse
        var_match = ['jepnais','situa','nb_enf','anais','classif','couple','dip6', 'jemnais','jemprof','sexe']
        #TODO: gerer les valeurs nulles, pour l'instant c'est très moche
        #TODO: avoir une bonne distance
        score = "- 1 * (other.anais - anais) **2 - 1.0 * (other.situa - situa) **2 - 0.5 * (other.sexe - sexe) **2 - 1.0 * (other.dip6 - dip6) \
         **2 - 1.0 * (other.nb_enf - nb_enf) **2"

        # etape1 : deux parents vivants
        cond1_enf = (enf_look_par['per1e'] == 2) & (enf_look_par['mer1e'] == 2)
        cond1_par = notnull(par_look_enf['pere']) & notnull(par_look_enf['mere'])
        # TODO: si on fait les modif de variables plus tôt, on peut mettre directement par_look_enf1
        #à cause du append plus haut, on prend en fait ici les premiers de par_look_enf
        match1 = Matching(enf_look_par.ix[cond1_enf, var_match], 
                          par_look_enf.ix[cond1_par, var_match], score)
        parent_found = match1.evaluate(orderby=None, method='cells')
        ind.ix[parent_found.index, ['pere','mere']] = par_look_enf.ix[parent_found, ['pere','mere']]
         
        enf_look_par.ix[parent_found.index, ['pere','mere']] = par_look_enf.ix[parent_found, ['pere','mere']]
        cond2_enf = (~notnull(enf_look_par['mere'])) & (enf_look_par['mer1e'] == 2)
        cond2_par = ~par_look_enf.index.isin(parent_found) & notnull(par_look_enf['mere'])
        match2 = Matching(enf_look_par.ix[cond2_enf, var_match], 
                          par_look_enf.ix[cond2_par, var_match], score)
        parent_found2 = match2.evaluate(orderby=None, method='cells')
        ind.ix[parent_found2.index, ['mere']] = par_look_enf.ix[parent_found2, ['mere']]        
            
        enf_look_par.ix[parent_found2.index, ['pere','mere']] = par_look_enf.ix[parent_found2, ['pere','mere']]
        cond3_enf = (~notnull(enf_look_par['pere'])) & (enf_look_par['per1e'] == 2)
        cond3_par = ~par_look_enf.index.isin(parent_found) & notnull(par_look_enf['pere'])
        # TODO: changer le score pour avoir un lien entre pere et mere plus évident
        match3 = Matching(enf_look_par.ix[cond3_enf, var_match], 
                          par_look_enf.ix[cond3_par, var_match], score)
        parent_found3 = match3.evaluate(orderby=None, method='cells')
        ind.ix[parent_found3.index, ['pere']] = par_look_enf.ix[parent_found3, ['pere']]               
        
        self.ind = ind
        self.drop_variable({'ind':['couple','enf','per1e','mer1e','gpar','dip14'] + ['jepnais','jemnais','jemprof']})
    
    def lien_couple_hdom(self):
        NotImplementedError()   
        
        
    def expand_data(self, seuil=150, nb_ligne=None):
        '''
        Note: ne doit pas tourner après lien parent_enfant
        TODO: Une sélecion préalable des variables des tables ind et men ne peut pas faire de mal
        '''
        if seuil!=0 and nb_ligne is not None:
            raise Exception("On ne peut pas à la fois avoir un nombre de ligne désiré et une valeur" \
            "qui va determiner le nombre de ligne")
        #TODO: on peut prendre le min des deux quand même...
        
        #TODO: travailler sur le nombre de variable et le type ! un peu plus
        all = self.men.columns.tolist()
        enfants_hdom = [x for x in all if x[:3]=='hod']
        self.drop_variable({'men':enfants_hdom})
        
        men = self.men      
        ind = self.ind        
        foy = self.foy
        par = self.par_look_enf
        
        if foy is None: 
            print("Notez qu'il est plus malin d'étendre l'échantillon après avoir fait les tables" \
            "foy et par_look_enf plutôt que de les faire à partir des tables déjà étendue")
        
        min_pond = min(men['pond'])
        target_pond = max(min_pond, seuil)
    
        men['nb_rep'] = 1 + men['pond'].div(target_pond).astype(int)
        men['pond'] = men['pond'].div(men['nb_rep'])
        columns_men = men.columns      
        nb_rep_men = np.asarray(men['nb_rep'])      
        men_exp = np.asarray(men).repeat(nb_rep_men, axis=0)
        men_exp = pd.DataFrame(men_exp)
        men_exp.columns = columns_men
        men_exp['id_rep'] =  index_repeated(nb_rep_men)
        men_exp['id_ini'] = men_exp['id']
        men_exp['id'] = men_exp.index
        
        

        if foy is not None:
            foy = merge(men.ix[:,['id','nb_rep']],foy, left_on='id', right_on='men', how='right', suffixes=('_men',''))
            columns_foy = foy.columns 
                       
            nb_rep_foy = np.asarray(foy['nb_rep'])       
            foy_exp =  np.asarray(foy).repeat(nb_rep_foy, axis=0)
            foy_exp = pd.DataFrame(foy_exp)
            foy_exp.columns = columns_foy
            foy_exp['id_ini'] = foy_exp['id']
            foy_exp['id'] = foy_exp.index
            foy_exp['id_rep'] =  index_repeated(nb_rep_foy)
            
            #lien foy men
            nb_foy_by_men = np.asarray(foy.groupby('men').size())
            #TODO: améliorer avec numpy et groupby ? 
            group_old_id = men_exp[['id_ini','id']].groupby('id_ini').groups.values()
            group_old_id = np.array(group_old_id)
            group_old_id =  group_old_id.repeat(nb_foy_by_men)
            new_id = []
            for el in group_old_id: 
                new_id += el
            foy_exp['men'] = new_id
        else: 
            foy_exp = None

        if par is not None:
            par = merge(men.ix[:,['id','nb_rep']], par, left_on='id', right_on='men', how='right', suffixes=('_men',''))
            columns_par = par.columns    
            nb_rep_par = np.asarray(par['nb_rep'])       
            par_exp =  np.asarray(par).repeat(nb_rep_par, axis=0)
            par_exp = pd.DataFrame(par_exp)
            par_exp.columns = columns_par
            par_exp['id_ini'] = par_exp['id']
            par_exp['id'] = par_exp.index
            par_exp['id_rep'] =  index_repeated(nb_rep_par)
            
            #lien par men
            nb_par_by_men = np.asarray(par.groupby('men').size())
            #TODO: améliorer avec numpy et groupby ? 
            group_old_id = men_exp.ix[men_exp['id_ini'].isin(par['men']),['id_ini','id']].groupby('id_ini').groups.values()
            group_old_id = np.array(group_old_id)
            group_old_id =  group_old_id.repeat(nb_par_by_men)
            new_id = []
            for el in group_old_id: 
                new_id += el 
            par_exp['men'] = new_id           
        else: 
            par_exp = None
                        
        ind = merge(men.ix[:,['id','nb_rep']],ind, left_on='id', right_on='men', how='right', suffixes = ('_men',''))
        nb_rep_ind = np.asarray(ind['nb_rep'])
        ind = ind.drop(['nb_rep'], axis=1)
        columns_ind = ind.columns
        id_ini = np.asarray(ind.index).repeat(nb_rep_ind, axis=0)
        ind_exp = np.asarray(ind).repeat(nb_rep_ind, axis=0)
        # on cree un numero de la duplication..., ie si l'indiv 1 est répété trois fois on met id_rep = [0,1,2]

        ind_exp = pd.DataFrame(ind_exp)
        ind_exp.columns = columns_ind
        ind_exp['id_rep'] = index_repeated(nb_rep_ind)
        ind_exp['id_ini'] = id_ini
        
        # liens entre individus
        ind_exp[['pere','id_rep']]
        tableA = ind_exp[['pere','mere','conj','id_rep']].reset_index()
        tableB = ind_exp[['id_rep','id_ini']]
        tableB['id_index'] = tableB.index
        ind_exp = ind_exp.drop(['pere', 'mere','conj'], axis=1)
        print("debut travail sur identifiant")
        pere = tableA.merge(tableB,left_on=['pere','id_rep'], right_on=['id_ini','id_rep'], how='inner').set_index('index')
        pere = pere.drop(['pere','mere','conj','id_ini','id_rep'], axis=1).rename(columns={'id_index':'pere'})
        ind_exp = ind_exp.merge(pere, left_index=True,right_index=True, how='left', copy=False) 
        
        mere = tableA.merge(tableB,left_on=['mere','id_rep'], right_on=['id_ini','id_rep'], how='inner').set_index('index')
        mere = mere.drop(['pere','mere','conj','id_ini','id_rep'], axis=1).rename(columns={'id_index':'mere'})
        ind_exp = ind_exp.merge(mere, left_index=True,right_index=True, how='left', copy=False) 
        
        conj = tableA.merge(tableB,left_on=['conj','id_rep'], right_on=['id_ini','id_rep'], how='inner').set_index('index')
        conj = conj.drop(['pere','mere','conj','id_ini','id_rep'], axis=1).rename(columns={'id_index':'conj'})
        ind_exp = ind_exp.merge(conj, left_index=True,right_index=True, how='left', copy=False) 
        print("fin travail sur identifiant")
        
        ind_exp['id'] = ind_exp.index
        
        # lien indiv - men
        nb_ind_men = np.asarray(ind.groupby('men').size())
        #TODO: améliorer avec numpy et groupby ? 
        group_old_id = men_exp[['id_ini','id']].groupby('id_ini').groups.values()
        group_old_id = np.array(group_old_id)
        group_old_id =  group_old_id.repeat(nb_ind_men)
        new_id = []
        for el in group_old_id: 
            new_id += el
        ind_exp['men'] = new_id

        men_exp['pref'] = ind_exp.ix[ ind_exp['quimen']==0,'id'].values
        
        # lien indiv - foy
        if foy is not None:
            nb_ind_foy = np.asarray(ind.groupby('foy').size())
            group_old_id = foy_exp[['id_ini','id']].groupby('id_ini').groups.values()
            group_old_id = np.array(group_old_id)
            group_old_id =  group_old_id.repeat(nb_ind_foy)
            new_id = []
            for el in group_old_id: 
                new_id += el
            ind_exp['foy'] = new_id  
            foy_exp['vous'] = ind_exp.ix[ ind_exp['quifoy']==0,'id'].values      
        
        self.par_look_enf = par
        self.men = men_exp
        self.ind = ind_exp
        self.foy = foy_exp
        self.drop_variable({'men':['id_rep','nb_rep','index'], 'ind':['id_rep','id_men',]})    

if __name__ == '__main__':
    data = DataTil()
    data.lecture()
    data.drop_variable()
    #drop_variable() doit tourner avant format_initial() car on fait comme si diplome par exemple n'existait pas
    # plus généralement, on aurait un problème avec les variables qui sont renommées.
    data.format_initial()
    
    data.conjoint()
    data.enfants()

#     data.creation_foy()
    data.creation_par_look_enf()
    data.mise_au_format()
    
    import time
    start = time.clock()
    data.expand_data(seuil=0)
    data.matching_par_enf()    
    print "temps de l'expansion : ", time.clock() - start
    pdb.set_trace()