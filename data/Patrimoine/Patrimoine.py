# -*- coding:utf-8 -*-
'''
Created on 2 août 2013

@author: a.eidelman

Ce programme :
- 

Input : 
Output :

'''

# 1- Importation des classes/librairies/tables nécessaires à l'importation des données de l'enquête Patrimoine
 
 
from data.DataTil import DataTil
from data.matching import Matching
from data.utils import recode, index_repeated, replicate, new_link_with_men, minimal_dtype
from pgm.CONFIG import path_data_patr, path_til

import pandas as pd
import numpy as np

from pandas import merge, notnull, DataFrame, Series
from numpy.lib.stride_tricks import as_strided

import pdb
import gc

# Patrimoine est définie comme une classe fille de DataTil
class Patrimoine(DataTil):   
       
    def __init__(self):
        DataTil.__init__(self)
        self.name = 'Patrimoine'
        self.survey_year = 2009
        self.last_year = 2009
        self.survey_date = 100*self.survey_year + 1
         
        #TODO: Faire une fonction qui check où on en est, si les précédent on bien été fait, etc.
        #TODO: Dans la même veine, on devrait définir la suppression des variables en fonction des étapes à venir.
        self.done = []
        self.methods_order = ['load','format_initial','drop_variable','table_initial','conjoint','enfants',
                      'creation_par_look_enf','expand_data','matching_par_enf','matching_couple_hdom',
                      'creation_foy','mise_au_format','var_sup','store_to_liam']
    
# drop_variable() doit tourner avant table_initial() car on aurait un problème avec les variables qui sont renommées.
# explication de l'ordre en partant de la fin, besoin des couples pour et des liens parents enfants pour les mariages.
# Ces liens ne peuvent se faire qu'après la dupplication pour pouvoir avoir le bon nombre de parents et de bons matchs
# La dupplication, c'est mieux si elle se fait après la création de par_look_enf, plutôt que de chercher à créer par_look_enf à partir de la base étendue
# Pour les enfants, on cherche leur parent un peu en fonction de s'ils sont en couple ou non, ça doit donc touner après conjoint.
# Ensuite, c'est assez évident que le format initial et le drop_variable doivent se faire le plus tôt possible
# on choisit de faire le drop avant le format intitial, on pourrait faire l'inverse en étant vigilant sur les noms
        
    def load(self):
        print "début de l'importation des données"
        ind = pd.read_csv(path_data_patr + 'individu.csv')
        men = pd.read_csv(path_data_patr + 'menage.csv')
        print "fin de l'importation des données"
              
        #check parce qu'on a un probleme dans l'import au niveau de identmen(pas au format numérique)
        men['identmen'] = men['identmen'].apply(int)
        ind['identmen'] = ind['identmen'].apply(int)
        self.men = men
        self.ind = ind
        
    def format_initial(self):
        ind = self.ind
        men = self.men
        def _correction_carriere(ind):
            '''
            Fait des corrections sur le déroulé des carrières(à partir de vérif écrit en R)
            ''' 
            # Note faire attention à la numérotation à partir de 0
            #TODO: verifier que c'est bien pris en compte malgré le fait qu'on ne passe pas par .loc 
            #TODO: faire une verif avec des asserts
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
            #Note : la solution ne semble pas être parfaite au sens qu'elle ne résout pas tout
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
            return ind
            
        def _correction_etamatri(ind):
            '''
            Cohérence entre le statut marital et le fait d'être en couple ou non
            Utile pour le lien mais aussi pour la création des foyers
            Noter que l'on recode le pacs comme un modalité de l'union
            '''
            statu_marit = ind[['identmen','age','couple','etamatri','pacs','lienpref']].fillna(0)    
    #        pour voir statu_marit.groupby(['couple','etamatri','pacs']).size()
            ind['etamatri'][ind['pacs']==1] = 5 
            prob_couple = (ind['etamatri'].isin([2,5])) & (ind['couple'] == 3) 
            statu_marit = statu_marit[prob_couple]
            statu_marit['identmen'] = statu_marit['identmen']/100
            many_by_men = statu_marit['identmen'].value_counts() > 1
            many_by_men = statu_marit['identmen'].value_counts()[many_by_men]
            prob_couple_ident = statu_marit[statu_marit['identmen'].isin(many_by_men.index.values.tolist())]
            ind.loc[prob_couple_ident.index,'couple'] = 1
            prob_couple2 = (ind['etamatri'].isin([2,5])) & (ind['couple'] == 3) 
            ind[prob_couple2]['etamatri'] = 4
            
            # présence d'un conjoint si couple=1 et lienpref in 0,1
            conj = ind.ix[ind['couple']==1,['identmen','lienpref','id']]
#             conj['lienpref'].value_counts()
            # pref signifie "personne de reference"
            pref0 = conj.ix[conj['lienpref']==0,'identmen']
            pref1 = conj.ix[conj['lienpref']==1,'identmen']
            assert sum(~pref1.isin(pref0)) == 0
            manque_conj = pref0[~pref0.isin(pref1)]
            ind.ix[manque_conj.index,'couple'] = 2
            
            pref2 = conj.ix[conj['lienpref']==2,'identmen']
            pref31 = conj.ix[conj['lienpref']==31,'identmen'] 
            assert sum(~pref31.isin(pref2)) == 0          
            manque_conj = pref2[~pref2.isin(pref31)]
            ind.ix[manque_conj.index,'couple'] = 2
            ind = ind.rename(columns={'etamatri': 'civilstate'})
            return ind
            
        def _champ_metro(ind,men):
            ''' 
            Se place sur le champ France métropolitaine en supprimant les antilles
            Pourquoi ? - elles n'ont pas les memes variables + l'appariemment EIR n'est pas possible
            '''
            antilles = men.ix[ men['zeat'] == 0,'identmen']
            men = men[~men['identmen'].isin(antilles)]
            ind = ind[~ind['identmen'].isin(antilles)]
            return ind, men  
        
        # Note avec la version Python3.x on utiliserait la notion de nonlocal pour ne pas mettre de paramètre 
        # et d'affectation aux fonctions ci-dessous
        ind = _correction_carriere(ind)
        # Remarque: correction_carriere() doit être lancer avant champ_metro à cause des numeros de ligne en dur
        ind = _correction_etamatri(ind)
        ind, men = _champ_metro(ind, men)

        self.men = men
        self.ind = ind 
        all = self.ind.columns.tolist()
        carriere =  [x for x in all if x[:2]=='cy' and x not in ['cyder', 'cysubj']] + ['jeactif','prodep']
        self.drop_variable(dict_to_drop={'ind':carriere})       
        
    def drop_variable(self, dict_to_drop=None, option='white'):
        '''
        - Si on dict_to_drop is not None, il doit avoir la forme table: [liste de variables],
        on retire alors les variable de la liste de la table nommée.
        - Sinon, on se sert de cette méthode pour faire la première épuration des données, on
         a deux options:
             - passer par la liste blanche ce que l'on recommande pour l'instant 
             - passer par  liste noire. 
        '''
        men = self.men
        if dict_to_drop is None:
            dict_to_drop={}
            
        # travail sur men 
            all = men.columns.tolist()
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
            famille = ['couple','lienpref','enf','civilstate','pacs','gpar','per1e','mer1e']
            jobmarket = ['statut','situa','preret','classif']
            info_parent = ['jepnais','jemnais','jemprof']
            carriere =  [x for x in all if x[:2]=='cy' and x not in ['cyder', 'cysubj']] + ['jeactif', 'anfinetu','prodep']
            revenus = ["zsalaires_i", "zchomage_i", "zpenalir_i", "zretraites_i", "cyder", "duree"]           
            
            white_list = ['identmen','noi', 'pond'] + info_pers + famille + jobmarket + carriere + info_parent + revenus 
            
            if option=='white':
                dict_to_drop['ind'] = [x for x in all if x not in white_list]
                
            else:
                dict_to_drop['ind'] = black_list            
            
        DataTil.drop_variable(self, dict_to_drop, option)
    
    def table_initial(self):
        
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
        "zpenalir_i":"alr", "zretraites_i":"rsti", "anfinetu":"findet",
        "cyder":"anc", "duree":"xpr"}
        ind = ind.rename(columns=dict_rename)
        
        def _work_on_workstate(ind):
            '''
            On code en s'inspirant de destinie et de PENSIPP ici. 
            Il faudrait voir à modifier pour avoir des temps partiel
            '''

            # inactif   <-  1  # chomeur   <-  2   # non_cadre <-  3  # cadre     <-  4
            # fonct_a   <-  5  # fonct_s   <-  6   # indep     <-  7  # avpf      <-  8
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
            return ind['workstate']

        ind['workstate'] = _work_on_workstate(ind)
        ind['workstate'].dtype = np.int8
        
        self.men = men
        self.ind = ind 
        self.drop_variable({'men':['identmen','paje','complfam','allocpar','asf'], 'ind':['identmen','preret']})       
        self.men = minimal_dtype(self.men)
        self.ind = minimal_dtype(self.ind)
        
    def conjoint(self):
        '''
        Calcul de l'identifiant du conjoint et vérifie que les conjoint sont bien reciproques 
        '''     
        print ("travail sur les conjoints")
        ind = self.ind
        conj = ind.ix[ind['couple']==1,['men','lienpref','id']]
        conj['lienpref'].value_counts()
        conj.ix[conj['lienpref']==1,'lienpref'] = 0
        conj.ix[conj['lienpref']==31,'lienpref'] = 2
        conj.ix[conj['lienpref']==32,'lienpref'] = 3
        conj.ix[conj['lienpref']==50,'lienpref'] = 10
        conj2 = merge(conj, conj, on=['men','lienpref'])
        conj2 = conj2[conj2['id_x'] != conj2['id_y']]
        assert len(conj2) == len(conj)
        conj = conj2
        test = pd.groupby(conj, ['men','lienpref']).size()
        assert max(test)==2 and min(test)==2
        couple = pd.groupby(conj, 'id_x')
        for id, potential in couple:
            if len(potential) == 1:
                conj.loc[ conj['id_x']==id, 'id_y'] = potential['id_y']
            else:
                pdb.set_trace()
        # TODO: pas de probleme, bizarre
        conj = conj.rename(columns={'id_x': 'id', 'id_y':'conj'})
        ind = merge(ind,conj[['id','conj']], on='id', how='left')
        
        test_conj = merge(ind[['conj','id']],ind[['conj','id']],
                             left_on='id',right_on='conj')
        self.ind = ind
        print "le nombre de couple non réciproque est:", sum(test_conj['id_x'] != test_conj['conj_y'])

        print ("fin du travail sur les conjoints")


    def enfants(self):   
        '''
        Calcule l'identifiant des parents 
        '''    
        ind = self.ind
        enf = ind.ix[ ind['enf'] != 0 ,['men','lienpref','id','enf']]
        enf0 = enf[enf['enf'].isin([1,2])]
        enf0['lienpref'] = 0
        enf0 = merge(enf0, ind[['men','lienpref','id']], on=['men','lienpref'], how='left', suffixes=('', '_1'))
        
        enf1 = enf[enf['enf'].isin([1,3])]
        enf1['lienpref'] = 1
        enf1 = merge(enf1, ind[['men','lienpref','id']], on=['men','lienpref'], how='left', suffixes=('', '_2'))
              
        # cas des petits-enfants : on cherche les enfants de la personne de référence (enf=1,2 ou 3) et on tente de les associer 
        # aux petits enfants (lienpref=31)
        # en toute rigueur, il faudrait garder un lien si on ne trouve pas les parents pour l'envoyer dans le registre...
        # et savoir que ce sont les petites enfants (pour l'héritage par exemple)
        
        par4 = enf[enf['enf'].isin([1,2,3])]
        par4['lienpref'] = 21
        par4 = merge(par4, ind[['men','lienpref','id']], on=['men','lienpref'], how='inner', suffixes=('_4', ''))
        enf4 = DataFrame( index=par4['id'].unique(), columns=['id_1','id_2'], dtype=np.int32)
        parents = pd.groupby(par4, 'id')
        for idx, parent in parents:
            id = int(idx)
            if len(parent) == 1:
                enf4['id_1'][id] = int(parent['id_4'])
            else:
                # cas à résoudre "à la main"
                potential = ind.loc[parent['id_4'], ['anais','lienpref','sexe','couple','conj']]
                potential = potential[ind.loc[id,'anais'] - potential['anais'] > 16 ]
                pot_mother = potential[potential['sexe']]
                if len(pot_mother):
                    par =  pot_mother['anais'].idxmin()
                else: 
                    par =  potential['anais'].idxmin()
                enf4['id_1'][id] = par
        
        enf4['id'] = enf4.index
        enf4['id_2'] = ind.ix[enf4['id_1'],'conj'].values
        

        enf = merge(enf0[['id','id_1']],enf1[['id','id_2']], how='outer')
        #enf = enf.append(enf4[['id','id_1','id_2']])       
        enf = merge(enf,ind[['id','sexe']], left_on='id_1', right_on='id', how = 'left', suffixes=('', '_'))
        del enf['id_']
    
        enf['pere'] = Series(dtype=np.int32)
        enf['pere'][enf['sexe']==0] = enf['id_1'][enf['sexe']==0] 
        enf['mere'] = Series(dtype=np.int32)
        enf['mere'][enf['sexe']==1] = enf['id_1'][enf['sexe']==1] 
        
        cond_pere = notnull(enf['mere']) & notnull(enf['id_2'])
        enf['pere'][cond_pere] = enf['id_2'][cond_pere]
        cond_mere = ~notnull(enf['mere']) & notnull(enf['id_2'])
        enf['mere'][cond_mere] = enf['id_2'][cond_mere]
        #sum(sexe1==sexe2) 6 couples de parents homosexuels
        ind = merge(ind,enf[['id','pere','mere']], on='id', how='left')
        ind['sexe'] = ind['sexe'].astype(bool)
        self.ind = ind

          
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
        info_pr_pere = info_pr[~info_pr['sexe']].rename(columns={'id':'pere', 'anais':'jepnais','gpar':'gparpat','cs42':'jepprof','sexe':'to_delete'})
        info_cj_pere = info_cj[~info_cj['sexe']].rename(columns={'id':'pere', 'anais':'jepnais','gpar':'gparpat','cs42':'jepprof','sexe':'to_delete'})
        info_pere = info_pr_pere.append(info_cj_pere)
        
        cond1 = par_look_enf['hodln']==1
        cond2 = par_look_enf['hodln']==2
        cond3 = par_look_enf['hodln']==3
        par_look_enf1 = merge(par_look_enf[cond1], info_pere, left_on='id', right_on='men', how='left')
        par_look_enf2 = merge(par_look_enf[cond2], info_pr_pere, left_on='id', right_on='men', how='left')
        par_look_enf3 = merge(par_look_enf[cond3], info_cj_pere, left_on='id', right_on='men', how='left')
         
        # puis les meres
        info_pr_mere = info_pr[info_pr['sexe']].rename(columns={'id':'mere', 'anais':'jemnais','gpar':'gparmat','cs42':'jemprof','sexe':'to_delete'}) 
        info_cj_mere = info_cj[info_cj['sexe']].rename(columns={'id':'mere', 'anais':'jemnais','gpar':'gparmat','cs42':'jemprof','sexe':'to_delete'}) 
        info_mere = info_pr_mere.append(info_cj_mere)

        par_look_enf1 = merge(par_look_enf1, info_mere, left_on='id', right_on='men', how = 'left')
        par_look_enf2 = merge(par_look_enf2, info_pr_mere, left_on='id', right_on='men', how = 'left')
        par_look_enf3 = merge(par_look_enf3, info_cj_mere, left_on='id', right_on='men', how = 'left')        
             
        par_look_enf =  par_look_enf1.append(par_look_enf2).append(par_look_enf3)  
        par_look_enf.index = range(len(par_look_enf))  
        par_look_enf['men'] = Series(dtype=np.int32)
        par_look_enf['men'][notnull(par_look_enf['men_x'])] = par_look_enf['men_x'][par_look_enf['men_x'].notnull()]
        par_look_enf['men'][notnull(par_look_enf['men_y'])] = par_look_enf['men_y'][par_look_enf['men_y'].notnull()]
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
         
        #etape 2 : seulement mère vivante
        enf_look_par.ix[parent_found.index, ['pere','mere']] = par_look_enf.ix[parent_found, ['pere','mere']]
        cond2_enf = (~notnull(enf_look_par['mere'])) & (enf_look_par['mer1e'] == 2)
        cond2_par = ~par_look_enf.index.isin(parent_found) & notnull(par_look_enf['mere'])
        match2 = Matching(enf_look_par.ix[cond2_enf, var_match], 
                          par_look_enf.ix[cond2_par, var_match], score)
        parent_found2 = match2.evaluate(orderby=None, method='cells')
        ind.ix[parent_found2.index, ['mere']] = par_look_enf.ix[parent_found2, ['mere']]        
        
        #étape 3 : seulement père vivant
        enf_look_par.ix[parent_found2.index, ['pere','mere']] = par_look_enf.ix[parent_found2, ['pere','mere']]
        cond3_enf = (~notnull(enf_look_par['pere'])) & (enf_look_par['per1e'] == 2)
        cond3_par = ~par_look_enf.index.isin(parent_found) & notnull(par_look_enf['pere'])
        
        # TODO: changer le score pour avoir un lien entre pere et mere plus évident
        match3 = Matching(enf_look_par.ix[cond3_enf, var_match], 
                          par_look_enf.ix[cond3_par, var_match], score)
        parent_found3 = match3.evaluate(orderby=None, method='cells')
        ind.ix[parent_found3.index, ['pere']] = par_look_enf.ix[parent_found3, ['pere']]               
        
        self.ind = minimal_dtype(ind)
        self.drop_variable({'ind':['enf','per1e','mer1e','gpar'] + ['jepnais','jemnais','jemprof']})
    
    def match_couple_hdom(self):
        '''
        Certaines personnes se déclarent en couple avec quelqu'un ne vivant pas au domicile, on les reconstruit ici. 
        Cette étape peut s'assimiler à de la fermeture de l'échantillon.
        On sélectionne les individus qui se déclarent en couple avec quelqu'un hors du domicile.
        On match mariés,pacsé d'un côté et sans contrat de l'autre. Dit autrement, si on ne trouve pas de partenaire à une personne mariée ou pacsé on change son statut de couple.
        Comme pour les liens parents-enfants, on néglige ici la possibilité que le conjoint soit hors champ (étrange, prison, casernes, etc).
        Calcul aussi la variable ind['nb_enf']
        '''
        ind = self.ind  
        couple_hdom = ind['couple']==2
        # ind[couple_hdom].groupby(['etamatri','sexe'])
        # vu leur nombre, on regroupe pacsés et mariés dans le même sac
        ind.ix[(couple_hdom) & (ind['civilstate']==5),  'civilstate'] = 2
        # note que du coup, on cherche un partenaire de pacs parmi le sexe opposé. Il y a une petite par technique là dedans qui fait qu'on
        # ne gère pas les couples homosexuels
        
        #pour avoir un age plus "continu" sans gap d'une année de naissance à l'autre
        age = self.survey_date/100 - ind['anais']
        ind['age'] = (12*age + 11 - ind['mnais'])/12
                
        ## nb d'enfant
        nb_enf_mere= ind.groupby('mere').size()
        nb_enf_pere = ind.groupby('pere').size()
        enf_tot = pd.concat([nb_enf_mere, nb_enf_pere], axis=1)
        enf_tot = enf_tot.sum(axis=1)
        #comme enf_tot a le bon index on fait
        ind['nb_enf'] = enf_tot
        ind['nb_enf'] = ind['nb_enf'].fillna(0)     
        men_contrat = couple_hdom & (ind['civilstate'].isin([2,5])) & (~ind['sexe'])
        women_contrat = couple_hdom & (ind['civilstate'].isin([2,5])) & (ind['sexe'])
        men_libre = couple_hdom & (~ind['civilstate'].isin([2,5])) & (~ind['sexe'])
        women_libre = couple_hdom & (~ind['civilstate'].isin([2,5])) & (ind['sexe'])   
        
       
        var_match = ['age','findet','nb_enf'] #,'classif','dip6'
        score = "- 0.4893 * other.age + 0.0131 * other.age **2 - 0.0001 * other.age **3 "\
                 " + 0.0467 * (other.age - age)  - 0.0189 * (other.age - age) **2 + 0.0003 * (other.age - age) **3 " \
                 " + 0.05   * (other.findet - findet) - 0.5 * (other.nb_enf - nb_enf) **2 "
         
        match_contrat = Matching(ind.ix[women_contrat, var_match], ind.ix[men_contrat, var_match], score)
        match_found = match_contrat.evaluate(orderby=None, method='cells')
        ind.ix[match_found.values,'conj'] =  match_found.index
        ind.ix[match_found.index,'conj'] =  match_found.values

        match_libre = Matching(ind.ix[women_libre, var_match], ind.ix[men_libre, var_match], score)
        match_found = match_libre.evaluate(orderby=None, method='cells')
        ind.ix[match_found.values,'conj'] =  match_found.index
        ind.ix[match_found.index,'conj'] =  match_found.values
        ind.ix[men_libre & ~notnull(ind['conj']),['civilstate','couple']] =  [1,3]
        ind.ix[women_libre & ~notnull(ind['conj']),['civilstate','couple']] =  [1,3]  
    
        #on corrige là, les innocents qui se disent mariés et pas en couple.  
        ind.ix[ind['civilstate'].isin([2,5]) & ~notnull(ind['conj']),['civilstate','couple']] =  [3,3] 
           
        self.ind = ind   
        self.drop_variable({'ind':['couple']})        
    
        

if __name__ == '__main__':
    
    import time
    start_t = time.time()
    data = Patrimoine()

    data.load()
    data.format_initial()
    data.drop_variable()
    # drop_variable() doit tourner avant table_initial() car on fait comme si diplome par exemple n'existait pas
    # plus généralement, on aurait un problème avec les variables qui sont renommées.
    data.table_initial()
    data.conjoint()
    data.enfants()
    data.creation_par_look_enf()
    data.expand_data(seuil=900)
    data.matching_par_enf() 
    data.match_couple_hdom()
    data.creation_foy()   
    data.mise_au_format()
    data.var_sup()  
    data.store_to_liam()
    print "temps de calcul : ", time.clock() - start_t, 's'
    print "nombre d'individus : ", len(data.ind) 


    # des petites verifs.
    ind = data.ind
    ind['en_couple'] = ind['conj']>-1 
    test = ind['conj']>-1   
    print ind.groupby(['civilstate','en_couple']).size()
    