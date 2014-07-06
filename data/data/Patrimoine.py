# -*- coding:utf-8 -*-
'''
Created on 2 août 2013

@author: a.eidelman

'''

# 1- Importation des classes/librairies/tables nécessaires à l'importation des données de l'enquête Patrimoine
from data.DataTil import DataTil
from data.utils.matching import Matching
from data.utils.utils import recode, minimal_dtype
from pgm.CONFIG import path_data_patr

import numpy as np
from pandas import merge, DataFrame, Series, concat, read_csv
import pdb

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
        self.methods_order = ['load','correction_initial','drop_variable','format_initial','conjoint','enfants',
                      'expand_data','creation_child_out_of_house','matching_par_enf','matching_couple_hdom',
                      'creation_foy','mise_au_format','var_sup','store_to_liam']
    
# drop_variable() doit tourner avant table_initial() car on aurait un problème avec les variables qui sont renommées.
# explication de l'ordre en partant de la fin, besoin des couples pour et des liens parents enfants pour les mariages.
# Ces liens ne peuvent se faire qu'après la dupplication pour pouvoir avoir le bon nombre de parents et de bons matchs
# La dupplication, c'est mieux si elle se fait après la création de child_out_of_house, plutôt que de chercher à créer child_out_of_house à partir de la base étendue
# Pour les enfants, on cherche leur parent un peu en fonction de s'ils sont en couple ou non, ça doit donc touner après conjoint.
# Ensuite, c'est assez évident que le format initial et le drop_variable doivent se faire le plus tôt possible
# on choisit de faire le drop avant le format intitial, on pourrait faire l'inverse en étant vigilant sur les noms

    def _output_name(self):
        #return 'Patrimoine_' + str(self.size) + '.h5'
        if self.seuil is None:
            return 'Patrimoine_0.h5' # + survey_date
        else: 
            return 'Patrimoine_' + str(self.seuil) +'.h5' # + survey_date 
            
    def load(self):
        print "début de l'importation des données"
        ind = read_csv(path_data_patr + 'individu.csv')
        men = read_csv(path_data_patr + 'menage.csv')
              
        #check parce qu'on a un probleme dans l'import au niveau de identmen(pas au format numérique)
        men['identmen'] = men['identmen'].apply(int)
        ind['identmen'] = ind['identmen'].apply(int)
        print "Nombre de ménages dans l'enquête initiale : " + str(len(ind['identmen'].drop_duplicates()))
        print "Nombre d'individus dans l'enquête initiale : " + str(len(ind['identind'].drop_duplicates()))
        self.men = men
        self.ind = ind
        print "fin de l'importation des données"
        
    def correction_initial(self):
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
            cond1 = ind['cyact2'].notnull() & ind['cyact1'].isnull() & \
                ((ind['cydeb1']==ind['cydeb2']) | (ind['cydeb1'] > ind['cydeb2']) | (ind['cydeb1']==(ind['cydeb2']-1)))
            cond1[8297] = True
            
            ind['modif'][cond1] = "decal act"
            # on decale tout de 1 à gauche en espérant que ça résout le problème
            for k in range(1,16):
                var_k = [x + str(k) for x in var]
                var_k1 = [x + str(k+1) for x in var]
                ind.ix[cond1, var_k] = ind.ix[cond1, var_k1]
                
            
            # si le probleme n'est pas resolu, le souci était sur cycact seulement, on met une valeur
            cond1 = ind['cyact2'].notnull() & ind['cyact1'].isnull() & \
                ((ind['cydeb1']==ind['cydeb2']) | (ind['cydeb1'] > ind['cydeb2']) | (ind['cydeb1']==(ind['cydeb2']-1)))
            ind['modif'][cond1] = "cyact1 manq"
            ind.ix[ cond1 & (ind['cyact2'] != 4),'cyact1'] = 4
            ind.ix[ cond1 & (ind['cyact2'] == 4),'cyact1'] = 2  
            
            cond2 = ind['cydeb1'].isnull() & ( ind['cyact1'].notnull() | ind['cyact2'].notnull())
            ind['modif'][cond1] = "jeact ou anfinetu manq"
            ind.ix[ cond2,'cydeb1'] =  ind.ix[ cond2,['jeactif','anfinetu']].max(axis=1)
            # quand l'ordre des dates n'est pas le bon on fait l'hypothèse que c'est la première date entre
            #anfinetu et jeactif qu'il faut prendre en non pas l'autre
            cond2 = ind['cydeb1'] > ind['cydeb2']
            ind.ix[ cond2,'cydeb1'] = ind.ix[ cond2,['jeactif','anfinetu']].min(axis=1)
            return ind
            
        def _correction_etamatri(ind):
            '''
            Création de pacs comme un modalité de l'union (5) 
            '''
            ind.loc[ind['pacs']==1,'etamatri'] = 5 
            ind = ind.rename(columns={'etamatri': 'civilstate', 'identind': 'id'})
            ind['civilstate'].replace([2,1,4,3,5],[1,2,3,4,5], inplace=True)
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
        ind = self.ind
        
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
            all = ind.columns.tolist()
            #liste noire
            parent_prop = [x for x in all if x[:6]=='jepro_']
            jeunesse_grave = [x for x in all if x[:6]=='jepro_']
            jeunesse = [x for x in all if x[:7]=='jegrave']
            black_list = jeunesse_grave + parent_prop  + diplom
            #liste blanche
            info_pers = ['anais','mnais','sexe','dip14']
            famille = ['couple','lienpref','enf','civilstate','pacs','grandpar','per1e','mer1e']
            jobmarket = ['statut','situa','preret','classif', 'cs42']
            info_parent = ['jepnais','jemnais','jemprof']
            carriere =  [x for x in all if x[:2]=='cy' and x not in ['cyder', 'cysubj']] + ['jeactif', 'anfinetu','prodep']
            revenus = ["zsalaires_i", "zchomage_i", "zpenalir_i", "zretraites_i", "cyder", "duree"]           
            
            white_list = ['identmen','noi', 'pond', 'id'] + info_pers + famille + jobmarket + carriere + info_parent + revenus 
            
            if option=='white':
                dict_to_drop['ind'] = [x for x in all if x not in white_list]
                
            else:
                dict_to_drop['ind'] = black_list            
            
        DataTil.drop_variable(self, dict_to_drop, option)
    
    def format_initial(self):
        men = self.men      
        ind = self.ind 
        men.index = range(10, len(men)+ 10)
        men['id'] = men.index
        #passage de ind à men, variable ind['men']
        idmen = men[['id', 'identmen']].rename(columns = {'id': 'men'})
        verif_match = len(ind)
        ind = merge(ind, idmen, on = 'identmen')
        if len(ind) != verif_match:
            raise Exception("On a perdu le lien entre ind et men via identmen")
        ind['id'] = ind.index
                # Pour avoir un age plus "continu" sans gap d'une année de naissance à l'autre
        age = self.survey_date/100 - ind['anais']
        ind['age'] = (12*age + 11 - ind['mnais'])/12
        
        dict_rename = {"zsalaires_i":"sali", "zchomage_i":"choi",
        "zpenalir_i":"alr", "zretraites_i":"rsti", "anfinetu":"findet",
        "cyder":"anc", "duree":"xpr"}
        ind = ind.rename(columns=dict_rename)
        
        def _recode_sexe(sexe):
            if sexe.max() == 2:
                sexe.replace(1,0, inplace=True)
                sexe.replace(2,1, inplace=True)
            return sexe

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
            ind.loc[ind['preret']==1, 'workstate'] = 9
            # precision AVPF
            #TODO: "vous pouverz bénéficier de l'AVPF si vous n'exercez aucune activité 
            # professionnelle (ou seulement à temps partiel) et avez 
            # la charge d'une personne handicapée (enfant de moins de 20 ans ou adulte).
            # Pour l'instant, on fait ça parce que ça colle avec PensIPP mais il faudrait faire mieux.
            #en particulier c'est de la législation l'avpf finalement.
            cond =  (men['paje']==1) | (men['complfam']==1) | (men['allocpar']==1) | (men['asf']==1)
            avpf = men.ix[cond,:].index.values + 1 
            ind.loc[(ind['men'].isin(avpf)) & (ind['workstate'].isin([1,2])), 'workstate'] = 8
            # public, privé, indépendant
            ind.loc[ind['statut'].isin([1,2]), 'workstate'] = 5
            ind.loc[ind['statut']==7, 'workstate'] =  7
            # cadre, non cadre
            ind.loc[(ind['classif']==6)  & (ind['workstate']==5), 'workstate'] = 6
            ind.loc[(ind['classif']==7)  & (ind['workstate']==3), 'workstate'] = 4
            #retraite
            ind.loc[(ind['anais'] < 2009-64)  & (ind['workstate']==1), 'workstate'] = 10
            # print ind.groupby(['workstate','statut']).size()
            # print ind.groupby(['workstate','situa']).size()
            return ind['workstate']
        
        def _work_on_couple(self):
            # 1- Personne se déclarant mariées/pacsées mais pas en couples
            statu_mari = ind[['men','couple','civilstate','pacs','lienpref']].fillna(-1)
            # (a) Si deux mariés/pacsés pas en couple vivent dans le même ménage -> en couple (2)
            prob_couple = (ind['civilstate'].isin([1,5])) & (ind['couple'] == 3) 
            if sum(prob_couple) != 0 :
                statu_marit = statu_mari[prob_couple]
                many_by_men = statu_marit['men'].value_counts() > 1
                many_by_men = statu_marit['men'].value_counts()[many_by_men]
                prob_couple_ident = statu_marit[statu_marit['men'].isin(many_by_men.index.values.tolist())]
                ind.loc[prob_couple_ident.index,'couple'] = 1
                
            # (b) si un marié/pacsé pas en couple est conjoint de la personne de ref -> en couple (0)
            prob_couple = (ind['civilstate'].isin([1,5])) & (ind['couple'] == 3) & (ind['lienpref'] == 1)
            ind.loc[prob_couple_ident.index,'couple'] = 1
            
            # (c) si un marié/pacsé pas en couple est ref et l'unique conjoint déclaré dans le ménage se dit en couple -> en couple (0)
            prob_couple = (ind['civilstate'].isin([1,5])) & (ind['couple'] == 3) & (ind['lienpref'] == 0)
            men_conj = statu_mari[prob_couple]
            men_conj = statu_mari.loc[(statu_mari['men'].isin(men_conj['men'].values))& (statu_mari['lienpref'] == 1), 'men' ].value_counts() == 1
            ind.loc[prob_couple_ident.index,'couple'] = 1

            
            # 2 - Check présence d'un conjoint dans le ménage si couple=1 et lienpref in 0,1
            conj = ind.loc[ind['couple']==1,['men','lienpref','id']]
#             conj['lienpref'].value_counts()
            # pref signifie "personne de reference"
            pref0 = conj.loc[conj['lienpref']==0,'men']
            pref1 = conj.loc[conj['lienpref']==1,'men']
            assert sum(~pref1.isin(pref0)) == 0
            conj_hdom = pref0[~pref0.isin(pref1)]
            ind.loc[conj_hdom.index,'couple'] = 2
            
            # Présence du fils/fille de la personne de ref si déclaration belle-fille/beau-fils
            pref2 = conj.loc[conj['lienpref']==2,'men']
            pref31 = conj.loc[conj['lienpref']==31,'men']
            assert sum(~pref31.isin(pref2)) == 0          
            manque_conj = pref2[~pref2.isin(pref31)]
            ind.loc[manque_conj.index,'couple'] = 2
   
            return ind
        
        ind['sexe'] = _recode_sexe(ind['sexe'])
        ind['workstate'] = _work_on_workstate(ind)
        ind['workstate'] = ind['workstate'].fillna(-1).astype(np.int8)
        #work in findet
        ind['findet'][ind['findet']==0] = np.nan
        ind['findet'] = ind['findet'] - ind['anais']
        ind['findet'][ind['findet'].isnull()] = np.nan
        
        ind = _work_on_couple(ind)
        self.men = men
        self.ind = ind
        self.drop_variable({'men':['identmen','paje','complfam','allocpar','asf'], 'ind':['identmen','preret']})
        
        # Sorties au format minimal
        # format minimal
        ind = self.ind.fillna(-1).replace(-1,np.nan)
        ind = minimal_dtype(ind)
        self.ind = ind
        
        
        
    def conjoint(self):
        '''
        Calcul de l'identifiant du conjoint et vérifie que les conjoint sont bien reciproques 
        '''     
        print ("Travail sur les conjoints")
        ind = self.ind
        conj = ind.loc[ind['couple']==1, ['men','lienpref','id','civilstate']]
        print "Nombre d'individus se déclarant en couple dans la table initiale : ", len(conj)
        # Personnes en couple vivant dans le même ménage (8230)
        conj.loc[conj['lienpref']==1, 'lienpref'] = 0
        # Couples fils/belle-fille (ou recip.) vivant ds ménage parents (18)
        # Rq : il y a aussi 39 couples vivant chez leur enfant (35 ref + 4conj)
        conj.loc[conj['lienpref']==31, 'lienpref'] = 2
        conj.loc[conj['lienpref']==32, 'lienpref'] = 3
        # frères, soeurs et liens indéterminés (4)
        conj.loc[conj['lienpref']==50, 'lienpref'] = 10
        conj2 = merge(conj, conj, on=['men','lienpref'], how= 'outer')
        conj2 = conj2[conj2['id_x'] != conj2['id_y']]
        assert len(conj2) == len(conj)
        conj = conj2
        test = conj.groupby(['men','lienpref']).size()
        assert (max(test)==2) and (min(test)==2)
        couple = conj.groupby('id_x')
        for id, potential in couple:
            if len(potential) == 1:
                conj.loc[conj['id_x']==id, 'id_y'] = potential['id_y'].values[0]
            else:
                pdb.set_trace()
                # TODO: pas de probleme, bizarre
        conj = conj.rename(columns={'id_x': 'id', 'id_y':'conj'})
        ind = merge(ind, conj[['id','conj']], on='id', how='left')
        test_conj = merge(ind[['conj','id']],ind[['conj','id']],
                             left_on='id',right_on='conj').astype(int)
        test_conj = test_conj.loc[test_conj['id_x']< test_conj['conj_x']]
        self.ind = ind
        assert sum(test_conj['id_y'] != test_conj['conj_x']) == 0
        print ("Fin du travail sur les conjoints")

    def enfants(self):   
        '''
        Calcule l'identifiant des parents 
        Rq : la variable enf précise le lien de parenté entre l'enfant et les personnes de ref du ménage
        1 : enf de pref et de son conj, 2:enf de pref seulement, 3 : enf de conj seulement, 4 : conj de l'enf de pref/conj
        '''    
        ind = self.ind
        enf = ind.loc[ ind['enf'] != -1, ['men','lienpref','id','enf']]
        info_par = ind.loc[:, ['men','lienpref','id', 'sexe']].rename(columns = {'sexe': 'sexe_par'})
        
        # [0] Enfants de la personne de référence
        enf0 = enf[(enf['enf'].isin([1,2]))].drop('enf', axis = 1)
        enf0['lienpref'] = 0
        enf0 = merge(enf0, info_par, on=['men','lienpref'], how='left', suffixes=('_enf', '_par'))

        # [1] Enfants du conjoint de la personne de référence
        enf1 = enf[(enf['enf'].isin([1,3]))].drop('enf', axis = 1)
        enf1['lienpref'] = 1
        enf1 = merge(enf1, info_par, on=['men','lienpref'], how='left', suffixes=('_enf', '_par'))
        enf_tot = enf0.append(enf1)
        
        # [2] Parents à charge
        gpar0 = ind.loc[ (ind['lienpref'] ==3), ['men','lienpref','id']]
        gpar0['lienpref'] = 0
        gpar0 = merge(gpar0, info_par, on=['men','lienpref'], how='left', suffixes=('_par', '_enf'))
        #enf_tot = enf_tot.append(gpar0)

        gpar1 = ind.loc[ (ind['lienpref'] == 32), ['men','lienpref','id']]
        gpar1['lienpref'] = 1
        gpar1 = merge(gpar1, info_par, on=['men','lienpref'], how='left', suffixes=('_par', '_enf'))
        gpar = gpar0.append(gpar1)
        # TODO: pas normal que plusieurs personnes du même sexe se déclare parent de la personne de ref
        gpar = gpar.drop_duplicates(['id_enf', 'sexe_par'])
        enf_tot = enf_tot.append(gpar)
        
        # [3] Petits-enfants : on cherche les enfants de la personne de référence ou de son conjoint et on tente de les associer 
        # à des petits enfants (lienpref=21)
        # TODO: en toute rigueur, il faudrait garder un lien si on ne trouve pas les parents pour l'envoyer dans le registre...
        # et savoir que ce sont les petites enfants (pour l'héritage par exemple)
        par = ind.loc[ (ind['enf'] != -1) & ind['enf'].isin([1,2,3]), ['men','lienpref','id', 'sexe', 'age']].rename(columns = {'sexe': 'sexe_par', 'age' : 'age_par'})
        par['lienpref'] = 21
        par = merge(par, ind[['men','lienpref','id']], on=['men','lienpref'], how='inner', suffixes=('_par', '_enf'))
        enf3 = par.drop_duplicates(['id_enf', 'sexe_par'])
        for enf in par['id_enf']:
            parent = par[par['id_enf']== enf]
            if len(parent) > 1:
                pot_mother = parent[parent['sexe_par']==1].sort('age_par')
                if len(pot_mother) > 1:
                    enf3.loc[enf3['id_enf']==enf] =  pot_mother.drop_duplicates('id_enf', take_last=True)
                pot_father = parent[parent['sexe_par']==0].sort('age_par')
                if len(pot_father) > 1:
                    enf3.loc[enf3['id_enf']==enf] =  pot_father.drop_duplicates('id_enf', take_last=True)
        enf_tot = enf_tot.append(enf3)

        enf = DataFrame(enf_tot[['sexe_par', 'id_enf', 'id_par']].fillna(-1), dtype = np.int32)
        enf = enf[enf['id_enf'] != -1]

        # [4] Création des variables 'pere' et 'mere'
        ind.index = ind['id']
        for par in ['pere', 'mere'] :
            ind[par] = -1
            if par == 'pere':
                sexe = 0
            else:
                sexe = 1  
            ind[par][enf.loc[(enf['sexe_par'] == sexe),'id_enf'].values] = enf.loc[(enf['sexe_par'] == sexe),'id_par'].values
            
        print 'Nombre de mineurs sans parents : ', sum((ind['pere'] == -1) & (ind['mere']==-1) & (ind['age']<18))
        par_mineur = ind.loc[(ind['age']<18), 'id']
        assert sum((ind['pere'].isin(par_mineur)) | (ind['mere'].isin(par_mineur))) == 0
        #-> Cas exotiques : bcp de lien indéterminé + frères/soeur : ind[(ind['pere'] == -1) & (ind['mere']==-1) & (ind['age']<18)].to_csv('mineurs.csv')
        self.ind = ind
        
    def creation_child_out_of_house(self):
        '''
        Renvoie une table qui doit se lire comme étant les enfants hors foyer déclarer par le ménage. 
        On marque les infos que l'on connait sur ces enfants.
        On ajouter les infos sur leurs parents (qui sont donc des membres du ménages)
 
        On fera ensuite un matching avec les enfants qui ne vivent pas avec leur parent alors que ceux-ci sont vivants.
        '''
        men = self.men      
        ind = self.ind
        #création brute de enfants hors du domicile
        child_out_of_house = DataFrame()
        for k in range(1,13):
            k = str(k)
            # hodln : lien de parenté
            var_hod = ['hodln','hodsex','hodan','hodco','hodip','hodenf',
                       'hodemp','hodcho','hodpri','hodniv']
            var_hod_rename=['hodln','sexe','anais','couple','dip6','nb_enf',
                            'hodemp','hodcho','hodpri','hodniv']
            var_hod_k = [var + k for var in var_hod]
            temp = men.loc[men[var_hod_k[0]].notnull(), ['id']+ var_hod_k]
            dict_rename = {'id': 'men'}
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
            temp.loc[prive, 'classif'] = temp.loc[prive, 'hodpri']
            temp.loc[~prive, 'classif'] = temp.loc[~prive, 'hodniv']
        
            child_out_of_house = child_out_of_house.append(temp)
            len_ini = len(child_out_of_house)
            
        var_parent = ["id","men","sexe","anais","cs42","grandpar"]
        #Si les parents disent qu'ils ont eux-même des parents vivants, c'est que les grands parents de leurs enfants sont vivants !
        ind['grandpar'] = ind['per1e'].isin([1,2]) | ind['mer1e'].isin([1,2])
        
        #info sur les personnes de référence et leur conjoint
        info_pr = ind.loc[(ind['lienpref']==0), var_parent]
        info_cj = ind.loc[(ind['lienpref']==1), var_parent]   
                 
        # répartition entre père et mère en fonction du sexe...
        info_pr_pere = info_pr[info_pr['sexe']==0].rename(columns={'id': 'pere', 'anais': 'jepnais', 'grandpar': 'grandpar_pat',
                                                                    'cs42': 'jepprof', 'sexe': 'to_delete'})
        info_cj_pere = info_cj[info_cj['sexe']==0].rename(columns={'id': 'pere', 'anais': 'jepnais', 'grandpar': 'grandpar_pat',
                                                                    'cs42': 'jepprof', 'sexe': 'to_delete'})
        #... puis les meres
        info_pr_mere = info_pr[info_pr['sexe']==1].rename(columns={'id': 'mere', 'anais': 'jemnais', 'grandpar': 'grandpar_mat',
                                                                    'cs42': 'jemprof', 'sexe': 'to_delete'}) 
        info_cj_mere = info_cj[info_cj['sexe']==1].rename(columns={'id': 'mere', 'anais': 'jemnais', 'grandpar': 'grandpar_mat',
                                                                    'cs42': 'jemprof', 'sexe': 'to_delete'}) 
        info_pere = info_pr_pere.append(info_cj_pere)
        info_mere = info_pr_mere.append(info_cj_mere)
        
        # A qui est l'enfant ? 
        ## aux deux
        cond1 = child_out_of_house['hodln']==1
        child_out_of_house1 = merge(child_out_of_house[cond1], info_pere, on='men', how='left')
        child_out_of_house1 = merge(child_out_of_house1, info_mere, on='men', how = 'left')
        # à la pref
        cond2 = child_out_of_house['hodln']==2
        child_out_of_house2 = merge(child_out_of_house[cond2], info_pr_pere, on='men', how='left')
        child_out_of_house2 = merge(child_out_of_house2, info_pr_mere, on='men', how = 'left')
        # au conjoint
        cond3 = child_out_of_house['hodln']==3
        child_out_of_house3 = merge(child_out_of_house[cond3], info_cj_pere, on='men', how='left')
        child_out_of_house3 = merge(child_out_of_house3, info_cj_mere, on='men', how = 'left') 

        temp = child_out_of_house1.append(child_out_of_house2, ignore_index = True)
        temp = temp.append(child_out_of_house3, ignore_index = True) 
        # len(temp) = len(child_out_of_house) - 4 #deux personnes du même sexe qu'on a écrasé a priori.
        child_out_of_house = temp
        # TODO: il y a des ménages avec hodln = 1 et qui pourtant n'ont pas deux membres (à moins qu'ils aient le même sexe. 
        #child_out_of_house = child_out_of_house.drop(['hodcho','hodemp','hodniv','hodpri','to_delete_x','to_delete_y','jepprof'],axis=1)
        
        assert child_out_of_house['jemnais'].max() < 2010 - 18
        assert child_out_of_house['jepnais'].max() < 2010 - 18
        self.child_out_of_house = child_out_of_house.fillna(-1)

    def matching_par_enf(self):
        '''
        Matching des parents et des enfants hors du domicile
        '''
        ind = self.ind
        ind = ind.fillna(-1)
        ind.index = ind['id']
        child_out_of_house = self.child_out_of_house
        ## info sur les parents hors du domicile des enfants
        cond_enf_look_par = (ind['per1e']==2) | (ind['mer1e']==2)
        enf_look_par = ind[cond_enf_look_par]
        # Remarque: avant on mettait à zéro les valeurs quand on ne cherche pas le parent, maintenant
        # on part du principe qu'on fait les choses assez minutieusement                                           
        
        recode(enf_look_par, 'dip14', 'dip6', [[30,5], [41,4], [43,3], [50,2], [60,1]] , method='geq')
        recode(enf_look_par, 'classif', 'classif2', [ [[1,2,3],4], [[4,5],2], [[6,7],1], [[8,9], 3], [[10],0]], method='isin')
        enf_look_par.loc[:,'classif'] = enf_look_par.loc[:,'classif2']

        ## nb d'enfant
        # -- Au sein du domicile
        nb_enf_mere_dom = ind.groupby('mere').size()
        nb_enf_pere_dom= ind.groupby('pere').size()
        # On assemble le nombre d'enfants pour les peres et meres en enlevant les manquantes ( = -1)
        enf_tot_dom = concat([nb_enf_mere_dom, nb_enf_pere_dom], axis=0)
        enf_tot_dom = enf_tot_dom.drop([-1])
        
        # -- Hors domicile
        nb_enf_mere_hdom = child_out_of_house.groupby('mere').size()
        nb_enf_pere_hdom = child_out_of_house.groupby('pere').size()
        enf_tot_hdom = concat([nb_enf_mere_hdom, nb_enf_pere_hdom], axis=0)
        enf_tot_hdom = enf_tot_hdom.drop([-1])
        
        enf_tot = concat([enf_tot_dom, enf_tot_hdom], axis = 1).fillna(0)
        enf_tot = enf_tot[0] + enf_tot[1]
        # Sélection des parents ayant des enfants (enf_tot) à qui on veut associer des parents (enf_look_par)
        enf_tot = enf_tot.ix[enf_tot.index.isin(enf_look_par.index)].astype(int)
  
        enf_look_par.index = enf_look_par['id']
        enf_look_par['nb_enf'] = 0
        enf_look_par['nb_enf'][enf_tot.index.values] = enf_tot

        #Note: Attention le score ne peut pas avoir n'importe quelle forme, il faut des espaces devant les mots, à la limite une parenthèse
        var_match = ['jepnais','situa','nb_enf','anais','classif','couple','dip6', 'jemnais','jemprof','sexe']
        #TODO: gerer les valeurs nulles, pour l'instant c'est très moche
        #TODO: avoir une bonne distance
        score = "- 1 * (other.anais - anais) **2 - 1.0 * (other.situa - situa) **2 - 0.5 * (other.sexe - sexe) **2 - 1.0 * (other.dip6 - dip6) \
         **2 - 1.0 * (other.nb_enf - nb_enf) **2"

        # etape1 : deux parents vivants
        cond1_enf = (enf_look_par['per1e'] == 2) & (enf_look_par['mer1e'] == 2)
        cond1_par = (child_out_of_house['pere'] != -1) & (child_out_of_house['mere'] != -1)
        # TODO: si on fait les modif de variables plus tôt, on peut mettre directement child_out_of_house1
        
        #à cause du append plus haut, on prend en fait ici les premiers de child_out_of_house
        match1 = Matching(enf_look_par.ix[cond1_enf, var_match], 
                          child_out_of_house.ix[cond1_par, var_match], score)
        parent_found = match1.evaluate(orderby=None, method='cells')
        ind.ix[parent_found.index.values, ['pere','mere']] = child_out_of_house.ix[parent_found.values, ['pere','mere']]
         
        #etape 2 : seulement mère vivante
        enf_look_par.ix[parent_found.index, ['pere','mere']] = child_out_of_house.ix[parent_found, ['pere','mere']]
        cond2_enf = ((enf_look_par['mere'] == -1)) & (enf_look_par['mer1e'] == 2)
        cond2_par = ~child_out_of_house.index.isin(parent_found) & (child_out_of_house['mere'] != -1)
        match2 = Matching(enf_look_par.ix[cond2_enf, var_match], 
                          child_out_of_house.ix[cond2_par, var_match], score)
        parent_found2 = match2.evaluate(orderby=None, method='cells')
        ind.ix[parent_found2.index, ['mere']] = child_out_of_house.ix[parent_found2, ['mere']]        
        
        #étape 3 : seulement père vivant
        enf_look_par.ix[parent_found2.index, ['pere','mere']] = child_out_of_house.ix[parent_found2, ['pere','mere']]
        cond3_enf = ((enf_look_par['pere'] == -1)) & (enf_look_par['per1e'] == 2)
        cond3_par = ~child_out_of_house.index.isin(parent_found) & (child_out_of_house['pere'] != -1)
        
        # TODO: changer le score pour avoir un lien entre pere et mere plus évident
        match3 = Matching(enf_look_par.ix[cond3_enf, var_match], 
                          child_out_of_house.ix[cond3_par, var_match], score)
        parent_found3 = match3.evaluate(orderby=None, method='cells')
        ind.ix[parent_found3.index, ['pere']] = child_out_of_house.ix[parent_found3, ['pere']]               

        self.ind = minimal_dtype(ind)
        all = self.men.columns.tolist()
        enfants_hdom = [x for x in all if x[:3]=='hod']
        self.drop_variable({'ind':['enf','per1e','mer1e','grandpar'] + ['jepnais','jemnais','jemprof'], 'men':enfants_hdom})
    
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
        # vu leur nombre, on regroupe pacsés et mariés dans le même sac
        ind.loc[(couple_hdom) & (ind['civilstate']==5), 'civilstate'] = 1
        # note que du coup, on cherche un partenaire de pacs parmi le sexe opposé. Il y a une petite par technique là dedans qui fait qu'on
        # ne gère pas les couples homosexuels
                
        ## nb d'enfant
        ind.index = ind['id']
        nb_enf_mere = DataFrame(ind.groupby('mere').size(), columns = ['nb_enf'])
        nb_enf_mere['id'] = nb_enf_mere.index.values
        nb_enf_pere = DataFrame(ind.groupby('pere').size(), columns = ['nb_enf'])
        nb_enf_pere['id'] = nb_enf_pere.index
        # On assemble le nombre d'enfants pour les peres et meres en enlevant les manquantes ( = -1)
        enf_tot = nb_enf_mere[nb_enf_mere['id'] != -1].append( nb_enf_pere[nb_enf_pere['id'] != -1]).astype(int)
        ind['nb_enf'] = 0
        ind['nb_enf'][enf_tot['id'].values] = enf_tot['nb_enf']
        
        men_contrat = couple_hdom & (ind['civilstate'].isin([1,5])) & (ind['sexe']==0)
        women_contrat = couple_hdom & (ind['civilstate'].isin([1,5])) & (ind['sexe']==1)
        men_libre = couple_hdom & (~ind['civilstate'].isin([1,5])) & (ind['sexe']==0)
        women_libre = couple_hdom & (~ind['civilstate'].isin([1,5])) & (ind['sexe']==1)   
        
        var_match = ['age','findet','nb_enf'] #,'classif','dip6'
        score = "- 0.4893 * other.age + 0.0131 * other.age **2 - 0.0001 * other.age **3 "\
                 " + 0.0467 * (other.age - age)  - 0.0189 * (other.age - age) **2 + 0.0003 * (other.age - age) **3 " \
                 " + 0.05   * (other.findet - findet) - 0.5 * (other.nb_enf - nb_enf) **2 "
         
        match_contrat = Matching(ind.loc[women_contrat, var_match], ind.loc[men_contrat, var_match], score)
        match_found = match_contrat.evaluate(orderby=None, method='cells')
        ind.ix[match_found.values,'conj'] =  match_found.index
        ind.ix[match_found.index,'conj'] =  match_found.values

        match_libre = Matching(ind.ix[women_libre, var_match], ind.ix[men_libre, var_match], score)
        match_found = match_libre.evaluate(orderby=None, method='cells')
        ind.loc[match_found.values,'conj'] =  match_found.index
        ind.loc[match_found.index,'conj'] =  match_found.values
        ind.loc[men_libre & ind['conj'].isnull(),['civilstate','couple']] =  [2,3]
        ind.loc[women_libre & ind['conj'].isnull(),['civilstate','couple']] =  [2,3]  
    
        self.ind = ind   
        self.drop_variable({'ind':['couple']})        


if __name__ == '__main__':
    
    import time
    start_t = time.time()
    data = Patrimoine()
    data.load()
    data.correction_initial()
    data.drop_variable()
    # drop_variable() doit tourner avant table_initial() car on fait comme si diplome par exemple n'existait pas
    # plus généralement, on aurait un problème avec les variables qui sont renommées.
    data.format_initial()
    data.conjoint()
    data.check_conjoint(couple_hdom = True)
    data.enfants()
    data.expand_data(seuil=400)
    data.creation_child_out_of_house()
    data.matching_par_enf() 
    data.match_couple_hdom()
    data.check_conjoint(couple_hdom = False)
    data.creation_foy()   
    data.format_to_liam()
    data.final_check()
    data.store_to_liam()
    print "Temps de calcul : ", (time.time() - start_t), 's'
    print "Nombre d'individus de la table final : ", len(data.ind) 
    
    # des petites verifs finales 
    ind = data.ind
    ind['en_couple'] = ind['conj']>-1 
    test = ind['conj']>-1   
    print ind.groupby(['civilstate','en_couple']).size()
    