# -*- coding:utf-8 -*-
'''
Created on 22 juil. 2013
Alexis Eidelman
'''


from matching import Matching
from pgm.CONFIG import path_data_patr, path_til
import pandas as pd
from pandas import merge, notnull, DataFrame, Series
import pdb
import gc

print path_data_patr


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
        self.decl = None
        
    def lecture(self):
        print "début de l'importation des données"
        # pd.read_stata(path_data_patr + 'individu.dta')
        ind = pd.read_csv(path_data_patr + 'individu.csv')
        men = pd.read_csv(path_data_patr + 'menage.csv')
        print "fin de l'importation des données"
        
        #check parce qu'on a un proble dans l'import au niveau de identmen
        #TODO: not solved, see with read_stat in forcoming pandas release
        men['identmen'] = men['identmen'].apply(int)
        ind['identmen'] = ind['identmen'].apply(int)
        self.men = men
        self.ind = ind
        
    def correction(self):
        '''
        Fait des corrections (à partir de vérif écrit en R)
        Se met sur le champ des Antilles. 
        '''  
        men = self.men      
        ind = self.ind        
        # correction après utilisation des programmes verif (en R pour l'instant)
        # Note faire attention à la numérotation à partir de 0
        ind['cydeb1'] = ind.prodep
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


        ############### on supprime les antilles ##################
        # Pourquoi ? - elles n'ont pas les memes variables + l'appariemment EIR n'est pas possible
        # Pourquoi pas dès le début ? - Des raison historiques : si on remonte ca casse les numeros de
        # lignes utilises plus haut mais TODO:
        antilles = men.ix[ men['zeat'] == 0,'identmen'].copy()
        men = men[~men['identmen'].isin(antilles)]
        men = men.reset_index()
        men['id'] = men.index
        # on fusionne ind et men pour ne garder que les ind de men.
        #pref inutile pour l'isntant, ajouter d'autre plus tard
        ind = merge(men.ix[:,['identmen','pref']],ind, \
                        on='identmen', how='left')
        
        # travail sur le 'men'
        idmen = Series(ind['identmen'].unique())
        idmen = DataFrame(idmen)
        idmen['men'] = idmen.index
        idmen.columns = ['identmen', 'men']
        ind = merge(idmen, ind)
        ind['id'] = ind.index
        
        dict_rename = {"dip14":"diplome", "zsalaires_i":"sali", "zchomage_i":"choi",
                "zpenalir_i":"alr", "zretraites_i":"rsti", "agfinetu":"findet",
                "cyder":"anc", "duree":"xpr"}
        ind = ind.rename(columns=dict_rename)
        
        self.men = men
        self.ind = ind 
        
        
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
        enf4['id_1'] = Series()
        enf4['id_2'] = Series()
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
        
        enf['pere'] = Series()
        enf['pere'][enf['sexe']==1] = enf['id_1'][enf['sexe']==1] 
        enf['mere'] = Series()
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
    
        def workstate(ind):
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
            ind['workstate'] = Series()
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
            return ind['workstate']
            
        ind['workstate'] = workstate(ind)
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

    def creations_foy(self):
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
        
        ind['foy'] = Series()
        ind['foy'][vous] = range(sum(vous))
        spouse1 = spouse & ~decl & ~notnull(ind['foy'])
        ind['foy'][spouse] = ind.ix[spouse,['foy']]
        ind['foy'][children] = ind.ix[ind['pere'][children],['foy']]
        children = children & ~notnull(ind['foy'])
        ind['foy'][children] = ind.ix[ind['mere'][children],['foy']]
        print "le nombre de personne sans foyer est: ", sum(~notnull(ind['foy']))
        pdb.set_trace()
        #repartition des revenus du ménage par déclaration
        var_to_declar = ['zcsgcrds','zfoncier','zimpot', \
             'zpenaliv','zpenalir','zpsocm','zrevfin','pond']
        foy_men = men[var_to_declar]
        nb_foy_men = ind[vous].groupby('men').size()
        foy_men = foy_men.div(nb_foy_men,axis=0) 
        
        foy = merge(ind[['foy','men']],foy_men, left_on='men', right_index=True)
        foy['period'] = self.survey_date
        foy['vous'] = ind['id'][vous]
        foy = foy.reset_index()
        foy['id'] = foy.index
        print("fin de la creation des declarations")
        #### fin de declar
        self.men = men
        self.ind = ind
        self.foy = foy

    def lien_parent_enfant_hdom(self):
        '''
        Travail sur les liens parents-enfants. 
        On regarde d'abord les variables utiles pour le matching puis on le réalise
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
            temp = men.ix[notnull(men[var_hod_k[0]]), ['id','pond']+var_hod_k]
            dict_rename = {}
            for num_varname in range(len(var_hod_rename)):
                dict_rename[var_hod_k[num_varname]] = var_hod_rename[num_varname]
            temp = temp.rename(columns=dict_rename)
            temp['situa'] = Series()
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
        info_pr_pere = info_pr[info_pr['sexe']==1].rename(columns={'id':'pere', 'anais':'jepnais','gpar':'gparpat','cs42':'jepprof'})
        info_cj_pere = info_cj[info_cj['sexe']==1].rename(columns={'id':'pere', 'anais':'jepnais','gpar':'gparpat','cs42':'jepprof'})
        info_pere = info_pr_pere.append(info_cj_pere)
        
        cond1 = par_look_enf['hodln']==1
        cond2 = par_look_enf['hodln']==2
        cond3 = par_look_enf['hodln']==3
        par_look_enf1 = merge(par_look_enf[cond1], info_pere, left_on='id', right_on='men', how = 'left')
        par_look_enf2 = merge(par_look_enf[cond2], info_pr_pere, left_on='id', right_on='men', how = 'left')
        par_look_enf3 = merge(par_look_enf[cond3], info_cj_pere, left_on='id', right_on='men', how = 'left')
         
        # d'abord les peres puis les meres
        info_pr_mere = info_pr[info_pr['sexe']==2].rename(columns={'id':'mere', 'anais':'jemnais','gpar':'gparmat','cs42':'jemprof'}) 
        info_cj_mere = info_cj[info_cj['sexe']==2].rename(columns={'id':'mere', 'anais':'jemnais','gpar':'gparmat','cs42':'jemprof'}) 
        info_mere = info_pr_mere.append(info_cj_mere)

        par_look_enf1 = merge(par_look_enf1, info_mere, left_on='id', right_on='men', how = 'left')
        par_look_enf2 = merge(par_look_enf2, info_pr_mere, left_on='id', right_on='men', how = 'left')
        par_look_enf3 = merge(par_look_enf3, info_cj_mere, left_on='id', right_on='men', how = 'left')        
             
        par_look_enf =  par_look_enf1.append(par_look_enf2).append(par_look_enf3)  
        par_look_enf.index = range(len(par_look_enf))

        ## info sur les parents hors du domicile des enfants
        cond_enf_look_par = (ind['per1e']==2) | (ind['mer1e']==2)
        enf_look_par = ind[cond_enf_look_par]
        # Remarque: avant on mettait à zéro les valeurs quand on ne cherche pas le parent, maintenant
        # on part du principe qu'on fait les choses assez minutieusement
                
        enf_look_par['dip6'] = Series()
        enf_look_par['dip6'][enf_look_par['diplome']>=30] = 5
        enf_look_par['dip6'][enf_look_par['diplome']>=41] = 4
        enf_look_par['dip6'][enf_look_par['diplome']>=43] = 3
        enf_look_par['dip6'][enf_look_par['diplome']>=50] = 2
        enf_look_par['dip6'][enf_look_par['diplome']>=60] = 1
        
        enf_look_par['classif2'] = enf_look_par['classif']
        enf_look_par['classif2'][enf_look_par['classif'].isin([1,2,3])] = 4
        enf_look_par['classif2'][enf_look_par['classif'].isin([4,5])] = 2
        enf_look_par['classif2'][enf_look_par['classif'].isin([6,7])] = 1
        enf_look_par['classif2'][enf_look_par['classif'].isin([8,9])] = 3
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
        var_match = ['jepnais','jepprof','situa','nb_enf','anais','classif','couple','dip6', 'jemnais','jemprof','sexe']
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
        parent_found = match1.evaluate()
        ind.ix[parent_found.index, ['pere','mere']] = par_look_enf.ix[parent_found, ['pere','mere']]
        
        enf_look_par.ix[parent_found.index, ['pere','mere']] = par_look_enf.ix[parent_found, ['pere','mere']]
        cond2_enf = (~notnull(enf_look_par['mere'])) & (enf_look_par['mer1e'] == 2)
        cond2_par = ~par_look_enf.index.isin(parent_found) & notnull(par_look_enf['mere'])
        match2 = Matching(enf_look_par.ix[cond2_enf, var_match], 
                          par_look_enf.ix[cond2_par, var_match], score)
        parent_found2 = match2.evaluate()
        ind.ix[parent_found2.index, ['mere']] = par_look_enf.ix[parent_found2, ['mere']]        
        
        parent_found = parent_found.append(parent_found2, True)    
        enf_look_par.ix[parent_found2.index, ['pere','mere']] = par_look_enf.ix[parent_found2, ['pere','mere']]
        cond3_enf = (~notnull(enf_look_par['pere'])) & (enf_look_par['per1e'] == 2)
        cond3_par = ~par_look_enf.index.isin(parent_found) & notnull(par_look_enf['pere'])
        # TODO: changer le score pour avoir un lien entre pere et mere plus évident
        match3 = Matching(enf_look_par.ix[cond3_enf, var_match], 
                          par_look_enf.ix[cond3_par, var_match], score)
        parent_found3 = match3.evaluate()
        ind.ix[parent_found3.index, ['pere']] = par_look_enf.ix[parent_found3, ['pere']]               
        
#          Temps de calcul approximatif : 15 secondes, je laisse là juste pour voir les évolution du temps de calcul par la suite 
#          mais il faudra supprimer un jour        
#         match = Matching(enf_look_par[var_match], par_look_enf[var_match], score)
#         match.evaluate()
        
        self.ind = ind
        
        pdb.set_trace()

         
    def lien_couple_maries(self):
        
        NotImplementedError()        
# #TODO au moment où on en a besoin
# nb_enf_mere = enf.groupby('mere').size()
# nb_enf_pere = enf.groupby('pere').size()
# # note, qu'on doit faire la somme en cas de couple homosexuel    
    
    def lien_couple_hdom(self):
        NotImplementedError()


if __name__ == '__main__':
    data = DataTil()
    data.lecture()
    
    data.correction()
    
    data.conjoint()
    data.enfants()

#     data.creations_foy()
    data.lien_parent_enfant_hdom()
#     data.mise_au_format()
