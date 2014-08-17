# -*- coding:utf-8 -*-
'''
Created on 22 juil. 2013
Alexis Eidelman
'''

#TODO: duppliquer la table avant le matching parent enfant pour ne pas se trimbaler les valeur de hod dans la duplication.

from til.data.utils.utils import replicate, new_link_with_men, of_name_to_til, new_idmen, count_dup
from til.CONFIG import path_model
import numpy as np
import os
import tables

from pandas import merge, notnull, DataFrame, concat, HDFStore
import pdb


# Dictionnaire des variables, cohérent avec les imports du modèle.
# il faut que ce soit à jour. Le premier éléments est la liste des
# entiers, le second celui des floats
variables_til = {'ind': (['agem','sexe','men','quimen','foy','quifoy','tuteur',
                         'pere','mere','conj','civilstate','findet',
                         'workstate','xpr','anc'],['sali','rsti','choi', 'tauxprime']),
                 'men': (['pref'],[]),
                 'foy': (['vous','men'],[]),
                 'futur':(['agem','sexe','men','quimen','foy','quifoy',
                         'pere','mere','conj','civilstate','findet',
                         'workstate','xpr','anc', 'deces'],['sali','rsti','choi']),
                 'past': ([],[])}

class DataTil(object):
    """
    La classe qui permet de lancer le travail sur les données
    La structure de classe n'est peut-être pas nécessaire pour l'instant
    """
    def __init__(self):
        self.name = None
        self.survey_date = None
        self.ind = None
        self.men = None
        self.foy = None
        self.futur = None
        self.past = None
        self.longitudinal = {}
        self.child_out_of_house = None
        self.seuil= None

        #TODO: Faire une fonction qui chexk où on en est, si les précédent on bien été fait, etc.
        self.done = []
        self.order = []

    def load(self):
        print "début de l'importation des données"
        raise NotImplementedError()
        print "fin de l'importation des données"

    #def rename_var(self, [pe1e, me1e]):
        # TODO : fonction qui renomme les variables pour qu'elles soient au format liam
        # period, id, agem, age, sexe, men, quimen, foy quifoy pere, mere, conj, dur_in_couple, civilstate, workstate, sali, findet

    def drop_variable(self, dict_to_drop=None, option='white'):
        '''
        - Si on dict_to_drop is not None, il doit avoir la forme table: [liste de variables],
        on retire alors les variable de la liste de la table nommée.
        - Sinon, on se sert de cette méthode pour faire la première épuration des données, on
         a deux options:
             - passer par la liste blanche ce que l'on recommande pour l'instant
             - passer par  liste noire.
        '''
        if 'ind' in dict_to_drop.keys():
            self.ind = self.ind.drop(dict_to_drop['ind'], axis=1)
        if 'men' in dict_to_drop.keys():
            self.men = self.men.drop(dict_to_drop['men'], axis=1)
        if 'foy' in dict_to_drop.keys():
            self.foy = self.foy.drop(dict_to_drop['foy'], axis=1)

    def format_initial(self):
        raise NotImplementedError()

    def check_conjoint(self, couple_hdom=False):
        '''
        Vérifications/corrections de :
            - La réciprocité des déclarations des conjoints
            - La concordance de la déclaration des états civils en cas de réciprocité
            - conjoint hdom : si couple_hdom=True, les couples ne vivant pas dans le même domicile sont envisageable, sinon non.
        '''
        ind = self.ind
        print ("Début de la vérification sur les conjoints")

        # TODO: faire une fonction qui s'adapte à ind pour check si les unions/désunions sont bien signifiées
        # pour les deux personnses concernées

        #1 -Vérifie que les conjoints sont bien reciproques
        ind = ind.fillna(-1)
        def _reciprocite_conj(ind):
            test = ind.loc[(ind['conj'] != -1),['id','conj','civilstate']] #| ind['civilstate'].isin([1,5])
            test = merge(test,test,left_on='id', right_on='conj', how='outer').fillna(-1)
            try:
                assert(sum(test['conj_x'] == test['id_y']) == 0)
            except :
                test = test[test['conj_x'] != test['id_y']]
                print "Nombre d'époux non réciproques : " + str(len(test))

                # (a) - l'un des deux se déclare célibataire -> le second le devient
                celib_y = test.loc[test['civilstate_x'].isin([1,5]) & ~test['civilstate_y'].isin([1,5,-1]) & (test['id_x']< test['conj_x']),
                                            ['id_x', 'civilstate_y']]
                if len(celib_y)>0:
                    ind.loc[celib_y['id_x'].values, 'civilstate']= celib_y['civilstate_y']
                    ind.loc[celib_y['id_x'].values, 'conj'] = np.nan

                celib_x = test.loc[test['civilstate_y'].isin([1,5]) & ~test['civilstate_x'].isin([1,5,-1]) & (test['id_x']< test['conj_x']),
                                            ['id_y','civilstate_x']]
                if len(celib_x>0):
                    ind.loc[celib_x['id_y'].values, 'civilstate'] = celib_x['civilstate_x']
                    ind[celib_x['id_y'].values, 'conj'] = -1

                # (b) - les deux se déclarent mariés mais conjoint non spécifié dans un des deux cas
                # -> Conjoint réattribué à qui de droit
                no_conj = test[test['civilstate_x'].isin([1,5]) & test['civilstate_y'].isin([1,5]) & (test['conj_x']==-1)][['id_y', 'id_x']]
                if len(no_conj)>0:
                    print "Les deux se déclarent  en couples mais conjoint non spécifié dans un des deux cas", len(no_conj)
                    ind['conj'][no_conj['id_x'].values] = no_conj['id_y'].values
                    ind = ind.fillna(-1)


            return ind
        ind = _reciprocite_conj(ind)
        #2 - Vérifications de la concordance des états civils déclarés pour les personnes ayant un conjoint déclaré (i.e. vivant avec lui)
        test = ind.loc[(ind['conj']!= -1), ['conj','id','civilstate', 'sexe']]
        test = merge(test, test, left_on='id', right_on='conj')

        # 2.a - Confusion mariage/pacs
        confusion = test[(test['id_y']> test['id_x'])& (test['civilstate_y']!= test['civilstate_x']) & ~test['civilstate_y'].isin([3,4]) &  ~test['civilstate_x'].isin([3,4])]
        if len(confusion)>0:
            print "Nombre de confusions sur l'état civil (corrigées) : ", len(confusion)
            # Hypothese: Celui ayant l'identifiant le plus petit dit vrai
            ind['civilstate'][confusion['id_y'].values] = ind['civilstate'][confusion['id_x'].values]
            ind = ind.fillna(-1)

        # 2.b - Un déclarant marié/pacsé l'autre veuf/divorcé -> marié/pacsé devient célibataire
        conf = test[(test['civilstate_y']!= test['civilstate_x']) & (test['civilstate_y'].isin([3,4]) |  test['civilstate_x'].isin([3,4]))]
        confusion = conf[conf['civilstate_y'].isin([3,4]) & conf['civilstate_x'].isin([1,5]) ]
        if len(confusion)>0:
            print "Nombre de couples marié/veuf (corrigés) : ", len(confusion)
            ind['civilstate'][confusion['id_x'].values] = 2

        #3- Nombre de personnes avec conjoint hdom
        conj_hdom = ind[ind['civilstate'].isin([1,5]) & (ind['conj'] == -1)]
        print "Nombre de personnes ayant un conjoint hdom : ", len(conj_hdom)
        if couple_hdom == False :
            print "Ces personnes sont considérées célibataires "
            ind.loc[ind['civilstate'].isin([1,5]) & (ind['conj'] == -1), 'civilstate'] = 2
            assert len(ind[ind['civilstate'].isin([1,5]) & (ind['conj'] == -1)]) == 0
        self.ind = ind
        #ind = _reciprocite_conj(ind)
        print ("Fin de la vérification sur les conjoints")
    def enfants(self):
        '''
        Calcule l'identifiant des parents
        '''
        raise NotImplementedError()

    def table_initial(self):
        raise NotImplementedError()

    def creation_foy(self):
        '''
        Créer les déclarations fiscale. Il s'agit principalement de regrouper certains individus entre eux.
        Ce n'est qu'ici qu'on s'occupe de verifier que les individus mariés ou pacsé ont le même statut matrimonial
        que leur partenaire légal. On ne peut pas le faire dès le début parce qu'on a besoin du numéro du conjoint.
        '''
        ind = self.ind
        men = self.men
        survey_date = self.survey_date
        print ("Creation des declarations fiscales")
        # 0eme étape : création de la variable 'nb_enf' si elle n'existe pas +  ajout 'lienpref'
        if 'nb_enf' not in ind.columns:
            ## nb d'enfant
            ind.index = ind['id']
            nb_enf_mere = ind.groupby('mere').size()
            nb_enf_pere = ind.groupby('pere').size()
            # On assemble le nombre d'enfants pour les peres et meres en enlevant les manquantes ( = -1)
            enf_tot = concat([nb_enf_mere, nb_enf_pere], axis=0)
            enf_tot = enf_tot.drop([-1])
            ind['nb_enf'] = 0
            ind['nb_enf'][enf_tot.index] = enf_tot.values

        def _name_var(ind, men):
            if 'lienpref' in ind.columns :
                ind['quimen'] = ind['lienpref']
                ind.loc[ind['quimen'] >1 , 'quimen'] = 2
                # a changer avec values quand le probleme d'identifiant et résolu .values
                men['pref'] = ind.loc[ind['lienpref']==0,'id'].values
            return men, ind
        men, ind = _name_var(ind, men)

        # 1ere étape : Identification des personnes mariées/pacsées
        spouse = (ind['conj'] != -1) & ind['civilstate'].isin([1,5])
        print str(sum(spouse)) + " personnes en couples"

        # 2eme étape : rôles au sein du foyer fiscal
        # selection du conjoint qui va être le vousrant (= déclarant principal du foyer fiscal) : pas d'incidence en théorie
        decl = spouse & ( ind['conj'] > ind['id'])
        conj = spouse & ( ind['conj'] < ind['id'])
        # Identification des personnes à charge (moins de 21 ans sauf si étudiant, moins de 25 ans )
        # attention, on ne peut être à charge que si on n'est pas soi-même parent
        pac_condition = (ind['civilstate'] == 2)  & ( ((ind['agem'] < 12*25) & \
                                                        (ind['workstate'] == 11)) | (ind['agem'] < 12*21) ) &(ind['nb_enf'] == 0)
        pac = ((ind['pere'] != -1) | (ind['mere'] != -1)) & pac_condition
        print str(sum(pac)) + ' personnes prises en charge'
        # Identifiants associés
        ind['quifoy'] = 0
        ind.loc[conj,'quifoy'] = 1
        # Comprend les enfants n'ayant pas de parents spécifiés (à terme rattachés au foyer 0= DASS)
        ind.loc[pac,'quifoy'] = 2
        ind.loc[(ind['men'] == 0) & (ind['quifoy'] == 0), 'quifoy'] = 2
        print "Nombres de foyers fiscaux", sum(ind['quifoy'] == 0), ", dont couple", sum(ind['quifoy'] == 1)

        # 3eme étape : attribution des identifiants des foyers fiscaux
        ind['foy'] = -1
        nb_foy = sum(ind['quifoy'] == 0)
        print "Le nombre de foyers créés est : " + str(nb_foy)
        # Rq: correspond au même décalage que pour les ménages (10premiers : institutions)
        ind.loc[ind['quifoy'] == 0, 'foy'] = range(10, nb_foy +10)

        # 4eme étape : Rattachement des autres membres du ménage
        # (a) - Rattachements des conjoints des personnes en couples
        conj = ind.loc[(ind['conj'] != -1) & (ind['civilstate'].isin([1,5]))& (ind['quifoy'] == 0), ['conj','foy']]
        ind['foy'][conj['conj'].values] = conj['foy'].values

        # (b) - Rattachements de leurs enfants (en priorité sur la décla du père)
        for parent in  ['pere', 'mere']:
            pac_par = ind.loc[ (ind['quifoy'] == 2) & (ind[parent] != -1) & (ind['foy'] == -1), ['id', parent]].astype(int)
            ind['foy'][pac_par['id'].values] = ind['foy'][pac_par[parent].values]
            print str(len(pac_par)) + " enfants sur la déclaration de leur " + parent

        # Enfants de la Dass -> foyer fiscal 'collectif'
        ind.loc[ind['men']==0, 'foy'] = 0

        # 5eme étape : création de la table foy
        vous = (ind['quifoy'] == 0) & (ind['foy'] > 9)
        foy = ind.loc[vous,['foy', 'id', 'men']]
        foy = foy.rename(columns={'foy': 'id', 'id': 'vous'})
        # Etape propre à l'enquete Patrimoine
        impots = ['zcsgcrds','zfoncier','zimpot', 'zpenaliv','zpenalir','zpsocm','zrevfin']
        var_to_declar = impots + ['pond', 'id', 'pref']
        foy_men = men.loc[men['pref'].isin(foy['vous']), var_to_declar].fillna(0)
        foy_men = foy_men.rename(columns = {'id' : 'men'})

        # hypothèse réparartition des élements à égalité entre les déclarations : discutable
        nb_foy_men = foy.loc[foy['men'].isin(foy_men['men'].values)].groupby('men').size()
        if (nb_foy_men.max() >1) & (foy_men ['zimpot'].max() >0) :
            assert len(nb_foy_men) ==  len(foy_men)
            for var in impots :
                foy_men[var] = foy_men[var] / nb_foy_men
            foy = merge(foy, foy_men, on = 'men', how ='left', right_index=True)
        foy['period'] = survey_date

        # Ajouts des 'communautés' dans la table foyer
        for k in [0]:
            if sum(ind['foy'] == k) !=0 :
                to_add = DataFrame([np.zeros(len(foy.columns))], columns = foy.columns)
                to_add['id'] = k
                to_add['vous'] = -1
                to_add['period'] = survey_date
                foy = concat([foy, to_add], axis = 0, ignore_index=True)

        foy.index = foy['id']
        assert sum(ind['foy']==-1) == 0
        print 'Taille de la table foyers :', len(foy)
        #### fin de declar
        self.ind = ind
        self.foy = foy

        print("fin de la creation des declarations")

    def creation_child_out_of_house(self):
        '''
        Travail sur les liens parents-enfants.
        On regarde d'abord les variables utiles pour le matching
        '''
        raise NotImplementedError()

    def matching_par_enf(self):
        '''
        Matching des parents et des enfants hors du domicile
        '''
        raise NotImplementedError()

    def match_couple_hdom(self):
        '''
        Certaines personnes se déclarent en couple avec quelqu'un ne vivant pas au domicile, on les reconstruit ici.
        Cette étape peut s'assimiler à de la fermeture de l'échantillon.
        On séléctionne les individus qui se déclare en couple avec quelqu'un hors du domicile.
        On match mariés,pacsé d'un côté et sans contrat de l'autre. Dit autrement, si on ne trouve pas de partenaire à une personne mariée ou pacsé on change son statut de couple.
        Comme pour les liens parents-enfants, on néglige ici la possibilité que le conjoint soit hors champ (étrange, prison, casernes, etc).
        Calcul aussi la variable ind['nb_enf']
        '''
        raise NotImplementedError()

    def expand_data(self, seuil=150, nb_ligne=None):
        #TODO: add future and past
        '''
        Note: ne doit pas tourner après lien parent_enfant
        Cependant child_out_of_house doit déjà avoir été créé car on s'en sert pour la réplication
        '''
        self.seuil = seuil
        if seuil != 0 and nb_ligne is not None:
            raise Exception("On ne peut pas à la fois avoir un nombre de ligne désiré et une valeur" \
            "qui va determiner le nombre de ligne")
        #TODO: on peut prendre le min des deux quand même...
        men = self.men
        ind = self.ind
        foy = self.foy
        par = self.child_out_of_house
        longit = self.longitudinal

        if par is None:
            print("Notez qu'il est plus malin d'étendre l'échantillon après avoir fait les tables " \
            "child_out_of_house plutôt que de les faire à partir des tables déjà étendue")

        if foy is None:
            print("C'est en principe plus efficace d'étendre après la création de la table foyer" \
                  " mais si on veut rattacher les enfants (par exemple de 22 ans) qui ne vivent pas au" \
                  " domicile des parents sur leur déclaration, il faut faire l'extension et la " \
                  " fermeture de l'échantillon d'abord. Pareil pour les couples. ")
        min_pond = min(men['pond'])
        target_pond = float(max(min_pond, seuil))

        # 1 - Réhaussement des pondérations inférieures à la pondération cible
        men['pond'][men['pond']<target_pond] = target_pond
        # 2 - Calcul du nombre de réplications à effectuer
        men['nb_rep'] = men['pond'].div(target_pond)
        men['nb_rep'] = men['nb_rep'].round()
        men['nb_rep'] = men['nb_rep'].astype(int)

        # 3- Nouvelles pondérations (qui seront celles associées aux individus après réplication)
        men['pond'] = men['pond'].div(men['nb_rep'])
        # TO DO: réflechir pondération des personnes en collectivité pour l'instant = 1
        men.loc[men['id'] < 10, 'pond'] = 1
        men_exp = replicate(men)

        # pour conserver les 10 premiers ménages = collectivités
        men_exp['id'] = new_idmen(men_exp, 'id')

        if foy is not None:
            foy = merge(men[['id','nb_rep']], foy, left_on='id', right_on='men', how='right', suffixes=('_men',''))
            foy_exp= replicate(foy)
            foy_exp['men'] = new_link_with_men(foy, men_exp, 'men')
        else:
            foy_exp = None

        if par is not None:
            par = merge(men[['id','nb_rep']], par, left_on = 'id', right_on='men', how='inner', suffixes=('_men',''))
            par_exp = replicate(par)
            par_exp['men'] = new_link_with_men(par, men_exp, 'men')
        else:
            par_exp = None

        ind = merge(men[['id','nb_rep']].rename(columns = {'id': 'men'}), ind, on='men', how='right', suffixes = ('_men',''))
        ind_exp = replicate(ind)
        # lien indiv - entités supérieures
        ind_exp['men'] = new_link_with_men(ind, men_exp, 'men')
        ind_exp['men'] += 10

        # liens entre individus
        tableB = ind_exp[['id_rep','id_ini']]
        tableB['id_index'] = tableB.index
#         ind_exp = ind_exp.drop(['pere', 'mere','conj'], axis=1)
        print("debut travail sur identifiant")
        def _align_link(link_name, table_exp):
            tab = table_exp[[link_name, 'id_rep']].reset_index()
            tab = tab.merge(tableB,left_on=[link_name,'id_rep'], right_on=['id_ini','id_rep'], how='inner').set_index('index')
            tab = tab.drop([link_name], axis=1).rename(columns={'id_index': link_name})
            table_exp[link_name][tab.index.values] = tab[link_name].values
#             table_exp.merge(tab, left_index=True,right_index=True, how='left', copy=False)
            return table_exp

        ind_exp = _align_link('pere', ind_exp)
        ind_exp = _align_link('mere', ind_exp)
        ind_exp = _align_link('conj', ind_exp)

        #TODO: add _align_link with 'pere' and 'mere' in child_out_ouf_house in order to swap expand
        # and creation_child_out_ouf_house, in the running order

        if foy is not None:
            #le plus simple est de repartir des quifoy, cela change du men
            # la vérité c'est que ça ne marche pas avec ind_exp['foy'] = new_link_with_men(ind, foy_exp, 'foy')
            vous = (ind['quifoy'] == 0)
            conj = (ind['quifoy'] == 1)
            pac = (ind['quifoy'] == 2)
            ind.loc[vous,'foy']= range(sum(vous))
            ind.loc[conj,'foy'] = ind.ix[ind['conj'][conj],['foy']]
            pac_pere = pac & notnull(ind['pere'])
            ind.loc[pac_pere,'foy'] = ind.loc[ind.loc[pac_pere,'pere'],['foy']]
            pac_mere = pac & ~notnull(ind['foy'])
            ind.loc[pac_mere,'foy'] = ind.loc[ind.loc[pac_mere,'mere'],['foy']]

        for name, table in longit.iteritems():
            table = table.merge(ind_exp[['id_ini', 'id']], right_on='id', left_index=True, how='right')
            table.set_index('id', inplace=True)
            table.drop('id_ini', axis=1, inplace=True)
            self.longitudinal[name] = table

        assert sum(ind['id']==-1) == 0
        self.child_out_of_house = par
        self.men = men_exp
        self.ind = ind_exp
        self.foy = foy_exp
        self.drop_variable({'men':['id_rep','nb_rep'], 'ind':['id_rep']})

    def format_to_liam(self):
        '''
        On met ici les variables avec les bons codes pour achever le travail de DataTil
        On crée aussi les variables utiles pour la simulation
        '''
        men = self.men
        ind = self.ind
        foy = self.foy
        futur = self.futur
        past = self.past
        longit = self.longitudinal

        ind_men = ind.groupby('men')
        ind = ind.set_index('men')
        ind['nb_men'] = ind_men.size().astype(np.int)
        ind = ind.reset_index()

        ind_foy = ind.groupby('foy')
        ind = ind.set_index('foy')
        ind['nb_foy'] = ind_foy.size().astype(np.int)
        ind = ind.reset_index()

        if 'lienpref' in ind.columns :
            self.drop_variable({'ind':['lienpref','anais','mnais']})

        for name in ['ind', 'foy', 'men', 'futur', 'past']:
            table = eval(name)
            if table is not None:
                vars_int, vars_float = variables_til[name]
                for var in vars_int + ['id','period']:
                    if var not in table.columns:
                        table[var] = -1
                    table = table.fillna(-1)
                    table[var] = table[var].astype(np.int32)
                for var in vars_float + ['pond']:
                    if var not in table.columns:
                        if var=='pond':
                            table[var] = 1
                        else:
                            table[var] = -1
                    table = table.fillna(-1)
                    table[var] = table[var].astype(np.float64)
                table = table.sort_index(by=['period','id'])
                setattr(self, name, table)

#        # In case we need to Add one to each link because liam need no 0 in index
#        if ind['id'].min() == 0:
#            links = ['id','pere','mere','conj','foy','men','pref','vous']
#            for table in [ind, men, foy, futur, past]:
#                if table is not None:
#                    vars_link = [x for x in table.columns if x in links]
#                    table[vars_link] += 1
#                    table[vars_link].replace(0,-1, inplace=True)

    def final_check(self):
        ''' Les checks sont censés vérifiés toutes les conditions
            que doit vérifier une base pour tourner sur Til '''
        men = self.men
        ind = self.ind
        foy = self.foy
        futur = self.futur
        longit = self.longitudinal
        
        assert all(ind['workstate'].isin(range(1,12)))
        assert all(ind['civilstate'].isin(range(1,6)))

        # Foyers et ménages bien attribués
        assert sum((ind['foy'] == -1)) == 0
        assert sum((ind['men'] == -1)) == 0
        print "Nombre de personnes dans ménages ordinaires : ", sum(ind['men']>9)
        print "Nombre de personnes vivant au sein de collectivités : ", sum(ind['men']<10)

        ## On vérifie qu'on a un et un seul qui = 0 et au plus un qui = 1 pour foy et men
        for ent in ['men', 'foy']:
            ind['qui0'] = (ind['qui' + ent] == 0).astype(int)
            ind['qui1'] = (ind['qui' + ent] == 1).astype(int)
            ind0 = ind[ind[ent] > 9].groupby(ent) # on exclut les collectivités
            # on vérifie qu'on a un et un seul qui = 0
            assert ind0['qui0'].sum().max() == 1
            assert ind0['qui0'].sum().min() == 1
            # on vérifie qu'on a au plus un qui = 1
            assert ind0['qui1'].sum().max() == 1
            # on vérifie que les noms d'identité sont bien dans la table entity et réciproquement
            list_id = eval(ent)['id']
            assert ind[ent].isin(list_id).all()
            assert list_id.isin(ind[ent]).all()
            # si on est un 2

            # si on est quimen = 1 alors on a son conjoint avec soi
            qui1 = ind['qui' + ent]==1
            conj = ind.loc[qui1, 'conj'].values
            conj_ent = ind.iloc[conj]
            conj_ent = conj_ent[ent]
            qui1_ent = ind.loc[qui1, ent]
            assert (qui1_ent == conj_ent).all()

        # Table futur bien construite
        if futur is not None:
            # -> On vérifie que persone ne nait pas dans le futur tout en étant présent dans les données intiales
            id_ini = ind[['id']]
            # 'naiss' != -1 <-> naissance
            id_futur = futur.loc[(futur['naiss']!=-1) , ['id']]
            id_ok = concat([id_ini, id_futur], axis = 0)
            assert count_dup(id_ok,'id') == 0
            assert len(futur[(futur['naiss']<= self.survey_year) & (futur['naiss']!= -1) ])== 0
            if len(futur.loc[~futur['id'].isin(id_ok['id']), 'id']) != 0:
                pb_id = futur.loc[~(futur['id'].isin(id_ok['id'])), :].drop_duplicates('id')
                print ('Nombre identifants problématiques dans la table futur: ', len(pb_id))

            print ("Nombre de personnes présentes dans la base "
                    + str( len(id_ok)) + " ("+ str( len(id_ini))
                    + " initialement et " + str( len(id_futur)) + " qui naissent ensuite)")

        for table in [ind, men, foy, futur]:
            if table is not None:
                test_month = table['period'] % 100
                assert all(test_month.isin(range(1, 13)))
                test_year = table['period'] // 100
                assert all(test_year.isin(range(1900, 2100)))
                
        for name, table in longit.iteritems():
            cols = table.columns
            cols_year = [(col // 100 in range(1900, 2100))  for col in cols]
            cols_month = [(col % 100 in range(1, 13)) for col in cols]
            assert all(cols_year)
            assert all(cols_month)
                
    def _output_name(self, extension='.h5'):
        if self.seuil is None:
            name = self.name + extension
        else: 
            name = self.name + '_' + str(self.seuil) + extension
        return os.path.join(path_model, name)

    def store_to_liam(self):
        '''
        Sauvegarde des données au format utilisé ensuite par le modèle Til
        Séléctionne les variables appelée par Til derrière
        Appelle des fonctions de Liam2
        '''

        path = self._output_name()
        h5file = tables.openFile(path, mode="w")

        ent_node = h5file.createGroup("/", "entities", "Entities")
        for ent_name in ['ind','foy','men','futur','past']:
            entity = eval('self.'+ ent_name)
            if entity is not None:
                entity = entity.fillna(-1)
                ent_table = entity.to_records(index=False)
                dtypes = ent_table.dtype
                final_name = of_name_to_til[ent_name]
                table = h5file.createTable(ent_node, final_name, dtypes, title="%s table" % final_name)
                table.append(ent_table)
                table.flush()

                if ent_name == 'men':
                    entity = entity.loc[entity['id']>-1]
                    ent_table2 = entity[['pond','id','period']].to_records(index=False)
                    dtypes2 = ent_table2.dtype
                    table = h5file.createTable(ent_node, 'companies', dtypes2, title="'companies table")
                    table.append(ent_table2)
                    table.flush()
                if ent_name == 'ind':
                    ent_table2 = entity[['agem','sexe','pere','mere','id','findet','period']].to_records(index=False)
                    dtypes2 = ent_table2.dtype
                    table = h5file.createTable(ent_node, 'register', dtypes2, title="register table")
                    table.append(ent_table2)
                    table.flush()
        h5file.close()

        # 3 - table longitudinal
        # Note: on conserve le format pandas ici
        store = HDFStore(path)
        for varname, table in self.longitudinal.iteritems():
            table['id'] = table.index
            store.append('longitudinal/' + varname, table)
        store.close()

    def store(self):
        path = self._output_name()
        self.men.to_hdf(path, 'entites/men')
        self.ind.to_hdf(path, 'entites/ind')
        self.foy.to_hdf(path, 'entites/foy')

    def run_all(self):
        for method in self.methods_order:
            eval('self.'+ method + '()')
