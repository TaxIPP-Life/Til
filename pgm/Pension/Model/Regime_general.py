# -*- coding:utf-8 -*-

#import pandas as pd
import numpy as np

from pandas import DataFrame
from pgm.CONFIG import path_data_destinie
from datetime import datetime
from pgm.Pension.SimulPension import PensionSimulation

class Regime_general(PensionSimulation):
    
    def __init__(self, param_regime, param_common, param_longitudinal):
        PensionSimulation.__init__(self)
        self.regime = 'RG'
        self.ret_base = None
        self.ret_comp = None
        self._P = param_regime
        self._Pcom = param_common
        self._Plongitudinal = param_longitudinal
        self.workstate  = None
        self.sali = None

    def _nb_trim_cot(self):
        ''' Nombre de trimestres côtisés pour le régime général 
        ref : code de la sécurité sociale, article R351-9
        TODO :
        1) Sur données annuelles pour l'instant. A adapter sur pas mensuel : 
            - condition sur workstate : si a cotiser au moins un mois au régime
            - condition sur sali : la même mais traduire sali en sal(annuel) sommant que les salaires RG
         2) Construction de sal_mini : 
             - commence en 1949 mais voir avant (légilsation plus complexe) 
             - éviter la sélection aléatoire dans _build_sal_min
         3) Check revalorisation
         '''
        workstate = self.workstate[194901:]
        sali = self.sali[194901:]
        yearsim = self.datesim.year
        P = self._Pcom
        sal_min = 0

        def _build_salmin(yearsim, smic, avts):
            '''
            salaire annuel de référence minimum 
            '''
            salmin = DataFrame( {'year' : range(1949, yearsim + 1), 'sal' : - np.ones(yearsim - 1949 +1)} )
            avts_year = []
            smic_year = []
            for year in range(1949,1972):
                avts_old = avts_year
                avts_year = []
                for key in avts.keys():
                    if str(year) in key:
                        avts_year.append(key)
                if not avts_year:
                    avts_year = avts_old
                salmin.loc[salmin['year'] == year, 'sal'] = avts[avts_year[0]] 
                
            for year in range(1972,yearsim + 1):
                smic_old = smic_year
                smic_year = []
                for key in smic.keys():
                    if str(year) in key:
                        smic_year.append(key)
                if not smic_year:
                    smic_year = smic_old
                salmin.loc[salmin['year'] == year, 'sal'] = smic[smic_year[0]] * 200 * 4
                if year <= 2001 :
                     salmin.loc[salmin['year'] == year, 'sal'] = smic[smic_year[0]] * 200 * 4 / 6.5596
            print salmin.to_string()
            return salmin['sal']
        
        smic_long = self._Plongitudinal.common.smic
        avts_long = self._Plongitudinal.common.avts.montant
        salref = _build_salmin(yearsim, smic_long, avts_long)
        import pdb
        pdb.set_trace()
        def _calculate_trim_cot(data):
            ''' fonction de calcul effectif à partir d'une matrice contenant les salaires annuels cotisés au RG
            lignes : individus / colonnes : date '''
            print "test"
            
        workstate_selection = (workstate == 3 | workstate == 4 ).astype(int)
        sali_selection = np.greater_equal(sali, sal_min).astype(int)
        sal_cot = sali * workstate_selection * sali_selection
        nb_trim_cot = sal_cot.apply(_calculate_trim_cot)
        return nb_trim_cot
        

    ######
    # Déterminations des variables d'input de la simulation
    ####

    def nb_trim_maj(self,P):
        ''' Code sécu : article R. 351-9 '''
        
        print P
            
    def MDA(self, data):
        MDA_naiss = self.valparam['mda_naiss']
        MDA_educ = self.valparam['mda_educ']
        nb_enf = data['nb_enf']
        nb_educ = data['nb_enf_educ']
        if self._date < datetime.strptime("1974-06-01","%Y-%m-%d").date():
            nb_enf = nb_enf.replace(1,0)
            mda = MDA_naiss *nb_enf
        else:
            mda = MDA_naiss*nb_enf + MDA_educ*nb_educ
        return mda
            
    
    def calculate_CP(self, trim_cot, age):
        ''' Calcul du coefficient de proratisation '''
        N_CP =  self.valparam['N_CP']
        date = self._date
        if datetime.strptime("1948-01-01","%Y-%m-%d").date() <= date:
            trim_cot = trim_cot + (120 - trim_cot)/2
        if datetime.strptime("1983-01-01","%Y-%m-%d").date() <= date:
            trim_cot = np.minimum(N_CP, trim_cot* (1 + np.maximum(0, age/ 3 - 260)))
            
        print len(trim_cot / N_CP)
        CP = np.minimum(1, trim_cot / N_CP)
        print len(CP)
        return CP