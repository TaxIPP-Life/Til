# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd

from pandas import DataFrame
from pgm.CONFIG import path_data_destinie
from datetime import datetime, timedelta

from pgm.Pension.SimulPension import PensionSimulation, workstate_selection, years_to_months, months_to_years, unemployment_trimesters, calculate_trim_cot, substract_months, valbytranches_date, calculate_SAM

code_avpf = 8

class Regime_general(PensionSimulation):
    
    def __init__(self, param_regime, param_common, param_longitudinal):
        PensionSimulation.__init__(self)
        self.regime = 'RG'
        self.code_regime = [3,4]
        
        self._P = param_regime
        self._Pcom = param_common
        self._Plongitudinal = param_longitudinal
        
        self.workstate  = None
        self.sali = None
        self.info_ind = None
        self.info_child_mother = None
        self.info_child_father = None
        self.time_step = None

    def load(self):
        
        def _build_table(table, yearsim):
            table = table.reindex_axis(sorted(table.columns), axis=1)
            date_end = (yearsim - 1 )* 100 + 1
            possible_dates = []
            for y in range(1949, yearsim): 
                possible_dates += [y * 100 + m for m in range(1,13)]
            selected_dates = set(table.columns).intersection(possible_dates)
            table = table.loc[:, selected_dates]
            table = table.reindex_axis(sorted(table.columns), axis=1)
            return table
                        
        def _build_salmin(smic,avts):
            '''
            salaire trimestriel de référence minimum
            Rq : Toute la série chronologique est exprimé en euros
            '''
            yearsim = self.datesim.year
            salmin = DataFrame( {'year' : range(1949, yearsim ), 'sal' : - np.ones(yearsim - 1949)} ) 
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
                
            #TODO: Trancher si on calcule les droits à retraites en incluant le travail à l'année de simulation pour l'instant non (ex : si datesim = 2009 on considère la carrière en emploi jusqu'en 2008)
            for year in range(1972,yearsim):
                smic_old = smic_year
                smic_year = []
                for key in smic.keys():
                    if str(year) in key:
                        smic_year.append(key)
                if not smic_year:
                    smic_year = smic_old
                if year <= 2013 :
                    salmin.loc[salmin['year'] == year, 'sal'] = smic[smic_year[0]] * 200 
                    if year <= 2001 :
                        salmin.loc[salmin['year'] == year, 'sal'] = smic[smic_year[0]] * 200  / 6.5596
                else:
                    salmin.loc[salmin['year'] == year, 'sal'] = smic[smic_year[0]] * 150 
            return salmin['sal']
        
        def _build_naiss(agem, datesim):
            ''' Détermination de la date de naissance à partir de l'âge et de la date de simulation '''
            naiss = agem.apply(lambda x: substract_months(datesim, x))
            return naiss
        
        # Selection du déroulé de carrière qui nous intéresse (1949 -> année de simulation)
        # Rq : la selection peut se faire sur données mensuelles ou annuelles
        yearsim = self.datesim.year
        self.workstate = _build_table(self.workstate, yearsim)
        self.sali = _build_table(self.sali, yearsim)
        self.info_ind['naiss'] = _build_naiss(self.info_ind['agem'], self.datesim)
        # Salaires de référence (vecteur construit à partir des paramètres indiquant les salaires annuels de reférences)
        smic_long = self._Plongitudinal.common.smic
        avts_long = self._Plongitudinal.common.avts.montant
        self.salref = _build_salmin(smic_long, avts_long)
            
    def nb_trim_cot(self):
        ''' Nombre de trimestres côtisés pour le régime général 
        ref : code de la sécurité sociale, article R351-9
         '''
        # Selection des salaires à prendre en compte dans le décompte (mois où il y a eu côtisation au régime)
        wk_selection = workstate_selection(self.workstate, code_regime = self.code_regime, input_step = self.time_step, output_step = 'month')
        sal_selection = wk_selection * years_to_months(self.sali, division = True) 
        nb_trim_cot = calculate_trim_cot(months_to_years(sal_selection), self.salref)
        self.sal_RG = sal_selection
        return nb_trim_cot
        

    def nb_trim_ass(self):
        ''' Comptabilisation des périodes assimilées à des durées d'assurance
        Pour l"instant juste chômage workstate == 5 (considéré comme indemnisé) qui succède directement à une période de côtisation au RG workstate == [3,4]'''
        nb_trim_chom = unemployment_trimesters(self.workstate, code_regime = self.code_regime, input_step = self.time_step)
        nb_trim_ass = nb_trim_chom # TODO: + nb_trim_war + ....
        return nb_trim_ass
            
    def nb_trim_maj(self):
        ''' Trimestres majorants acquis au titre de la MDA, de l'assurance pour congé parental ou de l'AVPF '''
        
        def _mda(info_child, list_id, yearsim):
            ''' Majoration pour enfant à charge : nombre de trimestres acquis
            Rq : cette majoration n'est applicable que pour les femmes dans le RG'''
            mda = pd.DataFrame({'mda' : np.zeros(len(list_id))}, index = list_id)
            # TODO: distinguer selon l'âge des enfants après 2003
            # ligne suivante seulement if info_child['age_enf'].min() > 16 :
            info_child = info_child.groupby('id_parent')['nb_enf'].sum()
            if yearsim < 1972 :
                return mda
            elif yearsim <1975:
                # loi Boulin du 31 décembre 1971 
                mda.loc[info_child.index.values, 'mda'] = 4 * info_child.values
                mda.loc[mda['mda'] < 2, 'mda'] = 0
                return mda
            elif yearsim <2004:
                mda.loc[info_child.index.values, 'mda'] = 4 * info_child.values
                mda.loc[mda['mda'] < 2, 'mda'] = 0
                return mda
            else:
                # Réforme de 2003 : min(1 trimestre à la naissance + 1 à chaque anniv, 8)
                mda.loc[info_child.index.values, 'mda'] = 4 * info_child.values
                mda.loc[mda['mda'] < 2, 'mda'] = 0
                return mda['mda']
            
        def _avpf(workstate, sali, input_step):
            ''' Allocation vieillesse des parents au foyer : nombre de trimestres acquis'''
            avpf_selection = workstate_selection(workstate, code_regime = [code_avpf], input_step = input_step, output_step = 'month')
            sal_avpf = avpf_selection * years_to_months(sali, division = True) 
            nb_trim = calculate_trim_cot(months_to_years(sal_avpf), self.salref)
            return nb_trim
        
        # info_child est une DataFrame comportant trois colonnes : identifiant du parent, âge de l'enfant, nb d'enfants du parent ayant cet âge  
        info_child_mere = self.info_child_mother
        list_id = self.sali.index.values
        yearsim = self.datesim.year
        
        nb_trim_mda = _mda(info_child_mere, list_id, yearsim)
        nb_trim_avpf = _avpf(self.workstate, self.sali, self.time_step)
        return nb_trim_mda + nb_trim_avpf
    
    def SAM(self):
        nb_years = self._P.nb_sam
        if '_control' in  nb_years.__dict__ :
            var_control = self.info_ind[str(nb_years.control)]
            nb_years = valbytranches_date(var_control, nb_years)
        SAM = calculate_SAM(self.sal_RG, nb_years, time_step = 'month')
        return SAM
        
    def calculate_CP(self, trim_cot):
        ''' Calcul du coefficient de proratisation '''
        N_CP =  self._P.N_prorat
        date = self.datesim
        if datetime.strptime("1948-01-01","%Y-%m-%d").date() <= date:
            trim_cot = trim_cot + (120 - trim_cot)/2
        if datetime.strptime("1983-01-01","%Y-%m-%d").date() <= date:
            trim_cot = np.minimum(N_CP, trim_cot * (1 + np.maximum(0, age/ 3 - 260)))
        CP = np.minimum(1, trim_cot / N_CP)
        return CP