# -*- coding: utf-8 -*-


# OpenFisca -- A versatile microsimulation software
# By: OpenFisca Team <contact@openfisca.fr>
#
# Copyright (C) 2011, 2012, 2013, 2014 OpenFisca Team
# https://github.com/openfisca
#
# This file is part of OpenFisca.
#
# OpenFisca is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# OpenFisca is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import collections
from datetime import date

from openfisca_core.columns import BoolPresta, FloatPresta, IntPresta, EnumPresta
from openfisca_core.enumerations import Enum
from openfisca_core.formulas import AbstractSimpleFormula
from openfisca_core import entities

import individual_parameters as indparam
#from pgm.Pension.Model.Regime_general.Regime_general import Regime_general as RG


def build_simple_formula_couple(name, prestation):
    assert isinstance(name, basestring), name
    name = unicode(name)
    prestation.formula_constructor = type(name.encode('utf-8'), (SimpleFormula,), dict(
        calculate = staticmethod(prestation._func),
        ))
    del prestation._func
    if prestation.label is None:
        prestation.label = name
    assert prestation.name is None
    prestation.name = name

    entity_column_by_name = entities.entity_class_by_symbol[prestation.entity].column_by_name
    assert name not in entity_column_by_name, name
    entity_column_by_name[name] = prestation

    return (name, prestation)


prestation_by_name = collections.OrderedDict((

    ############################################################
    # Variables individuelles
    ############################################################

    # Salaires
    build_simple_formula_couple('CP_fonction_publique', FloatPresta(indparam._coefficient_proratisation, label = u"CCoeffeicient de proratisation de la fonction publique")),

    ))

