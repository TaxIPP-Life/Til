# -*- coding: utf-8 -*-


from __future__ import print_function


import numpy as np

try:
    from liam2.expr import always, expr_eval, FunctionExpr
except ImportError:
    from src.expr import always, expr_eval, FunctionExpr

try:
    from liam2.exprbases import FilteredExpression #, FunctionExpr)
except ImportError:
    from src.exprbases import FilteredExpression


from til.pgm.run_pension import get_pension

# TODO: implement functions in expr to generate "Expr" nodes at the python level
# less painful


class TimeScale(FunctionExpr):
    func_name = 'period'

    def compute(self, context, expr):
        return expr_eval(expr, context) + context.periodicity

    dtype = always(int)


class Year(FunctionExpr):
    func_name = 'year'

    def compute(self, context, expr):
        return int(expr_eval(expr, context) / 100)

    dtype = always(int)


class Month(FunctionExpr):
    func_name = 'month'

    def compute(self, context, expr):
        return (expr_eval(expr, context) % 100)

    dtype = always(int)


class AddTime(FunctionExpr):
    func_name = 'add_time'

    def compute(self, context, expr):
        periodicity = context.periodicity
        init_value = expr_eval(expr, context)
        # TODO: be more general with periodicity > 12
        if periodicity > 0:
            change_year = (init_value % 100) + periodicity >= 12
            value = init_value + periodicity * (1 - change_year) + (100 - 12 + periodicity) * change_year
        if periodicity < 0:
            change_year = (init_value % 100) + periodicity < 1
            value = init_value + periodicity * (1 - change_year) + (-100 + 12 + periodicity) * change_year
        return value

    dtype = always(int)

#------------------------------------




def check_structure(simulation):
    import pandas
    data_frame = pandas.DataFrame(dict(
        (variable_name, simulation.calculate(variable_name))
        for variable_name in [
            entity.role_for_person_variable_name
            for entity in simulation.entity_by_key_plural.values()
            if entity.key_plural != 'individus'
            ] + [
            entity.index_for_person_variable_name
            for entity in simulation.entity_by_key_plural.values()
            if entity.key_plural != 'individus'
            ]
        ))
    for key_plural, entity in simulation.entity_by_key_plural.iteritems():
        if key_plural == 'individus':
            continue
        print("Checking entity {}".format(key_plural))
        role_name = entity.role_for_person_variable_name
        idx_name = entity.index_for_person_variable_name

        assert not data_frame[role_name].isnull().any(), "there are NaN in qui{}".format(entity)
        max_entity = data_frame[role_name].max().astype("int")

        for position in range(0, max_entity + 1):
            test = data_frame[[role_name, idx_name]].groupby(by = idx_name).agg(lambda x: (x == position).sum())
            if position == 0:
                errors = (test[role_name] != 1).sum()
                if errors > 0:
                    print("There are {} errors for the head of {}".format(errors, entity))
            else:
                errors = (test[role_name] > 1).sum()
                if errors > 0:
                    print("There are {} duplicated qui{} = {}".format(errors, entity, position))

        assert len(data_frame[idx_name].unique()) == (data_frame[role_name] == 0).sum(),\
            "Wronger number of entity/head for {}".format(entity)


class OpenFiscaCaller(FilteredExpression):
    no_eval = ('filter', 'varname')
    already_simulated = None
    survey_scenario = None

    @classmethod
    def no_need_to_reload(cls, context):
        if OpenFiscaCaller.already_simulated is None:
            return False
        try:
            # Note that period is in context
            return (
                (set(OpenFiscaCaller.already_simulated['context']['id']) == set(context['id'])) &
                (OpenFiscaCaller.already_simulated['context']['period'] == context['period'])  # period changes
                )  # TODO: should test equality of contests of all entities
        except:
            import pdb
            pdb.set_trace()

    def compute(self, context, varname, expr=None, filter=None):
        # TODO: check that openfisca column entity matches liam column entity

        selected = expr_eval(filter, context)
        context = context.subset(selected)

        period_str = str(context['period'] // 100)
        if OpenFiscaCaller.no_need_to_reload(context):
            simulation = OpenFiscaCaller.already_simulated['simulation']
        else:
            simulation = get_openfisca_simulation(period_str, til_entity_by_til_entity_name = context.entities)  # Create, initalize or update tax_benefit_system simulation

        from openfisca_core import periods
        openfisca_period = periods.period(str(context['period'] // 100))

        result = simulation.calculate(varname, period = openfisca_period)
        OpenFiscaCaller.already_simulated = {
            'context': context,
            'simulation': simulation,
            }

        output = -1 * np.ones(len(selected))  # Initialisation of output to deal with unelected
        output[selected] = result.astype(float)
        return output


def deal_with_role(role, ident):
    ''' change qui to have a unique qui by ident
        assumes that there is an accumulation (on role=2) '''
    order = np.lexsort((role, ident))
    diff_ident = np.ones(role.shape, role.dtype)
    diff_ident[1:] = np.diff(ident[order])
    srole = role[order].copy()
    diff_role = np.ones(role.shape, role.dtype)
    diff_role[1:] = np.diff(srole)
    cond = (diff_ident == 0) & (diff_role == 0)
    while sum(cond) > 0:
        srole[cond] += 1
        diff_role[1:] = np.diff(srole)
        cond = (diff_ident == 0) & (diff_role == 0)
    role[order] = srole
    return role


def get_openfisca_simulation(period_str, til_entity_by_til_entity_name = None):

    import openfisca_france
    from openfisca_core import simulations
    from openfisca_core import periods
    from scipy.stats import rankdata

    assert til_entity_by_til_entity_name is not None
    TaxBenefitSystem = openfisca_france.init_country()
    tax_benefit_system = TaxBenefitSystem()
    print(period_str)
    period = periods.period(period_str)
    simulation = simulations.Simulation(
        period = period,
        debug = None,
        debug_all = None,
        tax_benefit_system = tax_benefit_system,
        trace = None,
        )

    input_variables_by_entity_name = dict()
    required_variables_by_entity_name = dict()  # pour conserver les valeurs que l'on va vouloir sortir de of.
    # pour chaque entité d'open fisca
    # load data :
    selected_rows = {}
    for openfisca_entity_name, openfisca_entity in tax_benefit_system.entity_class_by_key_plural.iteritems():
        input_variables_by_entity_name[openfisca_entity_name] = []
        required_variables_by_entity_name[openfisca_entity_name] = []
        # on cherche l'entité corrspondante dans liam
        til_entity_name = openfisca_entity_name if openfisca_entity_name != 'familles' else 'menages'
        til_entity = til_entity_by_til_entity_name[til_entity_name]
        til_array = til_entity.array.columns

        if openfisca_entity.is_persons_entity:
            selected = (til_array['idmen'] >= 10) & (til_array['idfoy'] >= 10)
            til_array['quimen'][selected] = deal_with_role(
                til_array['quimen'][selected],
                til_array['idmen'][selected]
                )
            til_array['quifoy'][selected] = deal_with_role(
                til_array['quifoy'][selected],
                til_array['idfoy'][selected]
                )
        else:
            selected = np.ones(len(til_array['id']), dtype=bool)
            selected[til_array['id'] < 10] = False

        selected_rows[openfisca_entity_name] = selected
        openfisca_entity.step_size = sum(selected)
        openfisca_entity.count = sum(selected)

        openfisca_entity.roles_count = 10  # TODO: faire une fonction (max du nombre d'enfant ?

        # pour toutes les variables de l'entité openfisca
        for column_name, column in openfisca_entity.column_by_name.iteritems():
            rename_variables = {
                'statmarit': 'civilstate',
                'quifam': 'quimen',
                'quimen': 'quimen',
                'quifoy': 'quifoy',
                }

            new_ident = {
                'noi': 'id',
                'idmen': 'idmen',
                'idfoy': 'idfoy',
                'idfam': 'idmen'
                }
            used_as_input_variables = ['age', 'age_en_mois', 'nbF']

            if column_name in list(set(rename_variables.keys() + new_ident.keys() + til_array.keys())):
                print(column_name)
                holder = simulation.get_or_new_holder(column_name)

                # Select input variables
                if column.is_input_variable() or column_name in used_as_input_variables:
                    input_variables_by_entity_name[openfisca_entity_name].append(column_name)
                    if column_name in new_ident:
                        ident = til_array[new_ident[column_name]][selected]
                        holder.set_input(simulation.period, rankdata(ident, 'dense').astype(int) - 1)
                    elif column_name in til_array:
                        if column_name == 'nbF':
                            print('Hitting ' + column_name)
                            print(min(til_array[column_name][selected]))
                            print(max(til_array[column_name][selected]))
                        holder.set_input(simulation.period, til_array[column_name][selected])
                    else:
                        holder.set_input(simulation.period, til_array[rename_variables[column_name]][selected])
                # record other required variables
                else:
                    required_variables_by_entity_name[openfisca_entity_name].append(column_name)

    # check_structure(simulation)
    # loading outputs
    for entity_name, required_variables in required_variables_by_entity_name.iteritems():
        til_entity_name = entity_name if entity_name != 'familles' else 'menages'
        til_entity = til_entity_by_til_entity_name[til_entity_name]
        til_array = til_entity.array.columns
        selected = selected_rows[entity_name]
        test = simulation.calculate('nbptr', period = period)
        if min(test) > 0:
            print('test passed')
        # import pdb; pdb.set_trace()
        # assert min(test) > 0
        for variable in required_variables:
            print(period)
            print(variable)
            # import pdb; pdb.set_trace()

            til_array[variable][selected] = simulation.calculate_add(variable, period = period)
            # TODO: check incidence of following line
            til_array[variable][~selected] = 0

    return simulation


class Pension(FilteredExpression):

    no_eval = ('filter', 'varname', 'regime')
    already_simulated = None

    @classmethod
    def no_need_to_reload(cls, context, yearleg):
        if Pension.already_simulated is None:
            return False

        try:
            # Note that period is in context
            return (
                (Pension.already_simulated['yearleg'] == yearleg) &  # legislation changes
                (set(Pension.already_simulated['context']['id']) == set(context['id'])) &
                (Pension.already_simulated['context']['period'] == context['period'])  # period changes
                )
        except:
            import pdb
            pdb.set_trace()


    def compute(self, context, varname, regime, expr=None, filter=None, yearleg=None):

        selected = expr_eval(filter, context)
        context = context.subset(selected)
        # determine yearleg
        if yearleg is None:
            yearleg = context['period'] // 100
            # if yearleg > 2009:  # TODO: remove when yearleg > 2009 possible
            #     yearleg = 2009

        if Pension.no_need_to_reload(context, yearleg):
            simul = Pension.already_simulated['simul']
        else:
            print('yearleg: {}'.format(yearleg))
            # try:
            simul = get_pension(context, yearleg)
            # except:
            #     import pdb
            #     pdb.set_trace()

        result = simul.calculate(varname, regime)
        Pension.already_simulated = {'context': context,
                                     'yearleg': yearleg,
                                     'simul': simul,
                                     }

        output = -1 * np.ones(len(selected))
        # TODO: understant why result is not float
        output[selected] = result.astype(float)
        return output


functions = {
    'add_time_scale': TimeScale,
    'add_time': AddTime,
    'year': Year,
    'month': Month,
    'pension': Pension,
    'openfisca_calculate': OpenFiscaCaller
}
