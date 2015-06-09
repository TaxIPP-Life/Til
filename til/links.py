from __future__ import print_function, division

from liam2.links import Link, Count, Sum, Avg, Max, Min

from liam2.expr import Variable


class LinkValue(Variable):
    def __init__(self, name, key, missing_value):
        Variable.__init__(self, '%s.%s' % (name, key), int)


class One2One(Link):

    def get(self, key, missing_value=None):
        return LinkValue(self, key, missing_value)

    __getattr__ = get

    def count(self, target_filter=None):
        return Count(self, target_filter)

    def sum(self, target_expr, target_filter=None):
        return Sum(self, target_expr, target_filter)

    def avg(self, target_expr, target_filter=None):
        return Avg(self, target_expr, target_filter)

    def min(self, target_expr, target_filter=None):
        return Min(self, target_expr, target_filter)

    def max(self, target_expr, target_filter=None):
        return Max(self, target_expr, target_filter)
