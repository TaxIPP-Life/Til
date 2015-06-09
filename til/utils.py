# -*- coding: utf-8 -*-


# [a,b]: a is number per year and b is the digit in the tens place to identify unit
time_period = {'month': 1, 'bimonth': 2, 'quarter': 3, 'triannual': 4, 'semester': 6, 'year': 12, 'year0': 1}


def addmonth(a, b):
    assert isinstance(a, int)  # should be a special type
    assert isinstance(b, int)
    if b >= 0:
        change_year = (a % 100) + b >= 12
        value = a + b * (1 - change_year) + (100 - 12 + b) * change_year
    if b < 0:
        change_year = (a % 100) + b < 1
        value = a + b * (1 - change_year) + (-100 + 12 + b) * change_year
    return value
