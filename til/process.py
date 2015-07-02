# -*- coding: utf-8 -*-


from __future__ import division, print_function


import importlib

try:
    from liam2.expr import Expr
    from liam2.process import Process
except ImportError:
    from src.expr import Expr
    from src.process import Process


class ExtProcess(Process):
    """these processes are not real Liam2 processes
    The file containing the function should be in the path and
    the function itself must be named "main".
    """

    def __init__(self, name, arg):
        super(ExtProcess, self).__init__(name, None)  # argument entity is set to None
        self.name = name
        self.args = arg

    def run(self, simulation, period):
        module = importlib.import_module(self.name)
        if self.args is not None:
            args = list(self.args)
            for index, arg in enumerate(self.args):
                if arg == 'period':
                    args[index] = int(period / 100)
                elif arg == 'simulation':
                    args[index] = simulation
                else:
                    args[index] = arg
            args = tuple(args)
            module.main(*args)
        else:
            module.main()

    def expressions(self):
        if isinstance(self.expr, Expr):
            yield self.expr
