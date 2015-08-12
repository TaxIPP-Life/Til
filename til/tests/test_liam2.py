# -*- coding: utf-8 -*-


import os
import pkg_resources


from liam2.simulation import Simulation


def test_liam2_examples_files():
    liam2_demo_directory = os.path.join(
        pkg_resources.get_distribution('liam2').location,
        'liam2',
        'tests',
        'examples'
        )
    excluded_files = [
        'demo_import.yml',  # non working example
        'demo02.yml',  # TODO: pb with figures
        ]
    yaml_files = [os.path.join(liam2_demo_directory, _file) for _file in os.listdir(liam2_demo_directory)
        if os.path.isfile(os.path.join(liam2_demo_directory, _file))
        and _file.endswith('.yml')
        and _file not in excluded_files]

    for yaml_file in yaml_files:
        print yaml_file
        simulation = Simulation.from_yaml(
            yaml_file,
            input_dir = os.path.join(liam2_demo_directory),
            # input_file = input_file,
            output_dir = os.path.join(os.path.dirname(__file__), 'output'),
            # output_file = output_file,
            )
        simulation.run(False)


def test_liam2_functionnal_files():
    liam2_functional_directory = os.path.join(
        pkg_resources.get_distribution('liam2').location,
        'liam2',
        'tests',
        'functional'
        )
    import_yaml_path = os.path.join(liam2_functional_directory, 'import.yml')
    assert os.path.exists(import_yaml_path)
    simulation = Simulation.from_yaml(
        import_yaml_path,
        input_dir = os.path.join(liam2_functional_directory),
        # input_file = input_file,
        # output_dir = os.path.join(os.path.dirname(__file__), 'output'),
        # output_file = output_file,
        )
    simulation.run(False)

    simulation_yaml_path = os.path.join(liam2_functional_directory, 'simulation.yml')
    assert os.path.exists(simulation_yaml_path)
    simulation = Simulation.from_yaml(
        simulation_yaml_path,
        input_dir = os.path.join(liam2_functional_directory),
        # input_file = input_file,
        output_dir = os.path.join(os.path.dirname(__file__), 'output'),
        # output_file = output_file,
        )
    simulation.run(False)
