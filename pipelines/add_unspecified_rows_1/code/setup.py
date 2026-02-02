from setuptools import setup, find_packages
setup(
    name = 'add_unspecified_rows_1',
    version = '1.0',
    packages = find_packages(include = ('add_unspecified_rows_1*', )) + ['prophecy_config_instances.add_unspecified_rows_1'],
    package_dir = {'prophecy_config_instances.add_unspecified_rows_1' : 'configs/resources/add_unspecified_rows_1'},
    package_data = {'prophecy_config_instances.add_unspecified_rows_1' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.9'],
    entry_points = {
'console_scripts' : [
'main = add_unspecified_rows_1.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
