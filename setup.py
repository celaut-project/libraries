from setuptools import setup, find_packages

setup(
    name='celaut_libs',
    version='0.0.1',

    url='https://github.com/celaut-project/libraries.git',

    py_modules=['node_controller', 'resource_manager'],
    install_requires=[
        'pee-rpc@git+https://github.com/pee-rpc-protocol/pee-rpc',
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.11",
)
