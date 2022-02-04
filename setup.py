from setuptools import setup

setup(
    name='doit',
    version='0.1.0',
    py_modules=['doit_san'],
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'doit-san = doit_san:cli',
        ],
    },
)