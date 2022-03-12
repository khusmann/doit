from setuptools import setup

setup(
    name='doit',
    version='0.1.0',
    py_modules=['doit'],
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'doit = doit.main:cli',
        ],
        'datasette': [
            'doit = doit.datasette'
        ],
    },
)