# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='jaya',
    version='0.0.1',
    description='AWS Service Pipelines',
    long_description=readme,
    author='Rajiv Abraham',
    author_email='rajiv.abraham@gmail.com',
    url='https://github.com/scoremedia/jaya',
    license=license,
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'boto3',
        'click',
        'dill',
        'sqlalchemy',
        'sqlalchemy-redshift',
    ],
    entry_points={
        'console_scripts': [
            'jaya=jaya.cli.cli:main'
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6'
    ]

)
