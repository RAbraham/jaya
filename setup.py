# -*- coding: utf-8 -*-

from setuptools import setup

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
    url='https://github.com/scoremedia/jaya-aws',
    license=license,
    packages=['jaya'],
    install_requires=[
        'dill',
        'boto3',
        'sqlalchemy',
        'sqlalchemy-redshift',
        'click',
        'sajan'
    ],
    entry_points={
        'console_scripts': [
            'jaya=jaya.deployment.jaya_deploy:handle'
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
