#from distutils.core import setup
from setuptools import setup

setup(name='revterm',
    version='1.0a1',
    description="A CLI for Working with Revori's SQL Interface.",
    url="http://readytalk.github.com/revori",
    classifiers=[
      'Programming Language :: Python',
      'Programming Language :: Python :: 3'
    ],
    license='ISC',
    packages=['revterm'],
    include_package_data=True,
    test_suite = 'revterm.tests')
