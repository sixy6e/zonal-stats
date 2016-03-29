#!/usr/bin/env python

from distutils.core import setup


setup(name='zonal_stats',
      version='0.1b',
      packages=['zonal_stats'],
      package_dir={'zonal_stats': 'zonal_stats'},
      scripts=['query/class_db_query.py', 'query/query.py',
               'workflow/classifier_workflow.py',
               'workflow/classifier_workflow_abs.py',
               'workflow/workflow.py',
               'workflow/combine_stats.py'],
      author='Josh Sixsmith',
      license='Apache 2.0')
