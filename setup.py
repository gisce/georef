#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup pel la llibreria de Facturació ATR.
"""
import os
import shutil
import unittest
from distutils.core import setup, Command
from distutils.command.clean import clean as _clean

PACKAGES = ['georef']
PACKAGES_DATA = {}

def find_scripts():
    _scripts = []
    for r, d, f in os.walk('bin'):
        if len(f):
            for _f in f:
                _scripts.append('bin/' + _f)
    return _scripts

class Clean(_clean):
    """Eliminem el directori build i els bindings creats."""
    def run(self):
        """Comença la tasca de neteja."""
        _clean.run(self)
        if os.path.exists(self.build_base):
            print "Cleaning %s dir" % self.build_base
            shutil.rmtree(self.build_base)

setup(name='georef',
      description = 'Scripts per la georeferenciació de la CNE.',
      author='GISCE Enginyeria',
      author_email='devel@gisce.net',
      url='http://www.gisce.net',
      version='0.5.0',
      license='General Public Licence 2',
      long_description='''Long description''',
      provides=['georef'],
      requires=['multiprocessing'],
      packages=PACKAGES,
      package_data=PACKAGES_DATA,
      scripts=find_scripts(),
      cmdclass={'clean': Clean})
        
