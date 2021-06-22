from __future__ import absolute_import
from __future__ import print_function

from distutils.command.install import install
from sys import executable as PYTHON_PATH

import setuptools
import subprocess

class CustomInstall(install):
  custom_install_commands = [
      ['apt-get', 'update'],
      ['apt-get', 'install', '-y', 'g++', 'unixodbc-dev'],
      [PYTHON_PATH, '-m', 'pip', 'install', 'pymssql==2.1.4', 'pyodbc==4.0.26'],
  ]

  def run(self):
    for cmd in self.custom_install_commands:
      try:
        proc = subprocess.run(cmd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT,
                              check=True)
        print(proc.stdout.decode('utf-8'))
      except subprocess.CalledProcessError as e:
        print(e.stdout.decode('utf-8'))
        raise
    install.run(self)

setuptools.setup(
    name='pyodbc_pipeline',
    version='0.0.1',
    description='Pipline to test pyodbc install',
    install_requires=[],
    cmdclass={'install': CustomInstall})