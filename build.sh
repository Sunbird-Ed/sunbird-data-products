#!/bin/bash

cd "$(dirname "$0")"

pybuilderInstalled=`pip freeze | grep 'pybuilder' | wc -l`

if [ $pybuilderInstalled != 1 ]
then
   echo "Installing pybuilder"
   pip install pybuilder
fi

pyb install_dependencies clean publish
tox

if [ ! -d "bin" ]; then
  mkdir 'bin'
fi

cp target/dist/dataproducts-1.0.0/dist/* bin/

# rm -rf target