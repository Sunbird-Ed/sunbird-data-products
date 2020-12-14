#!/bin/bash

cd "$(dirname "$0")"

rm -rf .local || true
pybuilderInstalled=`pip freeze | grep 'pybuilder' | wc -l`

if [ $pybuilderInstalled != 1 ]
then
   echo "Installing pybuilder"
   pip install pybuilder --pre
fi
export PATH=$PATH:$(pwd)/.local/bin

pyb

if [ ! -d "bin" ]; then
  mkdir 'bin'
fi

cp target/dist/dataproducts*/dist/* bin/
mv bin/dataproducts-*.tar.gz bin/dataproducts.tar.gz
mv bin/dataproducts-*.whl bin/dataproducts.whl

rm -rf target