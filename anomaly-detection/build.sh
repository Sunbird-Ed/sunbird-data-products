#!/bin/bash

cd "$(dirname "$0")"

rm -rf .local || true
pybuilderInstalled=`pip3 freeze | grep 'pybuilder' | wc -l`

if [ $pybuilderInstalled != 1 ]
then
   echo "Installing pybuilder"
   pip3 install -U pybuilder --pre
fi
export PATH=$PATH:$(pwd)/.local/bin

pyb

if [ ! -d "bin" ]; then
  mkdir 'bin'
fi

cp target/dist/anomaly_detection*/dist/* bin/
mv bin/anomaly_detection-*.tar.gz bin/anomaly_detection.tar.gz
mv bin/anomaly_detection-*.whl bin/anomaly_detection.whl

rm -rf target
