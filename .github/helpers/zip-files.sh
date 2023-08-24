#! /bin/bash

echo installing requirements..
pip install -r minimalRequirements.txt --target ./package

echo copying python files..
cp entity-counter-recalculator/main.py ./package
mkdir ./package/app
cp entity-counter-recalculator/app/* ./package/app

echo creating zip archive..
cd package && zip -r9 ../entity-counter-recalculator`date +%Y%m%d%H%M`.zip .