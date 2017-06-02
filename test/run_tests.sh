#!/bin/bash
CWD_BEFORE_SCRIPT=`pwd`
cd "$(dirname "$0")/.."
env | grep VIRTUAL_ENV 2>/dev/null
if [ "$?" = "0" ]
then
  echo "Running in VIRTUAL_ENV=${VIRTUAL_ENV}"
else
  echo "You should consider running in a virtual environment http://python-guide-pt-br.readthedocs.io/en/latest/dev/virtualenvs/"
  echo "If you know what you do you could change this script to drop this requirement -> Exiting for now"
  exit 33
fi

pip freeze | grep "nose==" 2>/dev/null
if [ "$?" = "0" ]
then
  echo "nose is already installed run all tests"
else
  pip install nose
fi

nosetests

cd "${CWD_BEFORE_SCRIPT}"
