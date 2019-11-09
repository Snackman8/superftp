#!/bin/sh

# This build script is part of the python_app_checklist project
#
# See https://github.com/Snackman8/python_app_checklist for updated versions


# ====================================================================================================
#    CONFIGURATION OPTIONS
# ====================================================================================================
# configure PROJECT_NAME to match the name of your project
PROJECT_NAME="superftp"


# ====================================================================================================
#    BUILD SCRIPT
# ====================================================================================================
echo "\033[0;93m"
echo "--------------------------------------------------"
echo "    CLEANING                                      "
echo "--------------------------------------------------"
echo "\033[0m"
rm -rf cover
rm -rf dist
rm -rf $PROJECT_NAME.egg-info
(cd docs && make clean)
echo " "

echo "\033[0;93m"
echo "--------------------------------------------------"
echo "    PYCODESTYLE                                   "
echo "--------------------------------------------------"
echo "\033[0m"
pycodestyle --max-line-length=120 .
echo " "

echo "\033[0;93m"
echo "--------------------------------------------------"
echo "    PYLINT                                        "
echo "--------------------------------------------------"
echo "\033[0m"
(cd $PROJECT_NAME && find . -iname "*.py" | grep -v ./docs/conf.py | xargs pylint --max-line-length=120 --good-names=a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,y,v,w,x,y,z --disable=R0902,R0913,R0914)

echo "\033[0;93m"
echo "--------------------------------------------------"
echo "    NOSETESTS                                     "
echo "--------------------------------------------------"
echo "\033[0m"
nosetests --with-coverage --cover-inclusive --cover-package=$PROJECT_NAME --cover-html
echo " "

echo "\033[0;93m"
echo "--------------------------------------------------"
echo "    SPHINX                                        "
echo "--------------------------------------------------"
echo "\033[0m"
(cd docs && make clean html)
echo " "

echo "\033[0;93m"
echo "--------------------------------------------------"
echo "    PACKAGING                                     "
echo "--------------------------------------------------"
echo "\033[0m"
python setup.py -q sdist
echo " "
