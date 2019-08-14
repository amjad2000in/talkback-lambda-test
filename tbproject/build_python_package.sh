#!/bin/bash

PROJECT=$(dirname $0)
pushd $PROJECT

if [ "$VIRTUAL_ENV" ] ; then
    printf "Do not run in (venv).  Run in regular shell.\n"
    popd
    exit
fi

T1=$(stat --format=%Y talkback/TBProject.py)
T2=$(stat --format %Y talkback.egg-info/PKG-INFO)

if [ "$T1" -lt "$T2" ] ; then
	echo "TBProject is up-to-date"
        popd
	exit 0 ;
fi

# prerequisites
# this fixed Error initializing plugin EntryPoint('windows', 'keyring.backends.Windows', None, Distribution('keyring', '13.2.1')).
# per https://stackoverflow.com/questions/53164278/missing-dependencies-causing-keyring-error-when-opening-spyder3-on-ubuntu18
# and https://github.com/vmware/vcd-cli/issues/235
#pip3 install keyring --force-reinstall
#pip3 install keyrings.alt --force-reinstall
pip install --user --upgrade pip setuptools wheel virtualenv keyring keyrings.alt

printf "Building TBProject python module.txt\n"
printf "Updating requirements.txt\n"
source ./venv/bin/activate


pip freeze | grep -v 'subdirectory=tbproject' > requirements.txt
deactivate


# aws `sam build` requires wheels: bdist_wheel
# the egg files are part of the wheel package: sdist
# https://pythonwheels.com/
# https://packaging.python.org/
# https://pip.pypa.io/en/stable/reference/pip_wheel/
# https://hynek.me/articles/sharing-your-labor-of-love-pypi-quick-and-dirty/

# clean build from scratch
BUILD="./build"
if [ -d "$BUILD" ] ; then
    rm -rf $BUILD
fi
python3 setup.py sdist bdist_wheel
if [ -d "$BUILD" ] ; then
    rm -rf $BUILD
fi

popd
