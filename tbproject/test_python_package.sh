#!/bin/bash

# https://hynek.me/articles/sharing-your-labor-of-love-pypi-quick-and-dirty/

TEST_DIR="/tmp/test-sdist-$$"
echo "Testing in $TEST_DIR"
virtualenv $TEST_DIR

$TEST_DIR/bin/pip install dist/*.tar.gz

printf "import talkback\nprint(talkback.__version__)\n" > $TEST_DIR/confirm.py

$TEST_DIR/bin/python $TEST_DIR/confirm.py

rm -rf $TEST_DIR

TEST_DIR="/tmp/test-wheel-$$"
echo "Testing in $TEST_DIR"
virtualenv $TEST_DIR

$TEST_DIR/bin/pip install dist/*.whl

printf "import talkback\nprint(talkback.__version__)\n" > $TEST_DIR/confirm.py

$TEST_DIR/bin/python $TEST_DIR/confirm.py

rm -rf $TEST_DIR

