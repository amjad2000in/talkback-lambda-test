#!/bin/bash

if [ $# -gt 0 ] ; then
  STAGE=$1
else
  STAGE="stage"
fi

../cloudformation/build_and_deploy_function.sh tberror TBError $STAGE
