#!/bin/bash

command -v virtualenv >/dev/null 2>&1 || pip install --no-index -f ./dependencies virtualenv
virtualenv -p python production && source production/bin/activate && pip install --no-index -f ./dependencies -r requirements.txt
