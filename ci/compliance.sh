#!/bin/sh

set -e

apt-get -y install git
python -m pip install pre-commit

pre-commit run -v --all-files