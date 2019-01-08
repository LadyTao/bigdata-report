# User-Order API

## prerequisites

python2.7.5 or above with pip and virtualenv support

## Install
virtualenv __VENV_NAME__

source __VENV_NAME__/bin/activate

pip install -r requirements.txt

## Config

modify ettings.sample.py and save as settings.py

## Execute

source __VENV_NAME__/bin/activate

python backend.py


## Test Example

order2user
- http://__IP__:__PORT__/order2user?q={"page":0, "license": "Yes", "marketdep": ["VP营销部"], "language":["English"]}

user_csv
- http://__IP__:__PORT__/user_csv?q={"marketdep":["VP营销部"],"inputtime":{"start":"2018-10-18","end":"2018-10-18"}}

