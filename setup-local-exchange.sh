#!/usr/bin/env bash
set -x

# create user
sudo rabbitmq-plugins enable rabbitmq_federation
sudo rabbitmqctl add_user sf-admin buzzword

# create vhosts 'cloud' and 'edge'
sudo rabbitmqctl add_vhost cloud
sudo rabbitmqctl add_vhost edge

# give user permissions on both vhosts
sudo rabbitmqctl -p cloud set_permissions sf-admin '.*' '.*' '.*'
sudo rabbitmqctl -p edge set_permissions sf-admin '.*' '.*' '.*'

# on each vhost, fedrate exchanges beginning with 'sf.'
sudo rabbitmqctl -p cloud set_policy federate-me '^sf\.' \
	'{"federation-upstream-set":"all"}'
sudo rabbitmqctl -p edge set_policy federate-me '^sf\.' \
	'{"federation-upstream-set":"all"}'

# on each vhost add the other as an upstream
sudo rabbitmqctl -p cloud set_parameter federation-upstream name \
	'{"uri":"amqp:///edge","max-hops":1}'
sudo rabbitmqctl -p edge set_parameter federation-upstream name \
	'{"uri":"amqp:///cloud","max-hops":1}'
