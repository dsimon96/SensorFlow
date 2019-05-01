#!/usr/bin/env bash
set -x

# create user
sudo rabbitmq-plugins enable rabbitmq_federation
sudo rabbitmqctl add_user sf-admin buzzword
sudo rabbitmqctl set_permissions sf-admin '.*' '.*' '.*'

# on each vhost, fedrate exchanges beginning with 'sf.'
sudo rabbitmqctl set_policy federate-me '^sf\.' \
	'{"federation-upstream-set":"all"}'

# on each vhost add the other as an upstream
sudo rabbitmqctl set_parameter federation-upstream name \
	"{\"uri\":\"amqp://$1/\",\"max-hops\":1}"
