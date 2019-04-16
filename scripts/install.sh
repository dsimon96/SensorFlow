#!/bin/bash
set -ex

# add esl-erlang repository to apt
wget https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
sudo dpkg -i erlang-solutions_1.0_all.deb
rm erlang-solutions_1.0_all.deb

# add packagecloud rabbitmq-server repository to apt
curl -s https://packagecloud.io/install/repositories/rabbitmq/rabbitmq-server/script.deb.sh | sudo bash

# install erlang
sudo apt update
sudo apt -y install esl-erlang


# install RabbitMQ server
sudo apt -y install rabbitmq-server

