#!/usr/bin/env bash
terraform taint aws_instance.cloud
terraform taint aws_instance.edge

source ./deploy.sh
