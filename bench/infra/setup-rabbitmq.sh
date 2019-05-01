#!/usr/bin/env bash
ssh -o "StrictHostKeyChecking no" -t ubuntu@`terraform output cloud_dns` \
    "/tmp/setup-remote-exchange.sh `terraform output edge_dns`"
ssh -o "StrictHostKeyChecking no" -t ubuntu@`terraform output edge_dns` \
    "/tmp/setup-remote-exchange.sh `terraform output cloud_dns`"
