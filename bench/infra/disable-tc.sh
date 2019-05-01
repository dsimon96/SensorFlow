#!/usr/bin/env bash
ResetTC='sudo tc qdisc del dev ens5 root'

ssh -o "StrictHostKeyChecking no" -t ubuntu@`terraform output cloud_dns` \
    $ResetTC
