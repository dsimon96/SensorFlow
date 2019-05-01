#!/usr/bin/env bash
PopEdgeNodeWAN='sudo tc qdisc add dev ens5 root netem delay 20ms rate 1000000kbit'
LocalEdgeNodeWAN='sudo tc qdisc add dev ens5 root netem delay 20ms rate 100000kbit'
UncongestedWifi='sudo tc qdisc add dev ens5 root netem delay 2ms rate 1000000kbit'
CongestedWifi='sudo tc qdisc add dev ens5 root netem delay 2ms rate 600000kbit'

ssh -o "StrictHostKeyChecking no" -t ubuntu@`terraform output cloud_dns` \
    $LocalEdgeNodeWAN
ssh -o "StrictHostKeyChecking no" -t ubuntu@`terraform output iot_dns` \
    $UncongestedWifi
