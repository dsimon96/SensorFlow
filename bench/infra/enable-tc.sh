#!/usr/bin/env bash
PopToCloud='sudo tc qdisc add dev ens5 root netem delay 10ms rate 1000000kbit'
LocalToCloud='sudo tc qdisc add dev ens5 root netem delay 18ms rate 50000kbit'
IotToPop='sudo tc qdisc add dev ens5 root netem delay 8ms rate 50000kbit'
IotToLocal='sudo tc qdisc add dev ens5 root netem delay 3ms rate 300000kbit'

ssh -o "StrictHostKeyChecking no" -t ubuntu@`terraform output cloud_dns` \
    $LocalToCloud
ssh -o "StrictHostKeyChecking no" -t ubuntu@`terraform output iot_dns` \
    $IotToLocal
