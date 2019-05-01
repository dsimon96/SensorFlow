#!/usr/bin/env bash
scp $1 ubuntu@`terraform output cloud_dns`:~
scp $1 ubuntu@`terraform output edge_dns`:~
