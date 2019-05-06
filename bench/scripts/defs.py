CLOUD_DNS="ec2-13-59-54-224.us-east-2.compute.amazonaws.com"
EDGE_DNS="ec2-3-14-150-67.us-east-2.compute.amazonaws.com"
TOKEN="dab01e04-8097-47d9-92be-3f4577d608e0"
SEND_SUFFIX="sensor-spout"
RECV_SUFFIX="sink2"

INGRESS_HOST=EDGE_DNS
EGRESS_HOST=EDGE_DNS
SEND_PREFIX="edge"
RECV_PREFIX="cloud"

MESSAGES = []
TPUT = 20

for i in xrange(15):
    for _ in xrange(5):
        MESSAGES.append(0)
    for j in xrange(5):
        MESSAGES.append(1+j)
    for j in xrange(5):
        MESSAGES.append(5-j)
    for _ in xrange(5):
        MESSAGES.append(0)
