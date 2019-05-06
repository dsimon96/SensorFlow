Names are of the form diff-<placement>-<network conditions>.txt

placements:
all-cloud - input to cloud, process on cloud, output from cloud
all-edge - input to edge, process on edge, output from edge
via-edge - input to edge, process on cloud, output from edge
split    - input to edge, process on both, output from edge

Network conditions is either no-tc (no traffic control), Pop (point of 
presence) or local (local IoT gateway). Methodology for determining simulated
network conditions:

Latency:
IotToLocal is based on typical Wi-Fi LAN latency.
IotToPop is based on ping to Akamai CDNs.
LocalToCloud is based on ping to nearest AWS datacenter.
PopToCloud is the difference between the previous two.

Bandwidth:
IotToLocal is based on typical Wi-Fi LAN bandwidth.
IotToPop is based on typical ISP last-mile bandwidth.
LocalToCloud is based on typical ISP last-mile bandwidth.
PopToCloud is assumed to be a high-bandwidth connection.

Simulated conditions
IotToLocal: 300Mbps bandwidth, 3ms ping
IotToPop: 50Mbps bandwidth, 8ms ping
LocalToCloud: 50Mbps bandwidth, 18ms ping
PopToCloud: 1Gbps bandwidth, 10ms ping

