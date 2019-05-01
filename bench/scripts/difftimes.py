from datetime import datetime

FMT = '%Y-%m-%dT%H:%M:%S.%f'

in_times = []
out_times = []

with open('input.txt', 'r') as infile:
    for line in infile.readlines():
        in_times.append(datetime.strptime(line.strip(), FMT))

with open('output.txt', 'r') as outfile:
    for line in outfile.readlines():
        out_times.append(datetime.strptime(line.strip(), FMT))

diffs = []

for (in_time, out_time) in zip(in_times, out_times):
    diff = out_time - in_time
    diffs.append(diff.total_seconds() * 1e6)

with open('diff.txt', 'w') as f:
    for diff in diffs:
        f.write(str(diff))
        f.write('\n')
