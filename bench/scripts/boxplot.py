import matplotlib.pyplot as plt
import numpy as np

# fake up some data
spread = np.random.rand(50) * 100
center = np.ones(25) * 50
flier_high = np.random.rand(10) * 100 + 100
flier_low = np.random.rand(10) * -100
data = np.concatenate((spread, center, flier_high, flier_low), 0)

# basic plot
#plt.boxplot(data)

# notched plot
# plt.figure()
# plt.boxplot(data, 1)

# # change outlier point symbols
# plt.figure()
# plt.boxplot(data, 0, 'gD')

# # don't show outlier points
# plt.figure()
# plt.boxplot(data, 0, '')

# # horizontal boxes
# plt.figure()
# plt.boxplot(data, 0, 'rs', 0)

# # change whisker length
# plt.figure()
# plt.boxplot(data, 0, 'rs', 0, 0.75)

# fake up some more data
spread = np.random.rand(50) * 100
center = np.ones(25) * 40
flier_high = np.random.rand(10) * 100 + 100
flier_low = np.random.rand(10) * -100
d2 = np.concatenate((spread, center, flier_high, flier_low), 0)
data.shape = (-1, 1)
d2.shape = (-1, 1)
# data = concatenate( (data, d2), 1 )
# Making a 2-D array only works if all the columns are the
# same length.  If they are not, then use a list instead.
# This is actually more efficient because boxplot converts
# a 2-D array into a list of vectors internally anyway.
#data = [data, d2, d2[::2, 0]]

edge_no_tc = [15262,
9592,
7812,
7900,
8218,
8403,
7912,
7683,
8407,
13104,
7771,
10515,
9490,
8782,
8862,
8445,
8114,
7812,
7742,
7457,
7970,
8762,
7781,
20751,
10600,
9645,
7713,
8411,
8414,
8207]

edge_tc = [13255,
9930,
9755,
10635,
10456,
9201,
11261,
13113,
10677,
14000,
10895,
9563,
9384,
14305,
13917,
13499,
12024,
11678,
9660,
22068,
12167,
9557,
11112,
9142,
11960,
9614,
10847,
11092,
19430,
12862]

cloud_tc = [34312,
33389,
34171,
34762,
34102,
34050,
33433,
33888,
33380,
33601,
33323,
34078,
33094,
33401,
33665,
32915,
33511,
33867,
33505,
34248,
33530,
34109,
33625,
33929,
34642,
36099,
34408,
34075,
33803,
33013
]

# half_and_half = []

data = [edge_no_tc, edge_tc, cloud_tc]
# multiple box plots on one figure
fig = plt.figure()
ax = fig.add_subplot(111)
plt.boxplot(data, notch=False, sym='+', vert=True, whis=10.5,
        positions=None, widths=None, patch_artist=False,
        bootstrap=None, usermedians=None, conf_intervals=None)
ax.set_xticklabels(["All on Edge (no tc)", "All on Edge (tc)", "All on Cloud (tc)"])
plt.xlabel("Configuration")
plt.ylabel("Microseconds")

plt.show()