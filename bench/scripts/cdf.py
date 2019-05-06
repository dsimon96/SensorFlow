import numpy as np
import matplotlib.pyplot as plt

MICROSECONDS = 1000

# Thanks Stacked Overflow post https://stackoverflow.com/questions/24575869/read-file-and-plot-cdf-in-python

conn_type = "pop" # "local" or "no-tc" or "pop"

cloud_local = sorted(set(np.loadtxt('../results/diff-all-cloud-local.txt') / MICROSECONDS))
edge_local = sorted(set(np.loadtxt('../results/diff-all-edge-local.txt') / MICROSECONDS))
split_local = sorted(set(np.loadtxt('../results/diff-split-local.txt') / MICROSECONDS))
via_edge_local = sorted(set(np.loadtxt('../results/diff-via-edge-local.txt') / MICROSECONDS))

cloud_no_tc = sorted(set(np.loadtxt('../results/diff-all-cloud-no-tc.txt') / MICROSECONDS))
edge_no_tc = sorted(set(np.loadtxt('../results/diff-all-edge-no-tc.txt') / MICROSECONDS))
split_no_tc = sorted(set(np.loadtxt('../results/diff-split-no-tc.txt') / MICROSECONDS))
via_edge_no_tc = sorted(set(np.loadtxt('../results/diff-via-edge-no-tc.txt') / MICROSECONDS))

cloud_pop = sorted(set(np.loadtxt('../results/diff-all-cloud-pop.txt') / MICROSECONDS))
edge_pop = sorted(set(np.loadtxt('../results/diff-all-edge-pop.txt') / MICROSECONDS))
split_pop = sorted(set(np.loadtxt('../results/diff-split-pop.txt') / MICROSECONDS))
via_edge_pop = sorted(set(np.loadtxt('../results/diff-via-edge-pop.txt') / MICROSECONDS))

data_len = len(cloud_local)

#print(split_local)

# Choose how many bins you want here
#num_bins = 15

# Use the histogram function to bin the data

def get_cdf(cdf_data):
	bins = np.append(cdf_data, cdf_data[-1]+1)
	counts, bin_edges = np.histogram(cdf_data, bins=bins, density=False)
	counts = counts.astype(float) / data_len
	return np.cumsum(counts), bin_edges

"""if (conn_type == "local"):
	c_counts, c_bin_edges = np.histogram(cloud_local, bins=num_bins, density=False, normed=True)
	e_counts, e_bin_edges = np.histogram(edge_local, bins=num_bins, normed=True)
	s_counts, s_bin_edges = np.histogram(split_local, bins=num_bins, normed=True)
	v_counts, v_bin_edges = np.histogram(via_edge_local, bins=num_bins, normed=True)
elif (conn_type == "pop"):
	c_counts, c_bin_edges = np.histogram(cloud_pop, bins=num_bins, normed=True)
	e_counts, e_bin_edges = np.histogram(edge_pop, bins=num_bins, normed=True)
	s_counts, s_bin_edges = np.histogram(split_pop, bins=num_bins, normed=True)
	v_counts, v_bin_edges = np.histogram(via_edge_pop, bins=num_bins, normed=True)
else: # "no-tc"
	c_counts, c_bin_edges = np.histogram(cloud_no_tc, bins=num_bins, normed=True)
	e_counts, e_bin_edges = np.histogram(edge_no_tc, bins=num_bins, normed=True)
	s_counts, s_bin_edges = np.histogram(split_no_tc, bins=num_bins, normed=True)
	v_counts, v_bin_edges = np.histogram(via_edge_no_tc, bins=num_bins, normed=True)"""


# Now find the cdf
# c_cdf = np.cumsum(c_counts)
# e_cdf = np.cumsum(e_counts)
# s_cdf = np.cumsum(s_counts)
# v_cdf = np.cumsum(v_counts)


#print(sum(c_cdf))
# print(sum(v_cdf))

if conn_type == "local":
	c_cdf, c_bin_edges = get_cdf(cloud_local)
	e_cdf, e_bin_edges = get_cdf(edge_local)
	s_cdf, s_bin_edges = get_cdf(split_local)
	v_cdf, v_bin_edges = get_cdf(via_edge_local)
elif conn_type == "pop":
	c_cdf, c_bin_edges = get_cdf(cloud_pop)
	e_cdf, e_bin_edges = get_cdf(edge_pop)
	s_cdf, s_bin_edges = get_cdf(split_pop)
	v_cdf, v_bin_edges = get_cdf(via_edge_pop)
else:
	c_cdf, c_bin_edges = get_cdf(cloud_local)
	e_cdf, e_bin_edges = get_cdf(edge_local)
	s_cdf, s_bin_edges = get_cdf(split_local)
	v_cdf, v_bin_edges = get_cdf(via_edge_local)

# And finally plot the cdf
fig = plt.figure()
ax = fig.add_subplot(111)

c_plot = plt.plot(c_bin_edges[1:], c_cdf, label="All Cloud")
e_plot = plt.plot(e_bin_edges[1:], e_cdf, label="All Edge")
s_plot = plt.plot(s_bin_edges[1:], s_cdf, label="Split")
v_plot = plt.plot(v_bin_edges[1:], v_cdf, label="Via Edge")

plt.xlabel("Milliseconds")
plt.ylabel("Probability")
#plt.legend([c_plot, e_plot, s_plot, v_plot], ["All Cloud", "All Edge", "Split", "Via Edge"])
#plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.legend(bbox_to_anchor=(.7, .95), loc=2, borderaxespad=0.)

axes = plt.gca()
axes.set_ylim([0, 1.0])
axes.set_xlim([0,70])


if conn_type == "local": plot_title = "Latency CDF for Local IoT Gateway Network Connection"
elif conn_type == "pop": plot_title = "Latency CDF for Point of Presence Network Connection"
else: plot_title = "Latency CDF without tc"
plt.title(plot_title)

plt.show()