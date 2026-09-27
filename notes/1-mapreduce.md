# Mapreduce notes


## Data locality

The fact that MapReduce workers are the same nodes that store the data via GFS seems to be the
solution to the I/O problem i was wondering about. My queestion being: how is MapReduce even able
to efficiently process large amounts of data with such simple operations without being limited by
network bandwith?

Also makes me question the value of MapReduce without this data locality feature. Is it only useful
with local data or are there other ways around the issue?

## Task granularity

O(M + R) compute, O(M * R) memory for the master process is the main limiting factor.
1 byte of memory per map/reduce task pair is kind of crazy
M and R should be much bigger than the amount of worker machines


## 5.2 Grep

If there are 10^10 100 byte files (1tb), and M=15000, how can the input delay statement mention 1000
input files?
64mb pieces, 1000 files per machine, 1700 machines. Not sure how the math works out.

Processed 30gb/s 



