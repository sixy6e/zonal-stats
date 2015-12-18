# zonal-stats

Example workflows for doing zonal statistics (polygonal statistics) with the agdc, see https://github.com/GeoscienceAustralia/agdc for details.

Luigi, https://github.com/spotify/luigi,  is used to parallelise the job within a HPC environment.
The code base is Python and file outputs are HDF5 PyTables, http://www.pytables.org/, using the pandas, http://pandas.pydata.org/, module.
