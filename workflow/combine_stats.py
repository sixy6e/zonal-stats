#!/usr/bin/env python

import os
from os.path import join as pjoin, dirname, basename
import cPickle as pickle
import argparse

import luigi
import numpy
import pandas
import ogr

from eotools.vector import retrieve_attribute_table

CONFIG = luigi.configuration.get_config()


def combine_all_cells():
    # Config params
    base_out_dir = CONFIG.get('work', 'output_directory')
    base_out_fname = CONFIG.get('outputs', 'final_output_filename')
    cells_list_fname = pjoin(base_out_dir,
                             CONFIG.get('outputs', 'cells_list'))
    combined_cell_stats_fname = CONFIG.get('outputs',
                                           'combined_cell_stats_filename')
    vector_fname = CONFIG.get('work', 'vector_filename')
    ngroups = int(CONFIG.get('internals', 'pandas_groups'))
    chunksize = int(CONFIG.get('internals', 'pandas_chunksize'))

    with open(cells_list_fname, 'r') as infile:
        cells = pickle.load(infile)


    headings = ['SID', 'Timestamp', 'Band', 'Observed_Count', 'Min', 'Max',
                'Sum', 'Sum_of_Squares']

    items = ['Timestamp', 'SID', 'Band']

    tmp1_fname = pjoin(base_out_dir, 'tmp_combined_results.h5')
    tmp1_store = pandas.HDFStore(tmp1_fname)

    for cell in cells:
        cell_dir = pjoin(base_out_dir, cell)
        cell_stats_fname = pjoin(cell_dir, combined_cell_stats_fname)

        # Open the current cells WC result file
        try:
            store = pandas.HDFStore(cell_stats_fname, 'r')
        except IOError:
            print "No stats result for cell: {}".format(cell)
            continue

        if '/data' in store.keys():
            # We have data to retrieve
            df = store['data']
            df.drop('index', 1, inplace=True)
            tmp1_store.append('data', df, index=False)
            tmp1_store.flush()

        store.close()

    tmp1_store.close()

    # Chunking method
    # http://stackoverflow.com/questions/25459982/trouble-with-grouby-on-millions-of-keys-on-a-chunked-file-in-python-pandas/25471765#25471765
    # http://stackoverflow.com/questions/15798209/pandas-group-by-query-on-large-data-in-hdfstore/15800314#15800314

    # We need to group as many records we can into more memory manageable
    # chunks, that also balance well for I/O
    tmp1_store = pandas.HDFStore(tmp1_fname)
    tmp2_fname = pjoin(base_out_dir, 'tmp_grouped_results.h5')
    tmp2_store = pandas.HDFStore(tmp2_fname)

    for chunk in tmp1_store.select('data', chunksize=chunksize):
        g = chunk.groupby(chunk['SID'] % ngroups)
        for grp, grouped in g:
            tmp2_store.append('group_{}'.format(int(grp)), grouped,
                              data_columns=headings, index=False)
            tmp2_store.flush()

    tmp1_store.close()
    tmp2_store.close()

    # Define the output file
    out_fname = pjoin(base_out_dir, base_out_fname)
    combined_store = pandas.HDFStore(out_fname, 'w')

    new_headings = ['FID', 'Timestamp', 'Band', 'Observed_Count', 'Min', 'Max',
                    'Sum', 'Sum_of_Squares', 'Mean', 'Variance', 'StdDev']

    # Now read the grouped data and write to disk
    tmp2_store = pandas.HDFStore(tmp2_fname)
    for key in tmp2_store.keys():
        grouped = tmp2_store[key].groupby(items, as_index=False)
        df = grouped.agg({'Observed_Count': numpy.sum, 'Min': numpy.min,
                          'Max': numpy.max, 'Sum': numpy.sum,
                          'Sum_of_Squares': numpy.sum})

        # Account for the offset between the feature and segment ID's
        df['SID'] = df['SID'] - 1

        # Change the segment id column name to feature id
        df.rename(columns={'SID': 'FID'}, inplace=True)

        # Calculate the mean, variance, stddev
        df['Mean'] = df['Sum'] / df['Observed_Count']
        df['Variance'] = ((df['Sum_of_Squares'] - (df['Observed_Count'] *
                                                   df['Mean']**2)) / 
                          (df['Observed_Count'] - 1))
        df['StdDev'] = numpy.sqrt(df['Variance'].values)

        # Write the group to disk
        combined_store.append('data', df, data_columns=new_headings)
        combined_store.flush()

    # Add metadata
    metadata_group = 'Metadata'
    metadata = {'Vector_Filename': basename(vector_fname)}
    metadata = pandas.DataFrame(metadata, index=[0])
    combined_store[metadata_group] = metadata

    # Add the vector attribute table
    attribute_table = retrieve_attribute_table(vector_fname)
    combined_store['attribute_table'] = pandas.DataFrame(attribute_table)

    # Save and close the file
    combined_store.close()
    tmp1_store.close()
    tmp2_store.close()

    # Clean up temporary files
    os.remove(tmp1_fname)
    os.remove(tmp2_fname)


def combine_all_cells_distribution():
    """
    
    """
    # Config params
    base_out_dir = CONFIG.get('work', 'output_directory')
    base_out_fname = CONFIG.get('outputs', 'final_output_filename')
    cells_list_fname = pjoin(base_out_dir,
                             CONFIG.get('outputs', 'cells_list'))
    combined_cell_stats_fname = CONFIG.get('outputs',
                                           'combined_cell_stats_filename')
    vector_fname = CONFIG.get('work', 'vector_filename')
    ngroups = int(CONFIG.get('internals', 'pandas_groups'))
    chunksize = int(CONFIG.get('internals', 'pandas_chunksize'))

    with open(cells_list_fname, 'r') as infile:
        cells = pickle.load(infile)


    # 1st stage, combine all the results from each cell into a single file
    tmp1_fname = pjoin(base_out_dir, 'tmp_combined_results.h5')
    tmp1_store = pandas.HDFStore(tmp1_fname, 'w')

    for cell in cells:
        cell_dir = pjoin(base_out_dir, cell)
        cell_stats_fname = pjoin(cell_dir, combined_cell_stats_fname)

        # Open the current cells WC result file
        store = pandas.HDFStore(cell_stats_fname, 'r')

        if '/data' in store.keys():
            # We have data to retrieve
            df = store['data']
            headings = df.columns.values.tolist()
            df.drop('index', 1, inplace=True)
            tmp1_store.append('data', df, index=False)
            tmp1_store.flush()

        store.close()

    tmp1_store.close()

    # Chunking method
    # http://stackoverflow.com/questions/25459982/trouble-with-grouby-on-millions-of-keys-on-a-chunked-file-in-python-pandas/25471765#25471765
    # http://stackoverflow.com/questions/15798209/pandas-group-by-query-on-large-data-in-hdfstore/15800314#15800314

    # We need to group as many records we can into more memory manageable
    # chunks, that also balance well for I/O
    tmp1_store = pandas.HDFStore(tmp1_fname)
    tmp2_fname = pjoin(base_out_dir, 'tmp_grouped_results.h5')
    tmp2_store = pandas.HDFStore(tmp2_fname, 'w')

    for chunk in tmp1_store.select('data', chunksize=chunksize):
        g = chunk.groupby(chunk['SID'] % ngroups)
        for grp, grouped in g:
            tmp2_store.append('group_{}'.format(int(grp)), grouped,
                              data_columns=headings, index=False)
            tmp2_store.flush()

    tmp1_store.close()
    tmp2_store.close()

    # TODO We need a generic way of allowing a user to insert custom
    # classification headings as opposed to the numeric code
    # without begin specific like we did for WOfS.
    new_headings = ['FID' if x == 'SID' else x for x in headings]
    # items = ['Timestamp', 'SID']
    items = ['SID', 'Year', 'Month']

    # Define the output file
    out_fname = pjoin(base_out_dir, base_out_fname)
    combined_store = pandas.HDFStore(out_fname, 'w')

    # Now read the grouped data and write to disk
    tmp2_store = pandas.HDFStore(tmp2_fname)
    for key in tmp2_store.keys():
        df = tmp2_store[key].groupby(items, as_index=False).sum()

        # Account for the offset between the feature and segment ID's
        df['SID'] = df['SID'] - 1

        # Change the segment id column name to feature id
        df.rename(columns={'SID': 'FID'}, inplace=True)

        combined_store.append('data', df, data_columns=new_headings)
        combined_store.flush()


    # Save and close the file
    combined_store.close()
    tmp1_store.close()
    tmp2_store.close()

    # Clean up temporary files
    os.remove(tmp1_fname)
    os.remove(tmp2_fname)

if __name__ == '__main__':
    desc = ("Combines the all the statistical outputs from each cell "
            "and merges the results when required.")
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--cfg', required=True,
                        help="The config file used to drive the workflow.")
    parser.add_argument('--distribution', action='store_true',
                        help=("If set, then stats will be combined on the"
                              "assumption that the results are for a"
                              "class distribution"))
    parsed_args = parser.parse_args()
    CONFIG.add_config_path(parsed_args.cfg)

    if parsed_args.distribution:
        combine_all_cells_distribution()
    else:
        combine_all_cells()
