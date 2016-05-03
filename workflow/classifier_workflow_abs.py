#!/usr/bin/env python

import luigi

import os
from os.path import join as pjoin, exists, dirname
import cPickle as pickle
import glob
import argparse
import logging

import pandas
import rasterio

from datacube.api.model import DatasetType
from zonal_stats.classifier import classify_abs
from zonal_stats import zonal_stats
from zonal_stats import zonal_class_distribution
from image_processing.segmentation import rasterise_vector


CONFIG = luigi.configuration.get_config()


class RasteriseTask(luigi.Task):

    """
    Computes the rasterisation for a cell.
    """

    out_dir = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        out_fname = pjoin(self.out_dir, 'RasteriseTask.completed')

        return luigi.LocalTarget(out_fname)

    def run(self):
        out_fname = pjoin(self.out_dir,
                          CONFIG.get('outputs', 'rasterise_filename'))
        ds_list_fname = pjoin(self.out_dir,
                              CONFIG.get('outputs', 'query_filename'))

        with open(ds_list_fname, 'r') as infile:
            ds_list = pickle.load(infile)

        vector_fname = CONFIG.get('work', 'vector_filename')

        img_fname = ds_list[0].datasets[DatasetType.FC25].path
        with rasterio.open(img_fname) as src:
            crs = src.crs
            transform = src.affine
            height = src.height
            width = src.width

        res = rasterise_vector(vector_fname, shape=(height, width),
                               transform=transform, crs=crs)

        kwargs = {'count': 1,
                  'width': width,
                  'height': height,
                  'crs': crs,
                  'transform': transform,
                  'dtype': res.dtype.name,
                  'driver': 'GTiff',
                  'nodata': 0}

        with rasterio.open(out_fname, 'w', **kwargs) as src:
            src.write(res, 1)

        # We could just set the image as the Luigi completion target...
        with self.output().open('w') as outf:
            outf.write('Complete')


class ClassifierStatsTask(luigi.Task):

    """
    Computes a zonal class distribution task for the required dataset.
    """

    idx = luigi.IntParameter()
    out_fname = luigi.Parameter()

    def requires(self):
        return [RasteriseTask(dirname(self.out_fname))]

    def output(self):
        return luigi.LocalTarget(self.out_fname)

    def run(self):
        rasterised_fname = pjoin(dirname(self.out_fname),
                                 CONFIG.get('outputs', 'rasterise_filename'))

        ds_list_fname = pjoin(dirname(self.out_fname),
                              CONFIG.get('outputs', 'query_filename'))

        with open(ds_list_fname, 'r') as infile:
            ds_list = pickle.load(infile)

        timestamps = []
        t_idx = []
        for i, tile in enumerate(ds_list):
            timestamps.append(tile.start_datetime)
            t_idx.append(i)
        df = pandas.DataFrame({'timestamp': timestamps, 'tile_idx': t_idx,
                               'tiles': ds_list})
        df.set_index('timestamp', inplace=True)
        tiles = df[self.idx]['tiles'].tolist()

        # returns a dict with enum FC bands as the keys
        classified = classify_abs(tiles)

        # hard code; as this will be short lived due to agdc-v2 development
        class_ids = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

        with rasterio.open(rasterised_fname, 'r') as src:
            zones_img = src.read(1)

        # blank dataframe to join results into
        result = pandas.DataFrame()
        merge = False
        merge_on = ['SID']

        for key in classified:
            bname = key.name
            classified_img = classified[key]
            res = zonal_class_distribution(classified_img, zones_img,
                                           class_ids=class_ids)

            # change column names
            columns = {'Class_0': 'no_data_{}'.format(bname),
                       'Class_1': 'decile_1_{}'.format(bname),
                       'Class_2': 'decile_2_{}'.format(bname),
                       'Class_3': 'decile_3_{}'.format(bname),
                       'Class_4': 'decile_4_{}'.format(bname),
                       'Class_5': 'decile_5_{}'.format(bname),
                       'Class_6': 'decile_6_{}'.format(bname),
                       'Class_7': 'decile_7_{}'.format(bname),
                       'Class_8': 'decile_8_{}'.format(bname),
                       'Class_9': 'decile_9_{}'.format(bname),
                       'Class_10': 'decile_10_{}'.format(bname),
                       'Class_11': 'decile_11_{}'.format(bname)}

            res.rename(columns=columns, inplace=True)

            # merge new columns into the existing dataframe
            # result = pandas.concat([result, res], axis=1)
            if merge:
                # merge new columns into the existing dataframe
                res.drop('PixelCount', axis=1, inplace=True)
                result = pandas.merge(result, res, on=merge_on, how='outer')
            else:
                result = res
                merge = True

        # Set the timestamp
        # result['Timestamp'] = tiles[0].start_datetime # Do we need?
        result['Year'] = tiles[0].start_datetime.year
        result['Month'] = tiles[0].start_datetime.month

        # Open the output hdf5 file
        store = pandas.HDFStore(self.output().path)

        # Write the dataframe
        store.append('data', result)

        # Save and close the file
        store.close()


class CellStatsTask(luigi.Task):

    """
    For a given cell define a classifier stats task for each required Dataset.
    """

    out_dir = luigi.Parameter()

    def requires(self):
        base_name = CONFIG.get('outputs', 'stats_filename_format')
        base_name = pjoin(self.out_dir, base_name)

        ds_list_fname = pjoin(self.out_dir,
                              CONFIG.get('outputs', 'query_filename'))

        with open(ds_list_fname, 'r') as infile:
            ds_list = pickle.load(infile)

        """
        Fleshing out a workflow
        timestamps = []
        t_idx = []
        for i, tile in enumerate(tiles):
            timestamps.append(tile.start_datetime)
            t_idx.append(i)

        df = pandas.DataFrame({'timestamp': timestamps, 'tile_idx': idx})
        df.set_index('timestamp', inplace=True)
        period = df.index.to_period("M")
        groups = df.groupby(period)
        for name, group in groups:
            print group # here maybe define a task
            name.__str__() # send this through as the param
            # which will look like u'2016-02'
        """

        targets = []
        timestamps = []
        t_idx = []
        for i, tile in enumerate(ds_list):
            timestamps.append(tile.start_datetime)
            t_idx.append(i)
        df = pandas.DataFrame({'timestamp': timestamps, 'tile_idx': t_idx})
        df.set_index('timestamp', inplace=True)
        period = df.index.to_period("M")
        groups = df.groupby(period)
        for name, group in groups:
            out_fname = base_name.format(name)
            idx = "{}".format(name)
            targets.append(ClassifierStatsTask(idx, out_fname))

        return targets

    def output(self):
        out_fname = pjoin(self.out_dir, 'CellStatsTask.completed')
        return luigi.LocalTarget(out_fname)

    def run(self):
        with self.output().open('w') as outf:
            outf.write('Completed')


class CombineCellStatsTask(luigi.Task):

    """
    Combines all stats files from a single cell into a single file.
    """

    out_dir = luigi.Parameter()

    def requires(self):
        return [CellStatsTask(self.out_dir)]

    def output(self):
        out_fname = pjoin(self.out_dir, 'CombineCellStatsTask.completed')
        return luigi.LocalTarget(out_fname)

    def run(self):
        # Get a list of the stats files for each timeslice
        stats_files_list = glob.glob(pjoin(self.out_dir, '*.h5'))

        # Create an output file that we can continually append data
        out_fname = pjoin(self.out_dir,
                          CONFIG.get('outputs',
                                     'combined_cell_stats_filename'))
        combined_store = pandas.HDFStore(out_fname)

        store = pandas.HDFStore(stats_files_list[0])

        # If there is nothing in the first file there will be nothing for
        # every file
        if '/data' in store.keys():
            # We have data to retrieve
            headings = store['data'].columns.tolist()
            store.close()
            df = pandas.DataFrame(columns=headings)

            for sfile in stats_files_list:
                store = pandas.HDFStore(sfile, 'r')
                df = df.append(store['data'])
                store.close()

            df.reset_index(inplace=True)

            # Write to disk
            combined_store.append('data', df)

        with self.output().open('w') as outf:
            outf.write('Completed')


class RunCombineCellStatsTasks(luigi.Task):

    """
    Issues CombineCellStatsTask's to each cell associated
    with the tile defined by the start and end index.
    """

    idx1 = luigi.IntParameter()
    idx2 = luigi.IntParameter()

    def requires(self):
        base_out_dir = CONFIG.get('work', 'output_directory')
        cells_list_fname = pjoin(base_out_dir,
                                 CONFIG.get('outputs', 'cells_list'))
        with open(cells_list_fname, 'r') as infile:
            cells = pickle.load(infile)

        tasks = []
        for cell in cells[self.idx1:self.idx2]:
            out_dir = pjoin(base_out_dir, cell)
            tasks.append(CombineCellStatsTask(out_dir))

        return tasks

    def output(self):
        out_dir = CONFIG.get('work', 'output_directory')
        out_fname = pjoin(out_dir,
                          'RunCombineCellStatsTasks_{}:{}.completed')
        out_fname = out_fname.format(self.idx1, self.idx2)

        return luigi.LocalTarget(out_fname)

    def run(self):
        with self.output().open('w') as outf:
            outf.write('Completed')


if __name__ == '__main__':
    # Setup command-line arguments
    desc = "Processes zonal stats for a given set of cells."
    hlp = ("The tile/chunk index to retieve from the tiles list. "
           "(Needs to have been previously computed to a file named tiles.pkl")
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--tile', type=int, help=hlp)
    parser.add_argument('--cfg', required=True,
                        help="The config file used to drive the workflow.")

    parsed_args = parser.parse_args()
    tile_idx = parsed_args.tile
    CONFIG.add_config_path(parsed_args.cfg)


    # setup logging
    log_dir = CONFIG.get('work', 'logs_directory')
    if not exists(log_dir):
        os.makedirs(log_dir)

    logfile = "{log_path}/stats_workflow_{uname}_{pid}.log"
    logfile = logfile.format(log_path=log_dir, uname=os.uname()[1],
                             pid=os.getpid())
    logging_level = logging.INFO
    logging.basicConfig(filename=logfile, level=logging_level,
                        format=("%(asctime)s: [%(name)s] (%(levelname)s) "
                                "%(message)s "))


    # Get the list of tiles (groups of cells that each node will operate on
    tiles_list_fname = pjoin(CONFIG.get('work', 'output_directory'),
                             CONFIG.get('outputs', 'tiles_list'))
    with open(tiles_list_fname, 'r') as in_file:
        tiles = pickle.load(in_file)

    # Initialise the job
    tile = tiles[tile_idx]
    tasks = [RunCombineCellStatsTasks(tile[0], tile[1])]
    luigi.build(tasks, local_scheduler=True, workers=16)
    luigi.run()
