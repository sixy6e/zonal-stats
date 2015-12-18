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
from classifier import classifier
from zonal_stats import zonal_stats
from zonal_stats import zonal_class_distribution
from image_processing.segmentation import rasterise_vector


CONFIG = luigi.configuration.get_config()
CONFIG.add_config_path(pjoin(dirname(__file__), 'config.cfg'))


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
                  'nodata': 0}

        with rasterio.open(out_fname, 'w', **kwargs) as src:
            src.write(1, res)

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

        dataset = ds_list[self.idx]
        nbar_ds = dataset.datasets[DatasetType.ARG25]
        pq_ds = dataset.datasets[DatasetType.PQ25]

        classified_img = classifier(nbar_ds, pq_ds)

        # hard code; as this will be short lived due to agdc-v2 development
        class_ids = [0, 1, 2, 3, 4, 5]

        with rasterio.open(rasterised_fname, 'r') as src:
            zones_img = src.read(1)

        result = zonal_class_distribution(classified_img, zones_img,
                                          class_ids=class_ids)

        # Set the timestamp
        result['Timestamp'] = dataset.start_datetime

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

        targets = []
        for idx, ds in enumerate(ds_list):
            timestamp = bytes(ds.start_datetime).replace(' ', '-')
            out_fname = base_name.format(timestamp)
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

    parsed_args = parser.parse_args()
    tile_idx = parsed_args.tile


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
