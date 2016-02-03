#!/usr/bin/env python


from datetime import date
import os
from os.path import join as pjoin, exists
import cPickle as pickle
import subprocess
import logging
import argparse
import luigi

import ogr

from datacube.api.query import list_tiles_as_list
from datacube.api.model import DatasetType, Satellite, BANDS
from eotools.vector import spatial_intersection


CONFIG = luigi.configuration.get_config()

PBS_DSH = (
"""#!/bin/bash

#PBS -P {project}
#PBS -q {queue}
#PBS -l walltime={walltime},ncpus={ncpus},mem={mem}GB,jobfs=350GB
#PBS -l wd
#PBS -me
#PBS -M {email}

NNODES={nnodes}

for i in $(seq 1 $NNODES); do
   pbsdsh -n $((16 *$i)) -- bash -l -c "{modules} PBS_NNODES=$NNODES PBS_VNODENUM=$i python {pyfile} \
   --tile $[$i - 1]" --cfg {cfgfile} &
done;
wait
""")


def get_cells():
    """
    A simple procedure to generate a list which cells that will
     be analysed throughout the workflow.
    """
    out_dir = CONFIG.get('work', 'output_directory')
    envelope = bool(CONFIG.get('internals', 'envelope'))
    vector_fname = CONFIG.get('work', 'vector_filename')
    dcube_vector_fname = CONFIG.get('internals', 'cell_grid')
    out_fname = pjoin(out_dir, CONFIG.get('outputs', 'cells_list'))

    fids = spatial_intersection(dcube_vector_fname, vector_fname,
                                envelope=envelope)

    vec_ds = ogr.Open(dcube_vector_fname)
    layer = vec_ds.GetLayer(0)

    cell_dirs = []
    cells = []
    for fid in fids:
        feat = layer.GetFeature(fid)
        xmin = int(feat.GetField("XMIN"))
        ymin = int(feat.GetField("YMIN"))
        #cell_dir = "{:03.0f}_{:04.0f}".format(xmin, ymin)
        cell_dir = "{}_{}".format(xmin, ymin)
        cell_dirs.append(cell_dir)
        cells.append((xmin, ymin))

        out_cell_dir = pjoin(out_dir, cell_dir)
        if not exists(out_cell_dir):
            os.makedirs(out_cell_dir)

    with open(out_fname, 'w') as outf:
        pickle.dump(cell_dirs, outf)

    return cells


def query_cells(cell_list, satellites, min_date, max_date, dataset_types,
                output_dir):
    """
    
    """
    base_out_fname = CONFIG.get('outputs', 'query_filename')
    for cell in cell_list:
        x_cell = [int(cell[0])]
        y_cell = [int(cell[1])]
        tiles = list_tiles_as_list(x=x_cell, y=y_cell, acq_min=min_date,
                                   acq_max=max_date,
                                   dataset_types=dataset_types,
                                   satellites=satellites)
        out_dir = pjoin(output_dir, '{}_{}'.format(cell[0], cell[1]))
        out_fname = pjoin(out_dir, base_out_fname)
        with open(out_fname, 'w') as outf:
            pickle.dump(tiles, outf)


def create_tiles(array_size, tile_size=25):
    """
    A minor function to tile a 1D array or list into smaller manageable
    portions.
    """
    idx_start = range(0, array_size, tile_size)
    idx_tiles = []
    for idx_st in idx_start:
        if idx_st + tile_size < array_size:
            idx_end = idx_st + tile_size
        else:
            idx_end = array_size
        idx_tiles.append((idx_st, idx_end))

    return idx_tiles


if __name__ == '__main__':
    desc = "Initialises the datacube query and saves the output."
    parser = argparse.ArgumentParser(description=desc)
    hlp = "The config file used to drive the workflow."
    parser.add_argument('--cfg', required=True, help=hlp)
    args = parser.parse_args()
    CONFIG.add_config_path(args.cfg)

    # Create the output directory
    out_dir = CONFIG.get('work', 'output_directory')
    if not exists(out_dir):
        os.makedirs(out_dir)

    # setup logging
    log_dir = CONFIG.get('work', 'logs_directory')
    if not exists(log_dir):
        os.makedirs(log_dir)

    logfile = "{log_path}/query_{uname}_{pid}.log"
    logfile = logfile.format(log_path=log_dir, uname=os.uname()[1],
                             pid=os.getpid())
    logging_level = logging.INFO
    logging.basicConfig(filename=logfile, level=logging_level,
                        format=("%(asctime)s: [%(name)s] (%(levelname)s) "
                                "%(message)s "))

    # Just turn these into a simple function calls. In this framework we don't
    # need a luigi workload for a simple single task
    cells = get_cells()

    # Define the fractional cover dataset
    # TODO have this defined through the config.cfg
    fc_ds_type = [DatasetType.FC25, DatasetType.PQ25]

    # Get the satellites we wish to query
    satellites = CONFIG.get('work', 'satellites')
    satellites = [Satellite(i) for i in satellites.split(',')]

    # Get the min/max date range to query
    min_date = CONFIG.get('work', 'min_date')
    min_date = [int(i) for i in min_date.split('_')]
    min_date = date(min_date[0], min_date[1], min_date[2])
    max_date = CONFIG.get('work', 'max_date')
    max_date = [int(i) for i in max_date.split('_')]
    max_date = date(max_date[0], max_date[1], max_date[2])

    output_dir = CONFIG.get('work', 'output_directory')
    query_cells(cells, satellites, min_date, max_date, fc_ds_type, output_dir)


    # Create the tiles list that contains groups of cells to operate on
    # Each node will have a certain number of cells to work on
    cpnode = int(CONFIG.get('internals', 'cells_per_node'))
    tiles = create_tiles(len(cells), cpnode)
    tiles_list_fname = pjoin(out_dir, CONFIG.get('outputs', 'tiles_list'))
    with open(tiles_list_fname, 'w') as out_file:
        pickle.dump(tiles, out_file)


    # Setup the modules to use for the job
    modules = CONFIG.get('pbs', 'modules').split(',')

    modules = ['module load {}; '.format(module) for module in modules]
    modules.insert(0, 'module use /projects/u46/opt/modules/modulefiles;')
    modules = ''.join(modules)

    # Calculate the job node and cpu requirements
    nnodes = len(tiles)
    ncpus = nnodes * 16
    mem = nnodes * 32

    # Populate the PBS shell script
    project = CONFIG.get('pbs', 'project')
    queue = CONFIG.get('pbs', 'queue')
    walltime = CONFIG.get('pbs', 'walltime')
    email = CONFIG.get('pbs', 'email')
    #py_file = pjoin(dirname(__file__), 'workflow.py')
    py_file = 'workflow.py'
    cfg_file = args.cfg
    pbs_job = PBS_DSH.format(project=project, queue=queue,
                             walltime=walltime, ncpus=ncpus, mem=mem,
                             email=email, nnodes=nnodes,
                             modules=modules, pyfile=py_file,
                             cfgfile=cfg_file)

    # Out put the shell script to disk
    pbs_fname = pjoin(out_dir, CONFIG.get('outputs', 'pbs_filename'))
    with open(pbs_fname, 'w') as out_file:
        out_file.write(pbs_job)

    #subprocess.check_call(['qsub', pbs_fname])
