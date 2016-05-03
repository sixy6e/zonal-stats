#!/usr/bin/env python

import numpy

from datacube.api.utils import get_dataset_data_with_pq, TCI_COEFFICIENTS
from datacube.api.utils import calculate_tassel_cap_index, TasselCapIndex
from datacube.api.utils import calculate_ndvi
from datacube.api.utils import read_dataset_data, get_dataset_metadata
from datacube.api.utils import PqaMask, WofsMask
from datacube.api.utils import get_mask_pqa, get_mask_wofs
from datacube.api.model import Fc25Bands
from datacube.api.model import DatasetType
from eotools.tiling import generate_tiles
from idl_functions import histogram


def classifier(arg25_dataset, pq25_dataset):
    """
    Runs the classifier designed by SF.
    """
    # Get the metadata
    md = get_dataset_metadata(arg25_dataset)
    cols, rows = md.shape

    # Read the data and mask pixels via the PQ dataset
    data = get_dataset_data_with_pq(arg25_dataset, pq25_dataset)

    # Get the wetness coefficients and calculate
    coef = TCI_COEFFICIENTS[arg25_dataset.satellite][TasselCapIndex.WETNESS]
    wetness = calculate_tassel_cap_index(data, coef)

    # NDVI
    ndvi = calculate_ndvi(data[arg25_dataset.bands.RED],
                          data[arg25_dataset.bands.NEAR_INFRARED],
                          output_ndv=numpy.nan)

    # Dump the reflectance data, the classifier only needs tc_wetness and ndvi
    del data

    # Allocate the result
    classified = numpy.zeros((rows,cols), dtype='uint8')

    # Water
    r1 = wetness > 0
    classified[r1] = 1
    _tmp = ~r1

    #r2 = _tmp & ((wetness >= -250) & (wetness < 0))
    r2 = (wetness >= -250) & (wetness < 0)
    r3 = ndvi <= 0.3
    #_tmp2 = _tmp & r2 & ~r3
    _tmp2 = _tmp & r2

    # non-veg
    classified[_tmp2 & r3] = 2
    _tmp3 = _tmp2 & ~r3

    r4 = ndvi <= 0.45

    # saltmarsh
    classified[_tmp3 & r4] = 3
    _tmp2 = _tmp3 & ~r4

    r5 = ndvi <= 0.6

    # mangrove/saltmarsh
    classified[_tmp2 & r5] = 4

    # mangrove
    classified[_tmp2 & ~r5] = 5

    # finished rhs of r2
    _tmp2 = _tmp & ~r2

    r6 = wetness < -750
    r7 = ndvi >= 0.3
    _tmp3 = _tmp2 & r6

    # saltmarsh
    classified[_tmp3 & r7] = 3

    # non-veg
    classified[_tmp3 & ~r7] = 2

    r8 = ndvi <= 0.3
    _tmp3 = _tmp2 & ~r6

    # non-veg
    classified[_tmp3 & r8] = 2

    r9 = ndvi <= 0.45
    _tmp2 = _tmp3 & ~r8

    # saltmarsh
    classified[_tmp2 & r9] = 3

    r10 = ndvi <= 0.6
    _tmp3 = _tmp2 & ~r9

    # mangrove-saltmarsh
    classified[_tmp3 & r10] = 4

    # mangrove
    classified[_tmp3 & ~r10] = 5

    # set any nulls
    valid = numpy.isfinite(ndvi)
    classified[~valid] = 0

    return classified


def classify_abs(tiles):
    """
    Runs the classifier for the ABS FC deciles project.
    """
    bands = [Fc25Bands.BARE_SOIL,
             Fc25Bands.NON_PHOTOSYNTHETIC_VEGETATION,
             Fc25Bands.PHOTOSYNTHETIC_VEGETATION]

    # PQ bits to mask
    bits = [PqaMask.PQ_MASK_SATURATION,
            PqaMask.PQ_MASK_CONTIGUITY,
            PqaMask.PQ_MASK_CLOUD]

    # array metadata
    metadata = get_dataset_metadata(tiles[0].datasets[DatasetType.PQ25])
    cols, rows = metadata.shape

    # allocate classified result
    classified = {}
    for band in bands:
        classified[band] = numpy.zeros((rows, cols), dtype='int8')

    chunks = generate_tiles(cols, rows, cols, 100, False)

    for chunk in chunks:
        ys, ye = chunk[0]
        xs, xe = chunk[1]
        ysize = ye - ys
        xsize = xe - xs
        dims = (len(tiles), ysize, xsize)

        # allocate 3D block to read into
        time_data = {}
        for band in bands:
            time_data[band] = numpy.zeros(dims)

        for z, tile in enumerate(tiles):
            # extract pq
            mask = get_mask_pqa(tile.datasets[DatasetType.PQ25], bits,
                                x=xs, y=ys, x_size=xsize, y_size=ysize)

            # check for water
            if DatasetType.WATER in tile.datasets:
                mask = get_mask_wofs(tile.datasets[DatasetType.WATER],
                                     x=xs, y=ys, x_size=xsize, y_size=ysize,
                                     mask=mask)

            # extract FC data
            data = read_dataset_data(tiles[0].datasets[DatasetType.FC25],
                                     bands=bands, x=xs, y=ys,
                                     x_size=xsize, y_size=ysize)

            # insert and mask
            for key in data:
                time_data[key][z] = data[key]
                time_data[key][z][mask] = numpy.nan
                xbar = numpy.nanmean(time_data[key], axis=0)
                h = histogram(xbar, minv=0, maxv=10000, binsize=1000,
                              locations='loc', omin='omin', omax='omax',
                              reverse_indices='ri')
                subs = (classified[key][ys:ye, xs:xe]).ravel()
                hist = h['histogram']
                ri = h['ri']

                bins = h['loc'] / 1000
                for i in range(bins.shape[0]):
                    if hist[i] == 0:
                        continue
                    subs[ri[ri[i]:ri[i+1]]] = bins[i] + 1 # apply in place

    return classified
