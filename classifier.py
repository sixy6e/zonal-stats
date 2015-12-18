#!/usr/bin/env python

import numpy

from datacube.api.utils import get_dataset_data_with_pq, TCI_COEFFICIENTS
from datacube.api.utils import calculate_tassel_cap_index, TasselCapIndex
from datacube.api.utils import calculate_ndvi
from datacube.api.utils import get_dataset_metadata


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
