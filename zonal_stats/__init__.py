#!/usr/bin/env python

import numpy
import pandas
import rasterio


from datacube.api.model import DatasetType
from datacube.api.utils import get_dataset_data_with_pq
from image_processing.segmentation import Segments
from idl_functions import histogram


def zonal_stats(dataset, rasterised_fname, dataset_type):
    """
    Computes the Observed Count, Min, Max, Sum and Sum of Squares for
    the segments defined by the rasterised image. The stats are
    derived from the `dataset` defined by the `dataset_type`.

    :param dataset:
        A class of type `Dataset`.

    :param rasterised_fname:
        A string containing the full file pathname of an image
        containing the rasterised features. These features will be
        interpreted as segments.

    :param dataset_type:
        A class of type `DatasetType`.

    :return:
        A `pandas.DataFrame` containing the statistics for each segment
        and for each raster band contained with the `dataset_type`.
    """
    # Initialiase a blank dataframe
    headings = ["SID", "Timestamp", "Band", "Observed_Count", "Min", "Max",
                "Sum", "Sum_of_Squares"]
    df = pandas.DataFrame(columns=headings, dtype=numpy.float)

    # Read the rasterised image
    with rasterio.open(rasterised_fname) as src:
        img = src.read(1)

    # Initialise the segment visitor
    seg_vis = Segments(img)

    # Do we have any data to analyse???
    if seg_vis.n_segments == 0:
        return df

    # We need to get the PQ data and the DatasetType of interest
    pq_ds = dataset.datasets[DatasetType.PQ25]
    ds = dataset.datasets[dataset_type]
    timestamp = dataset.start_datetime
    bands = ds.bands

    no_data = -999

    for band in bands:
        # When the api has a release of get_pq_mask this will have to do
        # It'll re-compute the PQ every time which is not ideal
        # Otherwise go back to eotools???
        ds_data = (get_dataset_data_with_pq(ds, pq_ds, bands=[band],
                                            ndv=no_data)[band]).astype('float')
        # Set no-data to NaN
        ds_data[ds_data == no_data] = numpy.nan

        # Loop over each segment and get the data.
        # In other instances we may just need the locations
        for seg_id in seg_vis.segment_ids:
            data = seg_vis.data(ds_data, segment_id=seg_id)

            # dimensions of the data which will be 1D
            dim = data.shape

            # Returns are 1D arrays, so check if we have an empty array
            if dim[0] == 0:
                continue # Empty bin, (no data), skipping

            # Compute the stats
            count = numpy.sum(numpy.isfinite(data))
            sum_ = numpy.nansum(data)
            sum_sq = numpy.nansum(data**2)
            min_ = numpy.nanmin(data)
            max_ = numpy.nanmax(data)

            format_dict = {"SID": seg_id,
                           "Timestamp": timestamp,
                           "Band": band.name,
                           "Observed_Count": count,
                           "Min": min_,
                           "Max": max_,
                           "Sum": sum_,
                           "Sum_of_Squares": sum_sq}

            # Append the stat to the data frame
            df = df.append(format_dict, ignore_index=True)

    return df


def zonal_class_distribution(classified_image, zonal_image, class_ids=None):
    """
    Calculates the classification distribution for each zone.
    """
    # Initialise the segment visitor
    seg_vis = Segments(zonal_image)

    # Define the min/max class distribution if we didn't receive any class ID's
    if class_ids is None:
        min_class = 0
        max_class = numpy.max(classified_image)
        # if max_class == min_class:
            # We have a completely unclassified image
            # TODO Should we do anything???
            # If someone is continually calling this routine to build up
            # a table of results for numerous images based on the same
            # classification schema/algorithm, then idealy they should provide
            # a list of class id's, otherwise there could be mis-matching
            # histograms???
    else:
        min_class = numpy.min(class_ids)
        max_class = numpy.max(class_ids)

    # Initialise the dict to store our results
    results = {}

    # base class name
    cname_format = 'Class_{}'

    # Retrieve the class distributions for each segment/zone
    for zid in seg_vis.segment_ids:
        data = seg_vis.data(classified_image, segment_id=zid)

        # Skip invalid segments
        if data.size == 0:
            continue
        
        results[zid] = histogram(data, minv=min_class,
                                 maxv=max_class)['histogram']


    results = pandas.DataFrame(results).transpose()
    results['SID'] = results.index.values
    results['PixelCount'] = seg_vis.histogram[results.index.values]

    # convert the class names from ints to strings
    for col in results.columns:
        if type(col) != bytes:
            cname = cname_format.format(col)
            results.rename(columns={col: cname}, inplace=True)

    return results
