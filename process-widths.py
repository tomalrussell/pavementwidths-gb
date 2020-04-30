#!/usr/bin/env python
# coding: utf-8

# # Process pavement widths
#
# Based on methodology outlined in https://github.com/meliharvey/sidewalkwidths-nyc
#
# - read roadside (pavement) features
# - find centreline
# - calculate width along centreline
# - calculate some summary statistics
# - output cleaned

import glob
import json
import os
import sys

import centerline
import centerline.exceptions
import pandas
import geopandas
import requests
import shapely.wkt

from centerline.geometry import Centerline
from pandarallel import pandarallel
from shapely.geometry import LineString
from shapely.geometry import Point, MultiPoint, MultiLineString
from shapely.ops import linemerge, nearest_points
from tqdm import tqdm

# set up progress_apply
tqdm.pandas()
CACHE_PATH = os.path.join(os.path.dirname(__file__), 'db-data')


def remove_short_lines(line):
    if line.type == 'MultiLineString':
        passing_lines = []

        for i, linestring in enumerate(line):
            other_lines = MultiLineString([x for j, x in enumerate(line) if j != i])

            p0 = Point(linestring.coords[0])
            p1 = Point(linestring.coords[-1])

            is_deadend = False

            if p0.disjoint(other_lines): is_deadend = True
            if p1.disjoint(other_lines): is_deadend = True

            if not is_deadend or linestring.length > 5:
                passing_lines.append(linestring)

        return MultiLineString(passing_lines)

    if line.type == 'LineString':
        return line


def linestring_to_segments(linestring):
    return [
        LineString([linestring.coords[i], linestring.coords[i+1]])
        for i in range(len(linestring.coords) - 1)
    ]


def get_segments(line):
    if line.type == 'MultiLineString':
        line_segments = []
        for linestring in line.geoms:
            line_segments.extend(linestring_to_segments(linestring))
        return line_segments

    elif line.type == 'LineString':
        return linestring_to_segments(line)
    else:
        return []


def interpolate_by_distance(linestring, distance=1):
    count = round(linestring.length / distance) + 1

    if count == 1:
        # grab mid-point if it's a short line
        return [linestring.interpolate(linestring.length / 2)]
    else:
        # interpolate along the line
        return [linestring.interpolate(distance * i) for i in range(count)]


def interpolate(line):
    if line.type == 'MultiLineString':
        all_points = []

        for linestring in line:
            all_points.extend(interpolate_by_distance(linestring))

        return all_points

    if line.type == 'LineString':
        return interpolate_by_distance(line)


def polygon_to_multilinestring(polygon):
    return MultiLineString([polygon.exterior] + [line for line in polygon.interiors])


def get_avg_distances(row):
    avg_distances = []

    boundary = polygon_to_multilinestring(row.geometry)

    for segment in row.segments:
        points = interpolate(segment)

        distances = []

        for point in points:
            p1, p2 = nearest_points(boundary, point)
            distances.append(p1.distance(p2))

        avg_distances.append(sum(distances) / len(distances))

    return avg_distances


def explode_to_segments(df):
    data = {'geometry': [], 'width': []}

    for i, row in df.iterrows():

        for segment, distance in zip(row.segments, row.avg_distances):
            data['geometry'].append(segment.buffer(distance))
            data['width'].append(distance * 2)

    df_segments = pandas.DataFrame(data)
    df_segments = geopandas.GeoDataFrame(df_segments, crs=df.crs, geometry='geometry')
    return df_segments


def process_centreline(geom):
    try:
        line = Centerline(geom)
    except centerline.exceptions.TooFewRidgesError:
        line = Centerline(geom, interpolation_distance=0.1)

    line = remove_short_lines(linemerge(line))
    return line.simplify(1, preserve_topology=True)


def process_lad(lad_code):
    try:
        df = geopandas.read_file(os.path.join(CACHE_PATH, f'{lad_code}.gpkg'))
    except Exception as err:
        print(lad_code, err)
        return

    df['centerlines'] = df.geometry.progress_apply(process_centreline)
    df['segments'] = df.centerlines.progress_apply(get_segments)
    df['avg_distances'] = df.progress_apply(get_avg_distances, axis=1)
    df_segments = explode_to_segments(df)
    df_segments.to_file(os.path.join(CACHE_PATH, f'{lad_code}_segments.gpkg'), driver='GPKG')


if __name__ == '__main__':
    try:
        lad_code = sys.argv[1]
    except IndexError:
        print(f"Usage: python {__file__} <lad_code>")
        sys.exit()

    process_lad(lad_code)
