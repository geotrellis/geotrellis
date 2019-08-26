#!/bin/bash

./sbt "project accumulo" publish \
      "project accumulo-spark" publish \
      "project cassandra" publish \
      "project cassandra-spark" publish \
      "project geomesa" publish \
      "project geotools" publish \
      # "project geowave" publish \
      "project hbase" publish \
      "project macros" publish \
      "project proj4" publish \
      "project raster" publish \
      "project raster-testkit" publish \
      "project s3" publish \
      "project s3-spark" publish \
      "project hbase" publish \
      "project hbase-spark" publish \
      "project shapefile" publish \
      "project spark" publish \
      "project spark-testkit" publish \
      "project store" publish \
      "project util" publish \
      "project vector" publish \
      "project vector-testkit" publish \
      "project vectortile" publish \
      "project gdal" publish
