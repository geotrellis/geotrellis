#!/usr/bin/env bash

 set -e
 set -x

 # there is no geomesa project
 ./sbt -212 "project macros" publish -no-colors \
   && ./sbt -212 "project vector" publish -no-colors \
   && ./sbt -212 "project proj4" publish -no-colors \
   && ./sbt -212 "project raster" publish -no-colors \
   && ./sbt -212 "project spark" publish -no-colors \
   && ./sbt -212 "project spark-pipeline" publish -no-colors \
   && ./sbt -212 "project s3" publish -no-colors \
   && ./sbt -212 "project s3-spark" publish -no-colors \
   && ./sbt -212 "project accumulo" publish -no-colors \
   && ./sbt -212 "project accumulo-spark" publish -no-colors \
   && ./sbt -212 "project hbase" publish -no-colors \
   && ./sbt -212 "project hbase-spark" publish -no-colors \
   && ./sbt -212 "project cassandra" publish -no-colors \
   && ./sbt -212 "project cassandra-spark" publish -no-colors \
   && ./sbt -212 "project geotools" publish -no-colors \
   && ./sbt -212 "project shapefile" publish -no-colors \
   && ./sbt -212 "project layer" publish -no-colors \
   && ./sbt -212 "project store" publish -no-colors \
   && ./sbt -212 "project util" publish -no-colors \
   && ./sbt -212 "project vectortile" publish -no-colors \
   && ./sbt -212 "project raster-testkit" publish -no-colors \
   && ./sbt -212 "project vector-testkit" publish -no-colors \
   && ./sbt -212 "project spark-testkit" publish -no-colors \
   && ./sbt -212 "project gdal" publish -no-colors
