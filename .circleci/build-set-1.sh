#!/bin/bash

./sbt -Dsbt.supershell=false "++$SCALA_VERSION" \
  "project proj4" test \
  "project geotools" test \
  "project shapefile" test \
  "project layer" test \
  "project store" test \
  "project vector" test \
  "project vectortile" test \
  "project gdal" test \
  "project hbase" test:compile \
  "project hbase-spark" test:compile \
  "project cassandra" test \
  "project cassandra-spark" test || { exit 1; }
