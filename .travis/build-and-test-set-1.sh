#!/bin/bash

./sbt -J-Xmx2G "++$TRAVIS_SCALA_VERSION" "project spark" test  || { exit 1; }
./sbt -J-Xmx2G "++$TRAVIS_SCALA_VERSION" "project accumulo" test  || { exit 1; }
./sbt -J-Xmx2G "++$TRAVIS_SCALA_VERSION" "project proj4" test || { exit 1; }
./sbt -J-Xmx2G "++$TRAVIS_SCALA_VERSION" "project geotools" test || { exit 1; }
./sbt -J-Xmx2G "++$TRAVIS_SCALA_VERSION" "project shapefile" test || { exit 1; }
