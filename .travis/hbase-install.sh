#! /bin/bash

if [ ! -f $HOME/downloads/hbase-2.1.6-bin.tar.gz ]; then sudo wget -O $HOME/downloads/hbase-2.1.6-bin.tar.gz http://www-us.apache.org/dist/hbase/2.1.6/hbase-2.1.6-bin.tar.gz; fi
sudo mv $HOME/downloads/hbase-2.1.6-bin.tar.gz hbase-2.1.6-bin.tar.gz && tar xzf hbase-2.1.6-bin.tar.gz
sudo rm -f hbase-2.1.6/conf/hbase-site.xml && sudo mv .travis/hbase/hbase-site.xml hbase-2.1.6/conf
sudo hbase-2.1.6/bin/start-hbase.sh
