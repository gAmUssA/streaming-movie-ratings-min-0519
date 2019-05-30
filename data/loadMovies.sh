#!/usr/bin/env bash

cat ./data/movies.dat | confluent produce raw-movies
#cat movies.dat| kafka-console-producer --broker-list localhost:9092 --topic raw-movies
