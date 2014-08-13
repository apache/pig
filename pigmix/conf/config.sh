#!/bin/sh

hdfsroot=/user/pig/tests/data/pigmix
localtmp=/tmp

# configure the number of mappers for data generator
mappers=90

# ~1600 bytes per row for page_views (it is the base for most other inputs)
rows=625000000

# only used in L11 (widerow, ~2500 bytes per row)
widerowcnt=10000000
