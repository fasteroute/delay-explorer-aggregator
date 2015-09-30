delay-explorer-aggregator
=========================

This script can be run each day to turn the raw data from darwin-db into the aggregate data needed
by delay-explorer-api to power delay-explorer-web.

It requires Python 3.4+

Contributing
------------

This code is quite messy as it has evolved rather wildly (and rapidly). We're currently having a
major reconsideration of how best to store the statistics (trying to balance flexibility with not
needing a crazy amount of disk space) and once that process is complete, this script will get a
pretty substantial rewrite to both be better structured, tested and more maintainable, as well as
to implement this new statistics storage structure.


