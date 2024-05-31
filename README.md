# fitsconcat
Concatenate multiple channel images into a single FITS cube

- This script does NOT do anything with the image headers, it just uses the default FITS header
- The multiprocess version has NOT been extensively verified, use at your own peril.

The multiprocess version is faster, but initial benchmarks suggest that with 16 threads it is only ~ 2.5x faster. Any suggestions to speed it up further are welcome.


This script is an extract of the code that exists in [frocc](https://github.com/idia-astro/frocc).
