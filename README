# DataGen

DataGen is a pretty small Python program used to generate pretty large
datasets of random numbers for testing with MapReduce frameworks like
Hadoop and Disco.

Beyond simply being concurrent, there is a disproportionate amount of
work that went into targeting S3, including boto integration and
streaming gzip+md5 creation.

There are several configuration options, most of them available at the
top of main.py. Python may not be the fastest language when it comes
to CPU-bound tasks, but in this case, serviceable and
readily-available concurrency in the standard library outweighed other
optimization strategies.

## Usage

To run the program, navigate to DataGen/src and run something like:

    python main.py 20000 25 test

Provided you are using Python 2.6 or higher, this should generate 25
20,000-line gzipped TSV files all starting with the name 'test' (in
your current directory). If you've got boto and configured your AWS
environment properly, the files will be uploaded and removed as they
are generated.

## TODO

 * Try cStringIO-based streaming gzip
 * Try LFSR-based random number generation
 * PyPy tests