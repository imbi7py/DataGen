#!/usr/bin/env python
import gzip
import math
import multiprocessing
import hashlib, md5 #TODO: remove md5, it's just here for demonstration
import os
import Queue
import random
import sys

from functools import wraps

MP_ENABLED = True
MAX_WORKERS = 10
COMP_LEVEL = 9

if __name__ == '__main__' and len(sys.argv) > 4:
  S3_BUCKET = 'mock-test' # sys.argv[4] #TODO, found it this way, i think
  S3_PREFIX = sys.argv[5]
else:
  S3_BUCKET = None # or some other defaults
  S3_PREFIX = None

S3_ENABLED = True
try:
  import boto
except ImportError as ie:
  S3_ENABLED = False

if S3_ENABLED:
  # a decorator for saving to s3. could probably be enhanced
  # TODO: untested
  def save_to_s3(s3_prefix, s3_bucket):
    def decorator(f):
      @wraps(f)
      def g(*args, **kwargs):
        s3 = boto.connect_s3()
        bucket = boto.s3.bucket.Bucket(s3, s3_bucket)
        output_path = kwargs.get('outputFilePath') or args[1]
        # TODO: better pattern for the above?
      
        md5_digest, _, _ = f(*args, **kwargs)
        
        key = boto.s3.key.Key(bucket)
        key.key = s3_prefix + "/" + output_path
        digest = key.get_md5_from_hexdigest(md5_digest)
        key.set_contents_from_filename(output_path, md5=digest)
        os.remove(output_path)
        
        return md5_digest
      return g
    return decorator
else:
  # dummy wrapper
  def save_to_s3(*args, **kwargs):
    def decorator(f):
      return f
    return decorator

#TODO: not needed
def md5Digest(filePath):
  infile = open(filePath, 'rb')
  checksum = md5.new()
  data = infile.read(1024 * 1024)
  while len(data) > 0:
    checksum.update(data)
    data = infile.read(1024 * 1024)
  infile.close()
  return checksum.hexdigest()

@save_to_s3(S3_PREFIX, S3_BUCKET)
def makeRandomFile(numLines, outputFilePath, hashtype='md5', comp_level=9):
  random.seed()
  outfile = gzip.open(outputFilePath, 'wb', comp_level)
  hashemi = hashlib.__dict__.get(hashtype)()

  bytes_written      = 0
  comp_bytes_written = 0

  for i in range(numLines):
    seed1 = random.randint(0,2**32-1)
    seed2 = random.randint(-2**31,2**31-1)
    seed3 = random.randint(2**32, 2**50)
    seed4 = random.randint(-2**50, 2**50)
    seed5 = random.random()
    seed6 = random.random()
    seed7 = random.randint(-127,128)
    
    vals = ( seed1,
             seed2,
             seed3,
             seed4,
             seed5,
             seed6,
             seed1 ** 2,
             seed5 * seed6,
             -seed1,
             -seed3,
             seed5 ** 0.5,
             seed7,
             seed7 * seed1
             )

    #outString = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}\t{11}\t{12}\t{13}\n".format(a,b,c,d,e,f,g,h,i,j,k,l,m,n)
    # alt to above ^
    # outString = "{a}\t{b}\t{c}\n".format(**locals())
    outline = "\t".join([str(v) for v in vals])+"\n"
    hashemi.update(outline)
    bytes_written += outfile.write(outline)
  
  outfile.close()
  comp_bytes_written = os.path.getsize(outputFilePath)
  return hashemi.hexdigest(), bytes_written, comp_bytes_written

def processWork(queue):
  while True:
    try:
      (numLines, outputFilePath, bucket, prefix) = queue.get(True, 3)
    except Queue.Empty:
      continue
    if numLines == 0:
      queue.task_done()
      return
    makeRandomFile(numLines, outputFilePath + ".gz", bucket, prefix)
    queue.task_done()
    print "Finished processing {0}".format(outputFilePath)


def div_rounded(x,y,prec=3):
  if y == 0: return 0
  return round(float(x)/y, prec)

# This is just me being bored and playing with generators
# This would do better as a callable class. Call your local
# Python dealer for more information!
def file_complete():
  counter         = 0
  total_size      = 0
  total_comp_size = 0
  
  try:
    #yield #first call is None
    total_files = (yield)#(yield counter, total_size, total_comp_size)
    while True:
      ret = (yield counter, total_size, total_comp_size)
      if ret:
        hash_digest, size, comp_size = ret
        counter   += 1
        ratio      = div_rounded(comp_size, size)
        percent    = div_rounded(counter, total_files)
        print ("[{percent:>4.0%}]  Process #{counter} complete. Hash: {hash_digest}\n"
               "        Compressed: {comp_size}  Uncompressed: {size}"
               "  Ratio: {ratio}").format(**locals())

        total_size      += size
        total_comp_size += comp_size
        
  finally:
    pass # cleanup can go here.

def run(numLines, numFiles, outputFilePath, use_mp=True, max_workers=10, hash_type='md5', comp_level=9):
  print "Lines: {0}, Files: {1}, Base Name: {2}, Bucket: {3}, Prefix: {4}".format(numLines, numFiles, outputFilePath, S3_BUCKET, S3_PREFIX)

  callback = file_complete()
  callback.send(None) # prime it?
  callback.send(numFiles)

  if use_mp:
    pool = multiprocessing.Pool(min(max_workers, numFiles))

  for i in range(numFiles):
    kwargs = {numLines: numLines,
              outputFilePath: outputFilePath + "." + str(i) + '.tsv.gz',
              hash_type: hash_type,
              comp_level: comp_level,
              }
            
    if use_mp:
      pool.apply_async(makeRandomFile,
                       kwds=kwargs,
                       callback=callback.send)
    else:
      callback.send(makeRandomFile(**kwargs))
  
  if use_mp:
    pool.close()
    pool.join()

  final_stats = callback.next()
  done_count, total_size, total_comp_size = final_stats
  ratio = div_rounded(total_comp_size, total_size)

  print
  print
  print "Done."
  print
  print ("Wrote {done_count} files, {total_comp_size} bytes ({total_size} bytes "
         "uncompressed. Ratio: {ratio}).").format(**locals())
  print

  return

if __name__ == '__main__':
    run(numLines       = int(sys.argv[1]), 
        numFiles       = int(sys.argv[2]), 
        outputFilePath = sys.argv[3], 
        use_mp         = MP_ENABLED,
        max_workers    = MAX_WORKERS,
        comp_level     = COMP_LEVEL
        )
