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

MAX_WORKERS = 10

if __name__ == '__main__' and len(sys.argv) > 4:
  S3_BUCKET = sys.argv[4]
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
  def save_to_s3(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
      s3_prefix = S3_PREFIX
      s3_bucket = S3_BUCKET or 'mock-test'
      assert s3_prefix and s3_bucket

      s3 = boto.connect_s3()
      bucket = boto.s3.bucket.Bucket(s3, s3_bucket)
      output_path = kwargs.get('outputFilePath') or args[1]
      # TODO: better pattern for the above?
      
      md5_digest = f(*args, **kwargs)
      
      key = boto.s3.key.Key(bucket)
      key.key = s3_prefix + "/" + output_path
      digest = key.get_md5_from_hexdigest(md5_digest)
      key.set_contents_from_filename(output_path, md5=digest)
      os.remove(output_path)

      return md5_digest
    return wrapper
else:
  # dummy wrapper
  def save_to_s3(f):
    return f


def md5Digest(filePath):
  infile = open(filePath, 'rb')
  checksum = md5.new()
  data = infile.read(1024 * 1024)
  while len(data) > 0:
    checksum.update(data)
    data = infile.read(1024 * 1024)
  infile.close()
  return checksum.hexdigest()

@save_to_s3
def makeRandomFile(numLines, outputFilePath, hashtype='md5'):
  random.seed()

  outfile = gzip.open(outputFilePath, 'wb')
  hashemi = hashlib.__dict__.get(hashtype)

  for i in xrange(0,numLines):
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
    outline = "\t".join(str(v) for v in vals)+"\n"
    hashemi.update(outline)
    outfile.write(outline)
  
  outfile.close()

  md5_digest = hashemi.hexdigest()

  return md5_digest

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

def run(numLines, numFiles, outputFilePath):
  print "Lines: {0}, Files: {1}, Base Name: {2}, Bucket: {3}, Prefix: {4}".format(numLines, numFiles, outputFilePath, S3_BUCKET, S3_PREFIX)

  pool = multiprocessing.Pool(min(MAX_WORKERS, numFiles))

  for i in range(numFiles):
    pool.apply_async(makeRandomFile, (numLines, outputFilePath + "." + str(i) + '.tsv.gzip'))

  pool.close()
  pool.join()
  print "Done processing"
  sys.exit(0)

if __name__ == '__main__':
  run(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3])
