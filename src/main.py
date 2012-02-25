#!/usr/bin/env python
import gzip
import math
import multiprocessing
import hashlib, md5 #TODO: remove md5, it's just here for demonstration
import os
import Queue
import random
import sys

import fcntl
import errno
import select

from functools import wraps

MP_ENABLED = False
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

# a decorator for saving to s3. could probably be enhanced
# TODO: untested
def save_to_s3(s3_prefix, s3_bucket):
  def decorator(f):
    if not S3_ENABLED:
      return f
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

class GzipStreamFile(object):
  def __init__(self, filename, comp_level=9, out_hook=None):
    if out_hook and not callable(out_hook):
      raise TypeError("out_hook must be a callable.")
    self.out_hook = out_hook

    (fd_r, fd_w) = os.pipe() # Try a pipe. Could use cStringIO for the same.
    fcntl.fcntl(fd_r, fcntl.F_SETFL, os.O_NONBLOCK)
    self.pipe_r = os.fdopen(fd_r, 'rb')
    self.pipe_w = os.fdopen(fd_w, 'wb')

    self.gzip_pipe = gzip.GzipFile(fileobj=self.pipe_w, compresslevel=comp_level)
    self.outfile = open(filename, 'wb')

    self.bytes_written = 0
    self.comp_bytes_written = 0

  def write(self, data):
    self.bytes_written += self.gzip_pipe.write(data)
    try:
      comp_data = self.pipe_r.read()
      self.outfile.write(comp_data)

      comp_bytes_written = len(comp_data)
      self.comp_bytes_written += comp_bytes_written
      return comp_bytes_written
    except IOError as ioe:
      if ioe.errno == errno.EAGAIN:
        return 0
      else:
        raise

  def close(self, abandon=False):
    self.gzip_pipe.close()

    # This is being extra cautious. The close() above should flush and this
    # select should almost always return immediately and may not actually
    # be neceessary.
    if not abandon:
      rd,_,_ = select.select([self.pipe_r],[],[self.pipe_r], 1)
      if rd:
        comp_data = self.pipe_r.read()
        self.out_hook(comp_data)
        self.outfile.write(comp_data)

    self.pipe_w.close()
    self.pipe_r.close()
    self.outfile.close()

  def __del__(self):
    self.close(abandon=True)

  def flush(self):
    pass
    # Gzipfile doesn't expose this, but it's totally possible
    # to close the gzip_pipe to flush it and make a new one.

@save_to_s3(S3_PREFIX, S3_BUCKET)
def makeRandomFile(numLines, outputFilePath, hash_type='md5', comp_level=9):
  random.seed()

  hashemi = hashlib.__dict__.get(hash_type)()
  gzip_file = GzipStreamFile(outputFilePath, 
                             comp_level=comp_level, 
                             out_hook=hashemi.update)

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

    outline = "\t".join([str(v) for v in vals])+"\n"
    gzip_file.write(outline)
  
  gzip_file.close()
  print 'os:', os.path.getsize(outputFilePath), ' gzip_file:',gzip_file.comp_bytes_written
  return hashemi.hexdigest(), gzip_file.bytes_written, gzip_file.comp_bytes_written

def div_rounded(x,y,prec=3):
  if y == 0: return 0
  return round(float(x)/y, prec)

# This is just me being bored and playing with generators
# This would do better as a callable class. Contact your 
# local Python dealer for more information!
def file_complete():
  counter         = 0
  total_size      = 0
  total_comp_size = 0
  
  try:
    total_files = (yield)
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
  callback.send(None) # prime it
  callback.send(numFiles)

  if use_mp:
    pool = multiprocessing.Pool(min(max_workers, numFiles))

  for i in range(numFiles):
    kwargs = {'numLines': numLines,
              'outputFilePath': outputFilePath + "." + str(i) + '.tsv.gz',
              'hash_type': hash_type,
              'comp_level': comp_level,
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
