#!/usr/bin/env python
import boto
import gzip
import math
import multiprocessing
import md5
import os
import Queue
import random
import sys

def md5Digest(filePath):
  infile = open(filePath, 'rb')
  checksum = md5.new()
  data = infile.read(1024 * 1024)
  while len(data) > 0:
    checksum.update(data)
    data = infile.read(1024 * 1024)
  infile.close()
  return checksum.hexdigest()

def makeRandomFile(numLines, outputFilePath, s3Bucket, s3Prefix):
  random.seed()
  s3 = boto.connect_s3()
  bucket = boto.s3.bucket.Bucket(s3, "mock-test")
  outfile = gzip.open(outputFilePath, 'wb')
  for i in xrange(0,numLines):
    seed1 = random.randint(0,2**32-1)
    seed2 = random.randint(-2**31,2**31-1)
    seed3 = random.randint(2**32, 2**50)
    seed4 = random.randint(-2**50, 2**50)
    seed5 = random.random()
    seed6 = random.random()
    seed7 = random.randint(-127,128)
    a = seed1
    b = seed2
    c = seed3
    d = seed4
    e = seed5
    f = seed6
    g = seed1 * seed1
    h = seed5 * seed6
    j = -seed1
    k = -seed3
    l = math.sqrt(seed5)
    m = seed7
    n = seed7 * seed1
    outString = "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(a,b,c,d,e,f,g,h,i,j,k,l,m,n)
    outfile.write(outString)
  outfile.close()
  key = boto.s3.key.Key(bucket)
  key.key = s3Prefix + "/" + outputFilePath
  digest=key.get_md5_from_hexdigest(md5Digest(outputFilePath))
  key.set_contents_from_filename(outputFilePath, md5=digest)
  os.remove(outputFilePath)

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
    print "Finished processing {}".format(outputFilePath)

def run(numLines, numFiles, outputFilePath, bucket, prefix):
  print "Lines: {}, Files: {}, Base Name: {}, Bucket: {}, Prefix: {}".format(numLines, numFiles, outputFilePath, bucket, prefix)
  queue = multiprocessing.JoinableQueue(100)
  for i in xrange(0,10):
    p = multiprocessing.Process(target=processWork, args=(queue,))
    p.start()
  for i in xrange(0, numFiles):
    queue.put((numLines, outputFilePath + "." + str(i), bucket, prefix))
  for i in xrange(0, 10):
    queue.put((0,"","",""))
  queue.close()
  queue.join()
  print "Done processing"
  sys.exit(0)

if __name__ == '__main__':
  run(long(sys.argv[1]), long(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5])
