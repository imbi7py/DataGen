#!/usr/bin/env python
import boto
import gzip
import math
import md5
import random
import string

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

    
    

if __name__ == '__main__':
  numLines = sys.argv[0]
  outputFilePath = sys.argv[1]
  makeRandomFile(numLines, outputFilePath, "mock-test", "outputbillions")
