package com.bridgelabz.AWSUtilites

import awscala.Region
import awscala.s3.{Bucket, S3}

object S3Upload {
  implicit val s3: S3 = S3.at(Region.Mumbai)

  /***
    * Checks Bucket Exists in S3
    * @param bucket Bucket
    * @return Boolean
    */
  def checkBucketExistsOrNot(bucket: String): Boolean =
    s3.doesBucketExistV2(bucket)

  /***
    * Creates Bucket in AWS S3
    * @return Bucket - Bucket Which Created
    */
  def createBucket(bucketName: String): Bucket = s3.createBucket(bucketName)

}
