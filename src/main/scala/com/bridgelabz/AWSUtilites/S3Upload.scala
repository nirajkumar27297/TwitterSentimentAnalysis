/**
  * @author Niraj Kumar
  */
package com.bridgelabz.AWSUtilites
import awscala.Region
import awscala.s3.{Bucket, S3}

/**
  * The class contains the function to upload it to S3 with specified bucket name
  */
object S3Upload {
  //Selecting the nearby location
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
