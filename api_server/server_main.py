from flask import Flask
from urlparse import urlparse
import boto3
import time
import json
import os

app = Flask(__name__)
s3 = boto3.resource('s3')
comprehend = boto3.client('comprehend', region_name='us-east-2')

def uploadClassificationData(inputdata):
    timestamp = str(int(time.time()))
    print("Upload commenced, Timestamp: " + timestamp);
    print("Upload Content : " + inputdata)
    object = s3.Object('drdab', 'testdata/' + timestamp +".txt")
    object.put(Body=inputdata)
    print("Starting classification job...")
    response = comprehend.start_document_classification_job(
    JobName=timestamp,
    DocumentClassifierArn='arn:aws:comprehend:us-east-2:783647116778:document-classifier/SmartFilter',
    InputDataConfig={
        'S3Uri': 's3://drdab/testdata/' + timestamp + ".txt",
        'InputFormat': 'ONE_DOC_PER_FILE'
    },
    OutputDataConfig={
        'S3Uri': 's3://drdab/testoutputs'
    },
    DataAccessRoleArn='arn:aws:iam::783647116778:role/service-role/AmazonComprehendServiceRoleS3FullAccess-DataChecker',
    ClientRequestToken=timestamp,
    )
    print(response)
    print("Done")
    return response["JobId"]

def getClassificationJobStatus(jobid):
    result = comprehend.describe_document_classification_job(JobId=jobid)
    print("Result:" + str(result))
    job_status = result["DocumentClassificationJobProperties"]["JobStatus"]
    return job_status

def getClassificationResultLocation(jobid):
    result = comprehend.describe_document_classification_job(JobId=jobid)
    s3_result_location = result["DocumentClassificationJobProperties"]["OutputDataConfig"]["S3Uri"]
    print("Result Location: " + s3_result_location)
    return s3_result_location

def downloadAndParseClassificationResults(s3_result_location, jobid):
   owo = boto3.client('s3')
   os.system("mkdir outputs/" + jobid)
   print("location:" + str(s3_result_location))
   o = urlparse(str(s3_result_location))
   print("path:" + o.path)
   owo.download_file("drdab", str(o.path).lstrip("/"), str("outputs/" + jobid + "/" + jobid + ".tar.gz"))
   os.system("tar -xf outputs/" + jobid + "/" + jobid + ".tar.gz -C outputs/" + jobid)
   f=open("outputs/" + jobid + "/predictions.jsonl", "r")
   results = f.read()
   return results


@app.route("/")
def hello():
    data = uploadClassificationData("notices bulge OwO Whats This")
    job_status = getClassificationJobStatus(data)
    return "Trying to upload classification data... \n" + data + "\n" + job_status

if (__name__ == "__main__"):
    print("Starting ShieldBot API Server...")
    #print("S3 buckets:")
    #for bucket in s3.buckets.all():
    #    print(bucket.name)
    #print("Done")
    print(downloadAndParseClassificationResults(getClassificationResultLocation("98e2ccf561f63051df381747c7bbf7dc"), "98e2ccf561f63051df381747c7bbf7dc"))
    app.run(port = 5000)

