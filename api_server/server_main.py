from flask import Flask, request
from urlparse import urlparse
import boto3
from botocore.exceptions import ClientError
import time
import json
import os

app = Flask(__name__)
s3 = boto3.resource('s3')
comprehend = boto3.client('comprehend', region_name='us-east-2')

# uploads classification data, given string input data, returns job-id of classification data.
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

# can return NON_EXISTENT, COMPLETED, IN_PROGRESS string.
def getClassificationJobStatus(jobid):
    try:
        result = comprehend.describe_document_classification_job(JobId=jobid)
        print("Result:" + str(result))
        job_status = result["DocumentClassificationJobProperties"]["JobStatus"]
        return job_status
    except ClientError as e:
        print ("Job " + jobid + " does not exist")
        return "NON_EXISTENT"

# If job id exists, return S3 URI, otherwise return "NON_EXISTENT"
def getClassificationResultLocation(jobid):
    try:
        result = comprehend.describe_document_classification_job(JobId=jobid)
    	s3_result_location = result["DocumentClassificationJobProperties"]["OutputDataConfig"]["S3Uri"]
    	print("Result Location: " + s3_result_location)
    	return s3_result_location
    except ClientError as e:
	    return "NON_EXISTENT"

# downloads and parses classification results given S3 URI and job id.
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

# returns json data of jobs.
def listAllClassificationJobs(maxjobs):
   toreturndata = "{\"jobs\":["
   response = comprehend.list_document_classification_jobs(MaxResults=maxjobs)
   jobList = response["DocumentClassificationJobPropertiesList"]
   isFirst = False
   for job in jobList:
        if isFirst == False:
		isFirst = True
	else:
		toreturndata += ","
	toreturndata += "["
	jobid = job["JobId"]
	jobstatus = job["JobStatus"]
	toreturndata += "\"" +jobid + "\",\"" + jobstatus + "\"]"
   toreturndata += "]}"
   #return toreturndata
   return json.loads(toreturndata)
	
	
# add the job given the comment and return the job id in JSON form.
@app.route("/add_job", methods=['GET', 'POST'])
def add_job():
    inputdata = request.args.get('comment')
    # return the job id
    id = uploadClassificationData(inputdata)
    return json.loads("{\n    \"job-id\":\""+id+"\"\n}")

# given the job id, check if the job is done.
@app.route("/check_job_status", methods=['GET', 'POST'])
def check_job():
    job_id = request.args.get('jobid')
    # return the job status. (finished, not finished, etc.)
    status = getClassificationJobStatus(job_id)
    return json.loads("{\n    \"status\":\""+status+"\"\n}")

# give the job id, return the classification results of the job.
@app.route("/get_results", methods=['GET', 'POST'])
def get_job_results():
    job_id = request.args.get('jobid')
    if job_id is None:
	return json.loads("{}")
    # return the job's classification results.
    try:
	location = getClassificationResultLocation(job_id)
        if(location=="NON_EXISTENT"):
        	return json.loads("{}")

    	data = downloadAndParseClassificationResults(location,job_id)

    	return json.loads(data)

    except ClientError as e:
		return json.loads("{}")
    
@app.route("/list_jobs", methods=['GET', 'POST'])
def list_jobs():
	numJobsToList = request.args.get('count')
	if numJobsToList is None:
		return listAllClassificationJobs(100)
	return listAllClassificationJobs(int(numJobsToList))


if (__name__ == "__main__"):
    print("Starting ShieldBot API Server...")
    #print("S3 buckets:")
    #for bucket in s3.buckets.all():
    #    print(bucket.name)
    #print("Done")
    # print(uploadClassificationData("This is a test message."))
    # print(downloadAndParseClassificationResults(getClassificationResultLocation("98e2ccf561f63051df381747c7bbf7dc"), "98e2ccf561f63051df381747c7bbf7dc"))
    # print(getClassificationJobStatus("98e2ccf561f63051df381747c7bbf7dc"))
    # print(getClassificationJobStatus("1798b4f6315e33fa3725a34c382df4be"))
    # print(listAllClassificationJobs(100))
    app.run(port = 5000)
