from flask import Flask
import boto3
import time

app = Flask(__name__)
s3 = boto3.resource('s3')


def uploadClassificationData(inputdata):
    object = s3.Object('drdab', 'testdata/' + str(int(time.time())) +".txt")
    object.put(Body=inputdata)

@app.route("/")
def hello():
    uploadClassificationData("Hello world");
    return "Trying to upload classification data..."

if (__name__ == "__main__"):
    print("Starting ShieldBot API Server...")
    print("S3 buckets:")
    for bucket in s3.buckets.all():
        print(bucket.name)
    print("Done")
    app.run(port = 5000)

