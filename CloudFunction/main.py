from pprint import pprint
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from google.cloud import storage
import calendar;
import time;

client = storage.Client()

credentials = GoogleCredentials.get_application_default()

dataflow = discovery.build('dataflow', 'v1b3', credentials=credentials)



project = 'snappy-meridian-255502'
def intitate_data_flow(data, context):

  name=data['name']
  dbName=name.split(".")
  ts = calendar.timegm(time.gmtime())
  jobName = dbName[0] + "-" + str(ts)
  tmpLocation = 'gs://df-temp-1/temp/'
  templatePath ='gs://df-templates-1/templateJobStatDF.json'
  fileLoc='gs://triggerbucket-1/'+name

  request_body = {
          "environment": {
            "zone": "us-central1-f",
            "tempLocation": tmpLocation
          },
          "parameters": {
            "inputFile": fileLoc,
            "output" : dbName[0] 
          },
          "jobName": jobName
        }

  request = dataflow.projects().templates().launch(
        projectId=project,
        body= request_body,
        gcsPath = templatePath
  )

  response = request.execute()

  pprint(response)
  

