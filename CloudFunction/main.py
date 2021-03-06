from pprint import pprint
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from google.cloud import storage

client = storage.Client()

credentials = GoogleCredentials.get_application_default()

dataflow = discovery.build('dataflow', 'v1b3', credentials=credentials)



project = 'snappy-meridian-255502'
def intitate_data_flow(data, context):
	
	jobName=data['name']
	tmpLocation = 'gs://df-temp-1/temp/'
	templatePath ='gs://df-templates-1/templateDF'
	fileLoc='gs://triggerbucket-1/'+jobName
	
	request_body = {
          "environment": {
            "zone": "us-central1-f",
            "tempLocation": tmpLocation
          },
         "parameters": {
           "inputFile": fileLoc,
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
		
	
