# CloudMigrationPoC

Command to deploy the Cloud Function
gcloud beta functions deploy intitate_data_flow --runtime python37 --trigger-resource triggerbucket-1  --trigger-event google.storage.object.finalize

Command to run Cloud DataFlow
For template: mvn -X compile exec:java -e \
-Dexec.mainClass=com.click.example.CloudSqlImport \
-Dexec.args="--project=snappy-meridian-255502 \
--stagingLocation=gs://df-staging-1/staging/ \
--tempLocation=gs://df-temp-1/temp/ \
--inputFile=gs://triggerbucket-1/Sample.txt \
--templateLocation=gs://df-templates-1/templateDF \
--runner=DataflowRunner"

for Running :mvn -X compile exec:java -e \
-Dexec.mainClass=com.click.example.CloudSqlImport \
-Dexec.args="--project=snappy-meridian-255502 \
--stagingLocation=gs://df-staging-1/staging/ \
--tempLocation=gs://df-temp-1/temp/ \
--inputFile=gs://triggerbucket-1/Sample.txt \
--runner=DataflowRunner"

