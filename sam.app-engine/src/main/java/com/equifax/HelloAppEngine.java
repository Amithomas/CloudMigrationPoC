package com.equifax;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.services.dataflow.Dataflow;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.auth.appengine.AppEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.RuntimeEnvironment;

@WebServlet(
    name = "HelloAppEngine",
    urlPatterns = {"/hello"}
)
public class HelloAppEngine extends HttpServlet {

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
      throws IOException{
	  GoogleCredential credential = GoogleCredential.getApplicationDefault();

	    // The createScopedRequired method returns true when running on GAE or a local developer
	    // machine. In that case, the desired scopes must be passed in manually. When the code is
	    // running in GCE, GKE or a Managed VM, the scopes are pulled from the GCE metadata server.
	    // See https://developers.google.com/identity/protocols/application-default-credentials for more information.
	    if (credential.createScopedRequired()) {
	      credential = credential.createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
	    }
	    HttpTransport httpTransport=null;
		try {
			httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		} catch (GeneralSecurityException e) {
			
		}
	    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
	    Dataflow dataflowService = new Dataflow.Builder(httpTransport, jsonFactory, credential)
	      .setApplicationName("Google Cloud Platform Sample")
	      .build();
	    String projectId ="pragmatic-braid-263313";
	    CreateJobFromTemplateRequest content = new CreateJobFromTemplateRequest();
	    content.setGcsPath("gs://df-templates-1/templateDF.json");
	    Map<String,String> parameters = new HashMap<String,String>();
	    parameters.put("output", "customer_details");
	    parameters.put("inputFile", "gs://df-trigger-bucket-1/customer_details.txt");
	    RuntimeEnvironment environment= new RuntimeEnvironment();
	    environment.setTempLocation("gs://df-temp-1/temp/");
	    content.setEnvironment(environment);
	    
	    content.setParameters(parameters);
	    Dataflow.Projects.Templates.Create dataflowRequest = dataflowService.projects().templates().create(projectId, content);
	    // Add your code to assign values to desired fields of the 'job' object

	    
	    Job dataflowResponse = dataflowRequest.execute();
    response.setContentType("text/plain");
    response.setCharacterEncoding("UTF-8");

    response.getWriter().print("Hello App Engine!\r\n");

  }
}