package com.click.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.testing.TestPipeline;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.storage.Storage;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageOptions;

import org.apache.beam.sdk.coders.*;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.json.JSONArray; 
import org.json.JSONObject; 
//import org.json.simple.parser.*; 
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;
import org.apache.commons.dbcp2.BasicDataSource;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class BikeTrip  {
	 

	private static final Logger LOG = LoggerFactory.getLogger(BikeTrip.class);

  

  public interface BikeTripOptions  extends PipelineOptions  {

  }
  
  
  static class BikeTripStatementSetter implements JdbcIO.PreparedStatementSetter<ArrayList<String>>
  {
    private static final long serialVersionUID = 1L;

    public void setParameters(ArrayList<String> element, PreparedStatement query) throws Exception
    {
      query.setLong(1, Integer.valueOf(element.get(2)));
      query.setString(2, element.get(4));
      query.setString(3, element.get(1));
      query.setString(4, element.get(3));
      query.setString(5, element.get(0));
      LOG.info(query.toString());
    }
  }

  public static void main(String[] args) {
	  String sourceFilePath = "gs://triggerbucket-1/Sample.txt";
 
BikeTripOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BikeTripOptions.class);      
  Pipeline p = Pipeline.create(options);
  		   
  PCollection<String> lines =p.apply("Read JSON text File", TextIO.read().from(sourceFilePath));
  PCollection<ArrayList<String>> values=lines.apply("Process JSON Object", ParDo.of(new DoFn<String, ArrayList<String>>() {
	  private static final long serialVersionUID = 1L;
      @ProcessElement
      public void processElement(ProcessContext c) throws ParseException {
    	  String object= c.element();
    	  JSONParser parser = new JSONParser();
    	  org.json.simple.JSONObject json = (org.json.simple.JSONObject) parser.parse(object);
         Collection values = json.values();
         Iterator keys = values.iterator();
         ArrayList<String> valueList= new ArrayList<String>();
         while (keys.hasNext()) {
             valueList.add(keys.next().toString());
         }
         LOG.info(valueList.get(0));
         c.output(valueList);
      }
  }));
  
  values.apply(JdbcIO.<ArrayList<String>>write()
          .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
        		  .create("com.mysql.jdbc.Driver", "jdbc:mysql://google/cloudsqltestdb?cloudSqlInstance=snappy-meridian-255502:us-central1:test-sql-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=root&useSSL=false")
                  )
          .withStatement("insert into customer_details values(?,?,?,?,?)")
              .withPreparedStatementSetter(new BikeTripStatementSetter()));
    p.run().waitUntilFinish();
  }

}
