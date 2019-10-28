package com.click.example;

import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.TextIO;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


import org.slf4j.Logger;


import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CloudSqlImport  {
	 

	private static final Logger LOG = LoggerFactory.getLogger(CloudSqlImport.class);

	static ResultSet rs=null;

  public interface TransformOptions  extends PipelineOptions  {
	  @Description("Path of the file to read from")
	  @Validation.Required
	  String getInputFile();
	  void setInputFile(String value);

  }
  
  
  static class StatementSetter implements JdbcIO.PreparedStatementSetter<Map<String,String>>
  {
    private static final long serialVersionUID = 1L;

    public void setParameters(Map<String,String> element, PreparedStatement query) throws Exception
    {
    	int count=1;
    	while(rs.next()) {
      
      query.setString(count, element.get(rs.getNString("COLUMN_NAME")));
      count++;
    	}
    	LOG.info(query.toString());
    }
  }

  public static void main(String[] args) throws SQLException {
	  String sourceBucket = "gs://triggerbucket-1/";
 
	  TransformOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TransformOptions.class);      
  Pipeline p = Pipeline.create(options);
  String sourceFile=options.getInputFile();
  String sourceFilePath = sourceBucket+sourceFile;
  
  LOG.info(sourceFilePath);
  String url = "jdbc:mysql://google/cloudsqltestdb?cloudSqlInstance=snappy-meridian-255502:us-central1:test-sql-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=root&useSSL=false";
  try (Connection con = DriverManager.getConnection(url)){
	  DatabaseMetaData meta = con.getMetaData(); 
	  rs = meta.getColumns(null,null,sourceFile.split("\\.")[0],null);
  } catch (SQLException e) {
	e.printStackTrace();
}

  
  
  
  PCollection<String> lines =p.apply("Read JSON text File", TextIO.read().from(sourceFilePath));
  PCollection<Map<String,String>> values=lines.apply("Process JSON Object", ParDo.of(new DoFn<String, Map<String,String>>() {
	  private static final long serialVersionUID = 1L;
      @ProcessElement
      public void processElement(ProcessContext c) throws ParseException, SQLException, JsonParseException, JsonMappingException, IOException {
    	  String object= c.element();
    	  JSONParser parser = new JSONParser();
    	  org.json.simple.JSONObject json = (org.json.simple.JSONObject) parser.parse(object);
    	  Map<String, String> nodeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    	  ObjectMapper mapper = new ObjectMapper();
    	  nodeMap=mapper.readValue(object, TreeMap.class);
						/*
						 * Collection values = json.values(); Iterator keys = values.iterator();
						 * ArrayList<String> valueList= new ArrayList<String>(); while (keys.hasNext())
						 * { valueList.add(keys.next().toString()); }
						 */
         c.output(nodeMap);
      }
  }));
  
  values.apply(JdbcIO.<Map<String,String>>write()
          .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
        		  .create("com.mysql.jdbc.Driver", "jdbc:mysql://google/cloudsqltestdb?cloudSqlInstance=snappy-meridian-255502:us-central1:test-sql-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=root&useSSL=false")
          )
  .withStatement("insert into customer_details values(?,?,?,?,?)")
              .withPreparedStatementSetter(new StatementSetter()));
    p.run().waitUntilFinish();
  }

}
