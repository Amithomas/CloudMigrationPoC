package com.click.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
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
import java.util.stream.Collectors;

import org.slf4j.Logger;


import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CloudSqlImport  {

	private static final Logger LOG = LoggerFactory.getLogger(CloudSqlImport.class);

	
	
		
	
  public interface TransformOptions  extends PipelineOptions  {
	  @Description("Path of the file to read from")
	  ValueProvider<String> getInputFile();
	    void setInputFile(ValueProvider<String> value);
	  
	  @Description("table")
	  String getOutput();
	  void setOutput(String output);

  }
  
  
  static class StatementSetter implements JdbcIO.PreparedStatementSetter<Map<String,String>>
  {
	  Map<String,List<String>> dbMeta;
	  String targetTable;
    private static final long serialVersionUID = 1L;
    StatementSetter(Map<String,List<String>> tableData, String tableName){
    	dbMeta=tableData;
    	targetTable=tableName;
    	LOG.info(tableName);
    }
    public void setParameters(Map<String,String> element, PreparedStatement query) throws Exception
    {	LOG.info(targetTable);
    	LOG.info(dbMeta.toString());
    	List<String> keyList= dbMeta.get("customer_details");
    	LOG.info(keyList.toString());
    	Map<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    	map.putAll(element);
    	int count=0;
    	LOG.info(String.valueOf(keyList.size()));
    	for(String key:keyList) {
    		if(count<keyList.size())
    		query.setString(++count, map.get(key.replaceAll("_", "")));
    		LOG.info(key);
    	}
    	LOG.info(query.toString());
    }
  }

  public static void main(String[] args) throws SQLException {
	  String sourceBucket = "gs://triggerbucket-1/";
	  PipelineOptionsFactory.register(TransformOptions.class);
	  TransformOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TransformOptions.class);      
  Pipeline p = Pipeline.create(options);
  //String sourceFile=options.getInputFile();
  //sourceFile.trim();
  String tableName= options.getOutput();
		/*
		 * if(sourceFile!=null || !sourceFile.isEmpty()||sourceFile.length()!=0) {
		 * sourceFilePath = sourceBucket+sourceFile; } else { sourceFilePath=sourceFile;
		 * }
		 */
  String url = "jdbc:mysql://google/cloudsqltestdb?cloudSqlInstance=snappy-meridian-255502:us-central1:test-sql-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=root&useSSL=false";
   Connection con = DriverManager.getConnection(url);
	  DatabaseMetaData meta = con.getMetaData();
	  Map<String,List<String>> tabelData= new HashMap<String,List<String>>();
	  ResultSet rs = meta.getTables(null, null, "%", null);
	  while (rs.next()) {
		  String metaTableName= rs.getString(3);
		  ResultSet rsColumns= meta.getColumns(null,null,metaTableName,null);
		  List<String> columnList= new ArrayList<String>();
		  while(rsColumns.next()){
			  columnList.add(rsColumns.getString("COLUMN_NAME"));
    		  }
		  tabelData.put(metaTableName,columnList);
		}
	  //ResultSet rs = meta.getColumns(null,null,options.getOutput(),null);
  
		/*
		 * while(rs.next()){ keyList.add(rs.getString("COLUMN_NAME")); }
		 */

  


  PCollection<String> lines =p.apply("Read JSON text File", TextIO.read().from(options.getInputFile()));
  PCollection<Map<String,String>> values=lines.apply("Process JSON Object", ParDo.of(new DoFn<String, Map<String,String>>() {
  private static final long serialVersionUID = 1L;
  @ProcessElement
  public void processElement(ProcessContext c) throws ParseException, SQLException, JsonParseException, JsonMappingException, IOException {
	  String object= c.element();
	  JSONParser parser = new JSONParser();
	  org.json.simple.JSONObject json = (org.json.simple.JSONObject) parser.parse(object);
	  Map<String, Object> nodeMap = new HashMap<String, Object>();
	  ObjectMapper mapper = new ObjectMapper();
	  nodeMap=mapper.readValue(object, HashMap.class);
	  Map<String,String> newMap = nodeMap.entrySet().stream()
			     .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
         c.output(newMap);
      }
  }));
  
  values.apply(JdbcIO.<Map<String,String>>write()
          .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
        		  .create("com.mysql.jdbc.Driver", "jdbc:mysql://google/cloudsqltestdb?cloudSqlInstance=snappy-meridian-255502:us-central1:test-sql-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=root&useSSL=false")
          )
  .withStatement("insert into "+tableName+" values(?,?,?,?,?)")
              .withPreparedStatementSetter(new StatementSetter(tabelData,tableName)));
    p.run().waitUntilFinish();
  }
  
}
