package com.click.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.TextIO;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


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
	  ValueProvider<String> getOutput();
	  void setOutput(ValueProvider<String> value);
	  
		/*
		 * @Description("project") ValueProvider<String> getProject(); void
		 * setProject(ValueProvider<String> value);
		 * 
		 * @Description("stagingLocation") ValueProvider<String> getStagingLocation();
		 * void setStagingLocation(ValueProvider<String> value);
		 * 
		 * @Description("tempLocation") String getTempLocation(); void
		 * setTempLocation(String value);
		 * 
		 * @Description("templateLocation") ValueProvider<String> getTemplateLocation();
		 * void setTemplateLocation(ValueProvider<String> value);
		 */

  }
  
  static class CustomFn extends DoFn<Map<String,String>, String> {
	  private static final long serialVersionUID = 1L;
	    ValueProvider<String> table;
	    Map<String,List<String>> tabelData;
	    Connection con=null;
	    public CustomFn(ValueProvider<String> table,Map<String,List<String>> tabelData) {
	        this.table = table;
	        this.tabelData=tabelData;
	    }
	    private String url = "jdbc:mysql://google/cloudsqltestdb?cloudSqlInstance=snappy-meridian-255502:us-central1:test-sql-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=root&useSSL=false";	
    private Connection JdbcCreateConnection() throws SQLException {
    	Connection connection = DriverManager.getConnection(url);
    	return connection;
    }
    @Setup
    public void setup() throws SQLException {
    	con = JdbcCreateConnection();
    }
    @ProcessElement
	  public void processElement(ProcessContext c) throws SQLException  {
		  Map<String,String> element= c.element();
		  
		  Map<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
		  map.putAll(element);
		  LOG.info(map.toString());
		  
		  
		  List<String> keyList= tabelData.get(table.get());
		  LOG.info(keyList.toString());
		  String formattedQuery= getQuery(map.size());
		  PreparedStatement query =con.prepareStatement(String.format(formattedQuery, table.get()));
		  int count=0;
		  for(String key:keyList) {
	    		if(count<keyList.size())
	    		query.setString(++count, map.get(key.replaceAll("_", "")));
	    		LOG.info(map.get(key.replaceAll("_", "")));
		    	}
			  query.execute();
			  
	    }
  }
	    

  public static void main(String[] args) throws SQLException {
	  
	  PipelineOptionsFactory.register(TransformOptions.class);
	  TransformOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TransformOptions.class);  
	  
	  ValueProvider<String> table = options.getOutput();
	  Pipeline p = Pipeline.create(options);
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
  
  
  

  PCollection<String> lines =p.apply("Read JSON text File", TextIO.read().from(options.getInputFile()));
  PCollection<Map<String,String>> values=lines.apply("Process JSON Object", ParDo.of(new DoFn<String, Map<String,String>>() {
	  private static final long serialVersionUID = 1L;
	  @ProcessElement
	  public void processElement(ProcessContext c) throws ParseException, SQLException, JsonParseException, JsonMappingException, IOException {
		  String object= c.element();
		  Map<String, Object> nodeMap = new HashMap<String, Object>();
		  ObjectMapper mapper = new ObjectMapper();
		  nodeMap=mapper.readValue(object, HashMap.class);
		  Map<String,String> newMap = nodeMap.entrySet().stream()
		     .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
		  c.output(newMap);
		  }
  }));
  
  
  
	values.apply("Jdbc Write", ParDo.of(new CustomFn(table,tabelData))); 
    p.run().waitUntilFinish();
  }
  
  private static String getQuery( int size) {
	  StringBuilder query = new StringBuilder();
	  query.append("insert into %s values(");
  for(int count = 0; count<size;count++) {
	  if(count ==size-1) {
		  query.append("?");
	  }
	  else {
		  query.append("?,");
	  }
  }
  query.append(")");
	  return query.toString();
	  
  	}
  }
