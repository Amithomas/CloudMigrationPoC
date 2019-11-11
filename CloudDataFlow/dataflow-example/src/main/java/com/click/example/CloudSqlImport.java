package com.click.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;

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

  }
  
  static class CustomUpdateFn extends DoFn<Integer, Integer>{
	  private static final long serialVersionUID = 1L; 
	  ValueProvider<String> table;
	  public CustomUpdateFn(ValueProvider<String> table) {
	        this.table = table;
	    }
	  @ProcessElement 
	  public void process(ProcessContext c) throws SQLException {
		   Integer count = c.element();
		   LOG.info(count.toString());
		   String url2 = "jdbc:mysql://google/cloudsqltestdb?cloudSqlInstance=snappy-meridian-255502:us-central1:test-sql-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=root&useSSL=false";
		   Connection con2 = DriverManager.getConnection(url2);
		   PreparedStatement query =con2.prepareStatement("insert into adabas_job_statistics values(?,?,?,?,?,?,?)");
		   query.setString(1, "test");
		   query.setString(2, table.get());
		   query.setString(3, null);
		   query.setString(4, null);
		   query.setString(5, null);
		   query.setString(6, null);
		   query.setInt(7, count);
		   query.execute();
	   }
	  
  }
  
  
  
  static class CustomJdbcInsertFn extends DoFn<Map<String,String>, Integer> {
	  private static final long serialVersionUID = 1L;
	    ValueProvider<String> table;
	    Map<String,List<String>> tabelData;
	    Connection con=null;
	    public CustomJdbcInsertFn(ValueProvider<String> table,Map<String,List<String>> tabelData) {
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
	  public void processElement(ProcessContext c)   {
		  Map<String,String> element= c.element();
		  
		  Map<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
		  map.putAll(element);
		  List<String> keyList= tabelData.get(table.get());
		  String formattedQuery= getQuery(map.size());
		  try {
		  PreparedStatement query =con.prepareStatement(String.format(formattedQuery, table.get()));
		  int count=0;
		  for(String key:keyList) {
	    		if(count<keyList.size())
	    		query.setString(++count, map.get(key.replaceAll("_", "")));
		    	}
		  Integer i = query.executeUpdate();
		  if (i > 0) {
	            c.output(1);;
	        }
		  } catch (SQLException e) {
			  	c.output(0);
				e.printStackTrace();
			} 
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
  
  
  PCollection<Integer>statusValues=values.apply("Jdbc Write", ParDo.of(new CustomJdbcInsertFn(table,tabelData)));
  
  PCollection<Integer> sum = statusValues.apply("Get Inserted Record Count",Sum.integersGlobally());
  
  sum.apply("Update Job Statistics Table",ParDo.of(new CustomUpdateFn(table))); 
  
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
