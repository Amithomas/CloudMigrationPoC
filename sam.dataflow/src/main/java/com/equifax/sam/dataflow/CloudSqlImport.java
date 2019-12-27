package com.equifax.sam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;

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

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CloudSqlImport  {

	private static final Logger LOG = LoggerFactory.getLogger(CloudSqlImport.class);
    private static final String URL = "jdbc:mysql://google/cloudsqltestdb?cloudSqlInstance=pragmatic-braid-263313:us-central1:test-sql-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=root&password=root&useSSL=false";
	
	
		
	
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
	  PCollectionView<Map<String,String>> statSideInput;
	  public CustomUpdateFn(ValueProvider<String> table,PCollectionView<Map<String,String>> statSideInput) {
	        this.table = table;
	        this.statSideInput=statSideInput;
	    }
	  @ProcessElement 
	  public void process(ProcessContext c) throws SQLException {
		   Integer count = c.element();
		   Map<String,String> stats =c.sideInput(statSideInput);
		   LOG.info(count.toString());
		   String endTime = getCurrentDateTime();
		   Connection con2 = DriverManager.getConnection(URL);
		   PreparedStatement query =con2.prepareStatement("insert into adabas_job_statistics values(?,?,?,?,?,?,?)");
		   query.setString(1, "test");
		   query.setString(2, table.get());
		   query.setString(3, "Done");
		   query.setString(4, stats.get("startTime"));
		   query.setString(5, endTime);
		   query.setString(6, stats.get("recordCount"));
		   query.setInt(7, count);
		   try {
		   query.execute();
		   }catch (SQLException e) {
				e.printStackTrace();
			} 
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
    private Connection JdbcCreateConnection() throws SQLException {
    	Connection connection = DriverManager.getConnection(URL);
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
		  LOG.info(map.toString());
		  List<String> keyList= tabelData.get(table.get());
		  LOG.info(keyList.toString());
		  String formattedQuery= getQuery(map.size());
		  try {
		  PreparedStatement query =con.prepareStatement(String.format(formattedQuery, table.get()));
		  int count=0;
		  for(String key:keyList) {
	    		if(count<keyList.size())
	    		query.setString(++count, map.get(key.replaceAll("_", "")));
	    		LOG.info(map.get(key.replaceAll("_", "")));
		    	}
		  Integer i = query.executeUpdate();
		  if (i > 0) {
	            c.output(1);
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
	  
  Connection con = DriverManager.getConnection(URL);
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
  PCollection<String>jobStartTime=  p.apply("Stats Initializing",Create.of(getCurrentDateTime()));
  final PCollectionView<String> jobStartTimeSideInput =jobStartTime.apply("Start Time Side Input",View.<String>asSingleton());
  
  PCollection<String> lines =p.apply("Read JSON text File", TextIO.read().from(options.getInputFile()));
  PCollection<Integer> lineCount= lines.apply("Get Total Record Count", ParDo.of(new DoFn<String, Integer>() {
	  private static final long serialVersionUID = 1L;
	  @ProcessElement
	  public void processElement(ProcessContext c) {
		  c.output(1);
		  }
  }));
  
  PCollection<Integer> recordCount = lineCount.apply("Computing Record Count",Sum.integersGlobally());
 
  
  PCollection<Map<String,String>> initialStats= recordCount.apply("Combining Stats",ParDo.of(new DoFn<Integer, Map<String,String>>() {
       private static final long serialVersionUID = 1L;
                    @ProcessElement
                    public void process(ProcessContext c) {
                    	Integer Rcount= c.element();
                    	String Rtime = c.sideInput(jobStartTimeSideInput);
                    	Map<String,String> statsMap= new HashMap<String,String>();
                    	statsMap.put("recordCount", Rcount.toString());
                    	statsMap.put("startTime",Rtime);
                    	c.output(statsMap);
                    }
                  })
              .withSideInputs(jobStartTimeSideInput));
  
  final PCollectionView<Map<String,String>> statSideInput =initialStats.apply("Stats Side Input",View.<Map<String,String>>asSingleton());
  
  
  
  PCollection<Map<String,String>> values=lines.apply("Process JSON Object", ParDo.of(new DoFn<String, Map<String,String>>() {
	  private static final long serialVersionUID = 1L;
	  @ProcessElement
	  public void processElement(ProcessContext c) throws  JsonParseException, JsonMappingException, IOException {
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
  
  sum.apply("Update Job Statistics",ParDo.of(new CustomUpdateFn(table,statSideInput)).withSideInputs(statSideInput)); 
  
  //p.run().waitUntilFinish();
  
  PipelineResult result = p.run();
  try {
      result.getState();
      result.waitUntilFinish();
  } catch (UnsupportedOperationException e) {
  } catch (Exception e) {
      e.printStackTrace();
  }
    
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
  private static String getCurrentDateTime() {
	  java.util.Date dt = new java.util.Date();
	   java.text.SimpleDateFormat sdf = 
			     new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	   sdf.setTimeZone(TimeZone.getTimeZone("IST"));
	   String currentTime = sdf.format(dt);
	   return currentTime;
  }
  }
