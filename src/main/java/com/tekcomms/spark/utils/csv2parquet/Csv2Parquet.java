package com.tekcomms.spark.utils.csv2parquet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


/**
 * Csv2Parquete
 *
 */
public class Csv2Parquet 
{
	public static void main(String[] args) {

	    String inputFile = args[0];
	    String outputFile = inputFile.replaceFirst("csv$", "parquet");
	    System.out.println("OutputFile: "  + outputFile);
	    SparkConf conf = new SparkConf().
	    		setAppName("csv2parquet").
	    		setMaster("local[1]").
	    	    set("spark.executor.memory", "512k");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
	    DataFrame dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").load(inputFile);

	    dataFrame.write().parquet("hdfs://134.64.37.92:8020/tmp/"+outputFile);
	    
	  }
}
