package com.example.demo;

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DemoApplication {
	public static void main(String[] args) {
//		SparkSession spark = SparkSession.builder()
//				.appName("Myname")
//				.master("local")
//				.getOrCreate();
		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
		Dataset<Row> csv1 = spark.read().csv("/home/albanero/Downloads/Referal (1)/demo/src/main/resources/Sample-Spreadsheet-100-rows.csv");
		csv1.show();
/**		// using list
//		Dataset<String> intDataset = spark.createDataset(Arrays.asList("Reshwanth","Raju","Ravi"), Encoders.STRING());
//		intDataset.show();
		// using arraylist creating the ROw list which stores the values of the columns
		// and then i will try to create the schema in the tables (like name of the rows and
		// https://www.youtube.com/watch?v=gHAFeKGCRV8&list=PLm3O9IUTfMrcqsAjjxRSpj79unJPYyEhq&index=2
//		List<Row> rows = new ArrayList<>();
////		Row row = RowFactory.create("Reshwanth","Reshwanth");
//		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
//		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
//		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
//		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
//		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
//		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
//		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
//		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
//		Dataset<Row> dataset = spark.createDataset(rows, getEncode());
//		dataset.show();
		**/
		/*
		https://www.youtube.com/watch?v=QA2-LTBAZkQ&list=PLm3O9IUTfMrcqsAjjxRSpj79unJPYyEhq&index=4
		Ranking functions -- row_number() -->
		 */
		csv1.createOrReplaceTempView("csv1");
		Dataset<Row> rowNumber = spark.sql("select _c4,row_number()" +
				"over(order by _c4 ASC) as rowNumber from csv1");
		rowNumber.show();
	}
	private static ExpressionEncoder<Row> getEncode(){
		StructType structType = new StructType();
		structType = structType.add("name", DataTypes.StringType,false);
		structType = structType.add("company", DataTypes.StringType,false);
		return RowEncoder.apply(structType);
	}
}