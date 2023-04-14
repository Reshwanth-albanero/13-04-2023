package com.example.demo;

import lombok.val;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import javax.swing.text.Document;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class DemoApplication {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local")
				.config("spark.mongodb.input.uri", "mongodb://local.host:27017/Albanero.Product")
				.getOrCreate();
//      reading the csv file

		Dataset<Row> csv1 = spark.read().csv("/home/albanero/Downloads/Referal (1)/demo/src/main/resources/Sample-Spreadsheet-100-rows.csv");
		csv1.show(100,false);
//		 using arrays



		Dataset<String> intDataset = spark.createDataset(Arrays.asList("Reshwanth","Raju","Ravi"), Encoders.STRING());
		intDataset.show();
//		 using arraylist

		List<Row> rows = new ArrayList<>();
//		Row row = RowFactory.create("Reshwanth","Reshwanth");
		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
		rows.add(RowFactory.create("Reshwanth","Reshwanth"));
		Dataset<Row> dataset = spark.createDataset(rows, getEncode());
		dataset.show();
//		// sql queries
//		csv1.createOrReplaceTempView("csv1");
//		Dataset<Row> rowNumber = spark.sql("select _c4,row_number() over(order by _c4 DESC) as NewRow from csv1");
//		Dataset<Row> rowNumber1 = spark.sql("select _c4,rank() over(order by _c4 DESC) as NewRow from csv1");
//		Dataset<Row> rowNumber2 = spark.sql("select _c4,dense_rank() over(order by _c4 DESC) as NewRow from csv1");
//		Dataset<Row> rowNumber3 = spark.sql("select _c4,(percent_rank() over(order by _c4 DESC))*100 as NewRow from csv1");
//		Dataset<Row> rowNumber4 = spark.sql("select _c4,row_number() over(order by _c4 DESC) as NewRow from csv1");
//		// writing the files
//		csv1.write().mode(SaveMode.Append).csv("/home/albanero/Downloads/Referal (1)/demo/src/main/resources/File");

		// deleting the files
		// creating dataframe
//		Dataset<Row> dataset1 = spark.read().csv("/home/albanero/Downloads/Referal (1)/demo/src/main/resources/Sample-Spreadsheet-100-rows.csv");
//
//		File myFile = new File("/home/albanero/Downloads/Referal (1)/demo/src/main/resources/Sample-Spreadsheet-100-rows.csv");
//		if(myFile.delete()) {
//			System.out.println("Deleted the File: "+myFile.getName());
//		} else {
//			System.out.println("Failed to Delete file");
//		}
//
//		dataset1.write().csv("/home/albanero/Downloads/Referal (1)/demo/src/main/resources/Sample-Spreadsheet-100-rows.csv");
//
//		dataset1.show();

	}
	private static ExpressionEncoder<Row> getEncode(){
		StructType structType = new StructType();
		structType = structType.add("name", DataTypes.StringType,false);
		structType = structType.add("company", DataTypes.StringType,false);
		return RowEncoder.apply(structType);
	}
}