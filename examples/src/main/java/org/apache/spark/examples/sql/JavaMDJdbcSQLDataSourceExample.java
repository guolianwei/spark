/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql;

// $example on:schema_merging$
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
// $example off:schema_merging$
import java.util.Properties;

// $example on:basic_parquet_example$
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
// $example on:schema_merging$
// $example on:json_dataset$
// $example on:csv_dataset$
// $example on:text_dataset$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off:text_dataset$
// $example off:csv_dataset$
// $example off:json_dataset$
// $example off:schema_merging$
// $example off:basic_parquet_example$
import org.apache.spark.sql.SparkSession;

public class JavaMDJdbcSQLDataSourceExample {

  public static void main(String[] args) {
    System.out.println("中文!");
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate();
    runJdbcDatasetExample(spark);

    spark.stop();
  }
  private static void runJdbcDatasetExample(SparkSession spark) {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      System.out.println("MySQL JDBC 驱动未找到");
      e.printStackTrace();
//      return;
    }
    String driverPlugin="file:///C:\\Users\\29267\\.m2\\com\\mysql\\mysql-connector-j\\8.0.33\\mysql-8.0.33.zip";
    Dataset<Row> jdbcDF = spark.read()
      .format("mdjdbc")
      .option("url", "jdbc:mysql://192.168.153.130:3306")
      .option("dbtable", "hive.tbls")
      .option("user", "root")
      .option("password", "Root@123")
      .option("driver_plugins", driverPlugin)
      .load();
    jdbcDF.show();

    Properties connectionProperties = new Properties();
    connectionProperties.put("user", "root");
    connectionProperties.put("password", "Root@123");
    connectionProperties.put("driver_plugins", driverPlugin);
    Dataset<Row> jdbcDF2 = spark.read().format("mdjdbc")
      .jdbc("jdbc:mysql:192.168.153.130:3306", "hive.tbls", connectionProperties);
    jdbcDF2.show();

    // Saving data to a JDBC source
    jdbcDF.write()
      .format("mdjdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save();

    jdbcDF2.write()
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

    // Specifying create table column data types on write
    jdbcDF.write()
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
    // $example off:jdbc_dataset$
  }
}
