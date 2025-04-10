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
package org.apache.spark.examples.mdsql;

// $example on:schema_merging$
// $example off:schema_merging$

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class JavaMDParallelJdbcSQLDataSourceExample {

    public static void main(String[] args) {
        System.out.println("开始并行加载过程。");
        SparkSession spark = SparkSession
                .builder()
                .appName("并行加载过程测试验证")
                .getOrCreate();
        runJdbcDatasetExample(spark);
        spark.stop();
    }

    private static void runJdbcDatasetExample(SparkSession spark) {
        // 步骤1：生成测试数据
        generateTestData(spark);
        // 步骤2：执行并行加载与统计
        parallelLoadAndStatistics(spark);
        spark.stop();
    }

    static class JdbcConfigBuilder {
        // 带参数构造方法
        public static Map<String, String> buildJdbcOptions(String tableName) {
            //hdfsuserhome
            String hdfsuserhome="/user/tempodata";
            String driverPlugin1="hdfs://nn1:8020"+hdfsuserhome
                    +"/mon_plugins/mysql-8.0/";
            Map<String, String> options = new HashMap<>();
            options.put("url", "jdbc:mysql://192.168.153.130:3306/hive");
            options.put("user", "root");
            options.put("password", "Root@123");
            options.put("driver", "com.mysql.cj.jdbc.Driver");
            options.put("dbtable", tableName);  // 动态注入表名
            options.put("driver_plugins", driverPlugin1);
            return options;
        }
    }

    public static void parallelLoadAndStatistics(SparkSession spark) {
        // 并行加载配置
        Dataset<Row> jdbcDF = spark.read()
                .format("mdjdbc")
                .options(JdbcConfigBuilder.buildJdbcOptions("test_spark_driver_classloader_user_data"))
                .option("partitionColumn", "id")          // 分区字段
                .option("lowerBound", 1)                  // 最小值
                .option("upperBound", 1000)               // 最大值
                .option("numPartitions", 5)               // 1000/5=200条/分区
                .load();

        // 执行统计计算
        System.out.println("求总数，最大和平均");
        jdbcDF.agg(
                functions.count("id").as("total_count"),
                functions.max("age").as("max_age"),
                functions.avg("age").as("avg_age")
        ).show();

        // 查看分区分布情况
        System.out.println("分区数：" + jdbcDF.rdd().partitions().length);
        jdbcDF.foreachPartition(partition -> {
            long count = 0;
            while (partition.hasNext()) {
                partition.next();
                count++;
            }
            System.out.println("当前分区数据量：" + count);
        });
    }

    // 生成测试数据并写入MySQL
    public static void generateTestData(SparkSession spark) {
        // 创建1000条测试数据
        List<Row> data = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            data.add(RowFactory.create(i, "user_" + i, (int) (Math.random() * 50 + 18)));
        }

        // 创建带明确Schema的DataFrame
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false)
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        // 写入MySQL
        df.write()
                .format("mdjdbc")
                .options(JdbcConfigBuilder.buildJdbcOptions("test_spark_driver_classloader_user_data"))
                .mode("overwrite")
                .save();
    }
}
