package org.apache.spark.examples.mdsql;

import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.datasources.mdjdbc.connection.utils.DriverUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.Metadata;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class JavaDistributedSQLiteDemo {
    private static final StructType RESULT_SCHEMA = new StructType(new StructField[]{
            new StructField("location", DataTypes.StringType, false, Metadata.empty()),
            new StructField("sensor_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("avg_val", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("max_val", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("total", DataTypes.IntegerType, false, Metadata.empty())
    });

    // 生成测试数据到CSV
    private static void generateTestData(String path) throws Exception {
        List<String> lines = new ArrayList<>();
        lines.add("timestamp,sensor_id,value,location");
        Random rand = new Random(42);

        for (int i = 0; i < 1000; i++) {
            String timestamp = String.format("2023-07-%02d %02d:%02d:%02d",
                    rand.nextInt(31) + 1, rand.nextInt(24), rand.nextInt(60), rand.nextInt(60));
            int sensorId = 1000 + rand.nextInt(10); // 10个传感器
            double value = 20 + rand.nextDouble() * 15;
            String location = String.format("Zone-%c", 'A' + rand.nextInt(5));

            lines.add(String.join(",",
                    timestamp,
                    String.valueOf(sensorId),
                    String.format("%.2f", value),
                    location
            ));
        }

        Files.write(Paths.get(path), lines);
    }

    public static void main(String[] args) throws Exception {
        // 生成测试数据
        String inputPath = "/tmp/sensor_data.csv";
        generateTestData(inputPath);

        SparkSession spark = SparkSession
                .builder()
                .appName("Distributed SQLite Processing Demo")
                .getOrCreate();

        // 读取CSV并重新分区
        Dataset<Row> df = spark.read()
                .option("header", true)
                .csv(inputPath)
                .repartition(4, functions.col("sensor_id")); // 按传感器ID分区

        // 分布式处理逻辑
        Dataset<Row> results = df.mapPartitions(
                (MapPartitionsFunction<Row, Row>) iter -> {
                    List<Row> output = new ArrayList<>();
                    String url = "jdbc:sqlite::memory:";
                    Properties properties = new Properties();
                    properties.put("url",url);
                    properties.put("user","");
                    properties.put("password","");
                    properties.put("driver_plugin_id", "sqlite-3.34");
                    Driver driver= DriverUtils.loadDriverFromProperties(properties);
                    // 创建内存数据库
                    try (Connection conn = driver.connect(url, properties);
                         Statement stmt = conn.createStatement()) {

                        // 1. 创建表结构
                        stmt.executeUpdate(
                                "CREATE TABLE sensor_data(" +
                                        "timestamp TEXT, sensor_id INTEGER, " +
                                        "value REAL, location TEXT)");

                        // 2. 插入分区数据
                        PreparedStatement pstmt = conn.prepareStatement(
                                "INSERT INTO sensor_data VALUES(?,?,?,?)");

                        while (iter.hasNext()) {
                            Row r = iter.next();
                            pstmt.setString(1, r.getString(0));
                            pstmt.setInt(2, Integer.parseInt(r.getString(1)));
                            pstmt.setDouble(3, Double.parseDouble(r.getString(2)));
                            pstmt.setString(4, r.getString(3));
                            pstmt.addBatch();
                        }
                        pstmt.executeBatch();

                        // 3. 执行复杂查询
                        try (ResultSet rs = stmt.executeQuery(
                                "SELECT location, sensor_id, " +
                                        "AVG(value) as avg_val, " +
                                        "MAX(value) as max_val, " +
                                        "COUNT(*) as total " +
                                        "FROM sensor_data " +
                                        "GROUP BY location, sensor_id")) {

                            while (rs.next()) {
                                output.add(RowFactory.create(
                                        rs.getString("location"),
                                        rs.getInt("sensor_id"),
                                        rs.getDouble("avg_val"),
                                        rs.getDouble("max_val"),
                                        rs.getInt("total")
                                ));
                            }
                        }
                    }
                    return output.iterator();
                }, RowEncoder.apply(RESULT_SCHEMA) // 使用RowEncoder替换原有编码器
        );

        // 汇总并显示结果
        System.out.println("===== 分布式处理结果 =====");
        results.show(20, false);

        spark.stop();
    }
}