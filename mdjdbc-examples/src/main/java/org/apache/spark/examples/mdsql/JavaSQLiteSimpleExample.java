package org.apache.spark.examples.mdsql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.mdjdbc.connection.utils.DriverUtils;

import java.sql.*;
import java.util.Properties;

public class JavaSQLiteSimpleExample {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java SQLite Example")
                .getOrCreate();

        // 使用内存数据库作为临时库（连接关闭后自动销毁）
        String url = "jdbc:sqlite::memory:";
        Properties properties = new Properties();
        properties.put("url",url);
        properties.put("user","");
        properties.put("password","");
        properties.put("driver_plugin_id", "sqlite-3.34");
        Driver driver= DriverUtils.loadDriverFromProperties(properties);
        try (Connection conn = driver.connect(url, properties);
             Statement stmt = conn.createStatement()) {
            
            // 1. 创建临时表
            stmt.executeUpdate(
                "CREATE TABLE temp_sensor_data (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "sensor_id INTEGER NOT NULL," +
                "value REAL," +
                "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)");
            
            // 2. 插入示例数据
            stmt.addBatch("INSERT INTO temp_sensor_data(sensor_id, value) VALUES (101, 23.5)");
            stmt.addBatch("INSERT INTO temp_sensor_data(sensor_id, value) VALUES (102, 19.8)");
            stmt.addBatch("INSERT INTO temp_sensor_data(sensor_id, value) VALUES (101, 25.1)");
            stmt.addBatch("INSERT INTO temp_sensor_data(sensor_id, value) VALUES (103, 21.3)");
            stmt.executeBatch();

            // 3. 执行统计查询
            String statsSql = "SELECT " +
                "COUNT(*) AS total_records, " +
                "AVG(value) AS average_value, " +
                "MAX(value) AS max_value, " +
                "MIN(value) AS min_value " +
                "FROM temp_sensor_data";
            
            try (ResultSet rs = stmt.executeQuery(statsSql)) {
                // 4. 输出统计结果
                ResultSetMetaData meta = rs.getMetaData();
                System.out.println("\n===== 传感器数据统计 =====");
                while (rs.next()) {
                    for (int i=1; i<=meta.getColumnCount(); i++) {
                        String colName = meta.getColumnName(i);
                        Object value = rs.getObject(i);
                        System.out.printf("%-15s: %s\n", colName, value);
                    }
                }
            }

            // 5. 分组统计示例（按传感器ID）
            String groupStatsSql = "SELECT sensor_id, " +
                "COUNT(*) AS readings, " +
                "ROUND(AVG(value),2) AS avg_value " +
                "FROM temp_sensor_data " +
                "GROUP BY sensor_id";
            
            try (ResultSet rs = stmt.executeQuery(groupStatsSql)) {
                System.out.println("\n===== 按传感器分组统计 =====");
                while (rs.next()) {
                    System.out.printf(
                        "传感器ID: %d | 读数次数: %d | 平均值: %.2f\n",
                        rs.getInt("sensor_id"),
                        rs.getInt("readings"),
                        rs.getDouble("avg_value")
                    );
                }
            }
            
        } catch (SQLException e) {
            System.err.println("数据库异常：" + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}