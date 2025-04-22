package org.apache.spark.examples.mdsql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.mdjdbc.connection.utils.DriverUtils;
import sun.reflect.generics.reflectiveObjects.LazyReflectiveObjectGenerator;

import java.sql.*;
import java.util.Properties;

public class JavaMdJdbcGetConnectionExample {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java JavaMdJdbcGetConnectionExample example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        String mysqlhost = "192.168.153.130";
        if (args.length == 1) {
            mysqlhost = args[0];
        }
        System.out.println("mysqlhost:"+mysqlhost);
        // 数据库连接参数
        String url = "jdbc:mysql://"+mysqlhost+":3306/hive?useSSL=false&serverTimezone=UTC";
        String user = "root";
        String password = "Root@123";
        // 使用try-with-resources自动关闭资源（JDK7+特性）
        Properties properties = new Properties();
        properties.put("url",url);
        properties.put("user",user);
        properties.put("password",password);
        properties.put("driver_plugin_id", "mysql-8.0.29");
        Driver driver= DriverUtils.loadDriverFromProperties(properties);

        try (Connection conn = driver.connect(url, properties);
             Statement stmt = conn.createStatement()) {
            Thread.currentThread().setContextClassLoader(driver.getClass().getClassLoader());
            // 1. 加载驱动（MySQL 8.0+无需显式加载，但建议保留）
            Class.forName("com.mysql.cj.jdbc.Driver", true, driver.getClass().getClassLoader());

            // 2. 执行查询
            String sql = "SELECT * FROM hive.tbls";
            try (ResultSet rs = stmt.executeQuery(sql)) {
                // 3. 处理结果集
                while (rs.next()) {
                    int id = rs.getInt("TBL_ID");
                    String name = rs.getString("TBL_NAME");
                    System.out.println("ID: " + id + ", Name: " + name);
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("驱动未找到：" + e.getMessage());
        } catch (SQLException e) {
            System.err.println("数据库异常：" + e.getMessage());
            e.printStackTrace();
        }
        spark.stop();
    }
}
