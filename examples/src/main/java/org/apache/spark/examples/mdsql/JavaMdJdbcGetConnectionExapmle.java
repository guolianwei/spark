package org.apache.spark.examples.mdsql;

import org.apache.spark.sql.execution.datasources.mdjdbc.connection.utils.DriverUtils;

import java.sql.*;
import java.util.Properties;

public class JavaMdJdbcGetConnectionExapmle {
    public static void main(String[] args) throws Exception {
        // 数据库连接参数
        String url = "jdbc:mysql://localhost:3306/mydatabase?useSSL=false&serverTimezone=UTC";
        String user = "root";
        String password = "123456";
        // 使用try-with-resources自动关闭资源（JDK7+特性）
        Properties properties = new Properties();
        properties.put("url",url);
        properties.put("user",user);
        properties.put("password",password);
        Driver driver= DriverUtils.loadDriverFromProperties(properties);
        try (Connection conn = driver.connect(url, properties);
             Statement stmt = conn.createStatement()) {

            // 1. 加载驱动（MySQL 8.0+无需显式加载，但建议保留）
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 2. 执行查询
            String sql = "SELECT id, name FROM users";
            try (ResultSet rs = stmt.executeQuery(sql)) {
                // 3. 处理结果集
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    System.out.println("ID: " + id + ", Name: " + name);
                }
            }
        } catch (ClassNotFoundException e) {
            System.err.println("驱动未找到：" + e.getMessage());
        } catch (SQLException e) {
            System.err.println("数据库异常：" + e.getMessage());
            e.printStackTrace();
        }
    }
}
