package org.apache.spark.sql.execution.datasources.mdjdbc.connection.utils;

import org.apache.spark.SparkFiles;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.execution.datasources.mdjdbc.JDBCOptions;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.ZipFile;

public class DriverUtils {
    private static final Logger LOG = Logger.getLogger(DriverUtils.class.getName());
    public static final String DRIVER_ZIP_FILE_PATH_PARAM_NAME = "driver_plugins";

    //"com.mysql.cj.jdbc.Driver"
    public static Driver loadDriverFromZip(String url, String zipFileName)
            throws Exception {

        URLClassLoader classLoader = getUrlClassLoader(zipFileName);

        // 4. 加载驱动类
        String driverClassName = DriverUtils.getDriverClassName(url);
        return initializeDriver(url, classLoader, driverClassName);

    }

    public static Driver loadDriverFromZip(JDBCOptions options)
            throws Exception {
        String zipFilePath = options.parameters().get(DRIVER_ZIP_FILE_PATH_PARAM_NAME).get();
        URLClassLoader classLoader = getUrlClassLoader(zipFilePath);
        // 4. 加载驱动类
        String driverClassName = DriverUtils.getDriverClassName(options.url());
        return initializeDriver(options.url(), classLoader, driverClassName);
    }
    public static Class loadDriverClass(String className,CaseInsensitiveMap<String> parameters)
            throws Exception {
        String zipFilePath = parameters.get(DRIVER_ZIP_FILE_PATH_PARAM_NAME).get();
        URLClassLoader classLoader = getUrlClassLoader(zipFilePath);
        // 4. 加载驱动类
        Class<?> aClass = classLoader.loadClass(className);
        return aClass;
    }
    @NotNull
    private static URLClassLoader getUrlClassLoader(String zipFileValue) throws Exception {
        // 1. 获取ZIP文件路径（基于网页5的Spark文件分发机制）
        if (!zipFileValue.endsWith(".zip")) {
            throw new IllegalArgumentException("Invalid ZIP file name: " + zipFileValue);
        }
        String zipPath = null;
        if (zipFileValue.startsWith("file:///")) {
            Path pathRaw = Paths.get(zipFileValue.replace("file:///", "")).normalize();
            URI uri = new URI("file:///" + pathRaw.toString().replace("\\", "/"));
            LOG.info("Method 2 URI: " + uri);
            String path = uri.getPath();
            LOG.info("从本地加载驱动:" + zipFileValue);
            zipPath = path;
            if (!new File(path).exists()) {
                throw new FileNotFoundException("meritdata mon local ZIP file not found: " + zipPath);
            }
        } else {
            LOG.info("未能从本地加载到驱动，从spark dist中加载");
            //从zipFilePath中提取文件名称
            String zipFileName = zipFileValue.substring(zipFileValue.lastIndexOf("/") + 1);
            zipPath = SparkFiles.get(zipFileName);
            if (zipPath == null) {
                throw new FileNotFoundException("meritdata mon hdfs ZIP file not found: " + zipPath);
            }
        }


        LOG.info("meritdata mon ZIP file path: " + zipPath);
        // 2. 创建临时解压目录（参考网页3的临时文件处理）
        File tempDir = Files.createTempDirectory("meritdata_mon_spark_drivers_").toFile();
        List<File> jarFiles = unzipFiles(zipPath, tempDir);

        // 3. 构建自定义类加载器
        URLClassLoader classLoader = createClassLoader(jarFiles);
        return classLoader;
    }

    private static String getDriverClassName(String url) {
        if (url.indexOf("mysql") != -1) {
            return "com.mysql.cj.jdbc.Driver";
        }
        throw new RuntimeException("不支持的数据库类型");
    }

    // 解压ZIP文件到指定目录（基于网页3的解压逻辑优化）
    private static List<File> unzipFiles(String zipPath, File outputDir) throws IOException {
        List<File> jarFiles = new ArrayList<>();
        try (ZipFile zipFile = new ZipFile(zipPath)) {
            zipFile.stream().forEach(entry -> {
                try {
                    File outputFile = new File(outputDir, entry.getName());
                    if (!entry.isDirectory()) {
                        outputFile.getParentFile().mkdirs();
                        try (InputStream is = zipFile.getInputStream(entry);
                             OutputStream os = new FileOutputStream(outputFile)) {
                            byte[] buffer = new byte[1024];
                            int len;
                            while ((len = is.read(buffer)) > 0) {
                                os.write(buffer, 0, len);
                            }
                        }
                        if (outputFile.getName().endsWith(".jar")) {
                            jarFiles.add(outputFile);
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
        return jarFiles;
    }

    // 创建自定义类加载器（基于网页4的双亲委派机制重写）
    private static URLClassLoader createClassLoader(List<File> jarFiles) throws Exception {
        URL[] urls = jarFiles.stream()
                .map(f -> {
                    try {
                        return f.toURI().toURL();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .toArray(URL[]::new);

        return new URLClassLoader(urls, null) { // 隔离父类加载器
            @Override
            public Class<?> loadClass(String name) throws ClassNotFoundException {
                synchronized (getClassLoadingLock(name)) {
                    // 优先从自定义JAR加载
                    Class<?> cls = findLoadedClass(name);
                    if (cls == null) {
                        try {
                            cls = findClass(name);
                        } catch (ClassNotFoundException ignored) {
                        }
                    }
                    return cls != null ? cls : super.loadClass(name);
                }
            }
        };
    }

    // 初始化驱动类（结合网页5的驱动加载策略）
    private static Driver initializeDriver(String url, ClassLoader classLoader, String driverClassName)
            throws Exception {

        Class<?> driverClass;
        if (driverClassName != null && !driverClassName.isEmpty()) {
            // 显式加载指定驱动类
            driverClass = classLoader.loadClass(driverClassName);
        } else {
            // 自动探测驱动
            ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(classLoader);
                return DriverManager.getDriver(url);
            } finally {
                Thread.currentThread().setContextClassLoader(originalLoader);
            }
        }
        // 实例化驱动类
        return (Driver) driverClass.getDeclaredConstructor().newInstance();
    }

    public static java.util.Enumeration<Driver> getDrivers(JDBCOptions jdbcOptions) throws Exception {
        String zipFilePath = jdbcOptions.parameters().get(DRIVER_ZIP_FILE_PATH_PARAM_NAME).get();
        if (zipFilePath != null) {
            LOG.info("");
            URLClassLoader classLoader = getUrlClassLoader(zipFilePath);
            ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(classLoader);
                return DriverManager.getDrivers();
            } finally {
                Thread.currentThread().setContextClassLoader(originalLoader);
            }
        }
        return DriverManager.getDrivers();
    }

    // 清理临时目录
    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                deleteDirectory(f);
            }
        }
        dir.delete();
    }
}
