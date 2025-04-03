package org.apache.spark.sql.execution.datasources.mdjdbc.connection.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.execution.datasources.mdjdbc.JDBCOptions;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;
import java.util.zip.ZipFile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;

public class DriverUtils {
    private static final Logger LOG = Logger.getLogger(DriverUtils.class.getName());
    public static final String DRIVER_ZIP_FILE_PATH_PARAM_NAME = "driver_plugins";
    public static final String MERITDATA_MON_SPARK_DRIVERS = "meritdata_mon_spark_drivers_";
    private static ConcurrentMap<String, URLClassLoader> classLoaderMap = new ConcurrentHashMap<>();
    static {
        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down DriverUtils and cleaning up class loaders...");
            classLoaderMap.forEach((key, classLoader) -> {
                try {
                    // 关闭类加载器
                    if (classLoader instanceof Closeable) {
                        ((Closeable) classLoader).close();
                    }
                } catch (Exception e) {
                    LOG.severe("Error cleaning up class loader for key: " + key + " - " + e.getMessage());
                }
            });
            classLoaderMap.clear();
            LOG.info("Class loaders and temporary directories cleaned up.");
        }));
    }

    // 其他方法保持不变...



    //"com.mysql.cj.jdbc.Driver"
    public static Driver loadDriverFromPath(String url, String filePath)
            throws Exception {
        URLClassLoader classLoader = getUrlClassLoader(filePath);
        // 4. 加载驱动类
        String driverClassName = DriverUtils.getDriverClassName(url);
        return initializeDriver(url, classLoader, driverClassName);
    }

    public static Driver loadDriverFromPath(JDBCOptions options)
            throws Exception {
        String zipFilePath = options.parameters().get(DRIVER_ZIP_FILE_PATH_PARAM_NAME).get();
        URLClassLoader classLoader = getUrlClassLoader(zipFilePath);
        // 4. 加载驱动类
        String driverClassName = DriverUtils.getDriverClassName(options.url());
        return initializeDriver(options.url(), classLoader, driverClassName);
    }

    public static Class<?> loadDriverClass(String className, CaseInsensitiveMap<String> parameters)
            throws Exception {
        String zipFilePath = parameters.get(DRIVER_ZIP_FILE_PATH_PARAM_NAME).get();
        URLClassLoader classLoader = getUrlClassLoader(zipFilePath);
        // 4. 加载驱动类
        return classLoader.loadClass(className);
    }


    private static URLClassLoader getUrlClassLoader(String monPluginFileValue) throws Exception {
        if (monPluginFileValue == null || monPluginFileValue.isEmpty()) {
            throw new IllegalArgumentException(" file path cannot be null or empty");
        }
        List<File> jarFiles = new ArrayList<>();
        if (monPluginFileValue.endsWith(".zip")) {
            jarFiles = loadJarFilesFormZip(monPluginFileValue);
        } else {
            jarFiles = loadJarFilesFromFolder(monPluginFileValue);
        }
        // 3. 构建自定义类加载器
        URLClassLoader urlClassLoader = classLoaderMap.get(monPluginFileValue);
        if (urlClassLoader != null) {
            LOG.info("从缓存中获取驱动类加载器,key:" + monPluginFileValue);
            return urlClassLoader;
        }
        URLClassLoader classLoader = createClassLoader(jarFiles);
        classLoaderMap.put(monPluginFileValue, classLoader);
        LOG.info("构建驱动类加载器,并写入缓存,key:" + monPluginFileValue);
        return classLoader;
    }


    private static List<File> loadJarFilesFormZip(String zipFileValue) throws URISyntaxException, IOException {
        // 1. 获取ZIP文件路径（基于网页5的Spark文件分发机制）
        if (!zipFileValue.endsWith(".zip")) {
            throw new IllegalArgumentException("Invalid ZIP file name: " + zipFileValue);
        }
        String zipPath = null;
        if (zipFileValue.startsWith("file:///")) {
            java.nio.file.Path pathRaw = Paths.get(zipFileValue.replace("file:///", "")).normalize();
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

        File tempDir = crateTempoDirToSparkRootDir();
        List<File> jarFiles = unzipFiles(zipPath, tempDir);
        return jarFiles;
    }

    @NotNull
    private static File crateTempoDirToSparkRootDir() throws IOException {
        String rootDirectory = SparkFiles.getRootDirectory();
        java.nio.file.Path path = Paths.get(rootDirectory);
        return Files.createTempDirectory(path, MERITDATA_MON_SPARK_DRIVERS).toFile();
    }

    private static List<File> loadJarFilesFromFolder(String folderPath) throws IOException, URISyntaxException {
        // 1. 获取文件夹路径

        String jarFolderPath = null;
        if (folderPath.startsWith("file:///")) {
            if (!new File(folderPath).isDirectory()) {
                throw new IllegalArgumentException("Invalid folder path: " + folderPath);
            }
            jarFolderPath = getJarFolderPathFroFilePrefix(folderPath);
        } else if (folderPath.startsWith("/")) {
            if (!new File(folderPath).isDirectory()) {
                throw new IllegalArgumentException("Invalid folder path: " + folderPath);
            }
            jarFolderPath = folderPath;
            if (!new File(folderPath).exists()) {
                throw new FileNotFoundException("meritdata mon hdfs ZIP file not found: " + jarFolderPath);
            }
        } else if (folderPath.startsWith("hdfs://") || folderPath.startsWith("obs://")) {
            //从hdfs下载到本地
            LOG.info("hdfs 或者 obs协议，从文件夹加载驱动: " + folderPath);
            jarFolderPath = downloadFromHdfsToLocal(folderPath);
        } else {
            throw new IllegalArgumentException("不支持的文件协议: " + folderPath);
        }

        LOG.info("从文件夹加载驱动: " + jarFolderPath);

        // 2. 获取文件夹中的所有JAR文件
        File folder = new File(jarFolderPath);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".jar"));

        if (files == null || files.length == 0) {
            throw new FileNotFoundException("No JAR files found in folder: " + jarFolderPath);
        }

        List<File> jarFiles = Arrays.asList(files);
        return jarFiles;
    }

    private static String downloadFromHdfsToLocal(String hdfsFolderPath) throws FileNotFoundException {
        SparkContext sc = SparkContext.getOrCreate();
        Configuration configuration = sc.hadoopConfiguration();
        try {
            FileSystem fs = FileSystem.get(new URI(hdfsFolderPath), configuration);
            Path hdfsPath = new Path(hdfsFolderPath);
            boolean exists = fs.exists(hdfsPath);
            if (!exists) {
                String defaultFs = configuration.get("fs.defaultFS");
                throw new FileNotFoundException("Folder [" + hdfsFolderPath + "] not found in hdfs: " + defaultFs);
            }
            FileStatus[] fileStatuses = fs.listStatus(hdfsPath);

            // 创建临时本地目录
            File localDir = crateTempoDirToSparkRootDir();
            String localPath = localDir.getAbsolutePath();

            for (FileStatus fileStatus : fileStatuses) {
                Path filePath = fileStatus.getPath();
                String fileName = filePath.getName();
                Path localFilePath = new Path(localPath, fileName);
                // 下载文件到本地
                fs.copyToLocalFile(filePath, localFilePath);
                LOG.info(" meritdata 从HDFS下载文件: " + filePath + " 到本地: " + localFilePath);
            }
            //检查下载的目录是否为空，并打印下载到本地的所有文件夹中的文件。
            // 检查下载的目录是否为空，并打印下载到本地的所有文件夹中的文件
            File[] downloadedFiles = localDir.listFiles();
            if (downloadedFiles == null || downloadedFiles.length == 0) {
                throw new FileNotFoundException("No files downloaded to local directory: " + localPath + " , from: " + hdfsFolderPath + " .");
            }
            LOG.info("meritdata 下载到本地的所有文件:");
            for (File file : downloadedFiles) {
                LOG.info("meritdata 文件: " + file.getAbsolutePath());
            }

            return localPath;
        } catch (Exception e) {
            throw new FileNotFoundException("Failed to download folder from HDFS: " + hdfsFolderPath);
        }
    }


    private static String getJarFolderPathFroFilePrefix(String folderPath) throws URISyntaxException, FileNotFoundException {
        LOG.info("meritdata  getJarFolderPathFroFilePrefix:" + folderPath);
        java.nio.file.Path pathRaw = Paths.get(folderPath.replace("file:///", "")).normalize();
        URI uri = new URI("file:///" + pathRaw.toString().replace("\\", "/"));
        LOG.info("meritdata  URI: " + uri);
        String path = uri.getPath();
        LOG.info("meritdata 从本地加载驱动:" + folderPath);
        if (!new File(path).exists()) {
            throw new FileNotFoundException("meritdata mon local ZIP file not found: " + path);
        }
        return path;
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
    private static synchronized URLClassLoader createClassLoader(List<File> jarFiles) throws Exception {
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
