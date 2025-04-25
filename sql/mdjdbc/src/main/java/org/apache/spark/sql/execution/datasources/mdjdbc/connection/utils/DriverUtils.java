package org.apache.spark.sql.execution.datasources.mdjdbc.connection.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;
import java.util.zip.ZipFile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class DriverUtils {
    private static final Logger LOG = Logger.getLogger(DriverUtils.class.getName());
    public static final String MERITDATA_MON_SPARK_DRIVERS = "meritdata_mon_spark_drivers_";
    public static final String DRIVER_PLUGIN_ID = "driver_plugin_id";
    private static final ConcurrentMap<String, URLClassLoader> classLoaderMap = new ConcurrentHashMap<>();

    static {
        // 注册关闭钩子，避免进程停止时无法删除本地的临时jar文件。
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down DriverUtils and cleaning up class loaders...");
            classLoaderMap.forEach((key, classLoader) -> {
                try {
                    // 关闭类加载器
                    if (classLoader != null) {
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


    public static Driver loadDriverFromParmeters(CaseInsensitiveMap<String> parameters) throws Exception {
        String url = parameters.get("url").get();
        URLClassLoader classLoader = getUrlClassLoader(parameters);
        // 4. 加载驱动类
        String driverClassName = DriverUtils.getDriverClassName(url);
        return initializeDriver(url, classLoader, driverClassName);
    }

    public static Driver loadDriverFromProperties(Properties properties) throws Exception {
        String url = (String) properties.get("url");
        URLClassLoader classLoader = getUrlClassLoader(properties);
        // 4. 加载驱动类
        String driverClassName = DriverUtils.getDriverClassName(url);
        return initializeDriver(url, classLoader, driverClassName);
    }

    private static URLClassLoader getUrlClassLoader(CaseInsensitiveMap<String> parameters) throws Exception {
        String driverId = parameters.get(DRIVER_PLUGIN_ID).get();
        if (driverId == null || driverId.isEmpty()) {
            LOG.warning("driver_plugin_id is null or empty");
        }
        String filePath = extractPathFromSparkFiles(driverId);
        LOG.info("extractPathFromSparkFiles:" + filePath);
        return getUrlClassLoader(filePath);
    }

    private static URLClassLoader getUrlClassLoader(Properties parameters) throws Exception {
        String driverId = (String) parameters.get(DRIVER_PLUGIN_ID);
        if (driverId == null || driverId.isEmpty()) {
            LOG.warning("driver_plugin_id is null or empty");
        }
        String filePath = extractPathFromSparkFiles(driverId);
        LOG.info("extractPathFromSparkFiles:" + filePath);
        return getUrlClassLoader(filePath);
    }

    private static String extractPathFromSparkFiles(String driverId) throws IOException {
        String property = System.getProperty("cloud.mon.plugins.home");
        //如果是本地模式直接返回目录
        String s1 = localMode(driverId, property);
        if (s1 != null) return s1;

        return yarnMode(driverId);
    }

    private static String yarnMode(String driverId) throws IOException {
        String zipFileName = driverId + ".zip";
        String s = SparkFiles.get(zipFileName);
        if (s != null) {
            if (Files.exists(Paths.get(s))) {
                return s;
            } else {
                LOG.info("can not load file from SparkFiles.get:" + s);
            }
        }
        URL resource = DriverUtils.class.getClassLoader().getResource(zipFileName);
        if (resource != null) {
            LOG.info("loading file from classLoader:" + resource.getPath());
            return resource.getPath();
        }
        throw new IOException("Failed to find driver plugin file: " + zipFileName
                + " by plugin id: " + zipFileName);
    }


    private static String localMode(String driverId, String property) throws IOException {
        if (property != null && !property.isEmpty()) {
            LOG.info("cloud.mon.plugins.home:" + property + " is not null,use it to load driver");
            String s = property + File.separator + driverId;
            LOG.info("loading file:" + s);
            File file = new File(s);
            if (file.exists()) {
                LOG.info("file:" + s + " is exists,use it to load driver");
                return s;
            } else {
                LOG.info("file:" + s + " is not exists,use SparkFiles.get to load driver");
                throw new IOException("Failed to find driver plugin file: " + s);
            }
        } else {
            LOG.info("cloud.mon.plugins.home is null or empty,use SparkFiles.get to load driver");
        }
        return null;
    }

    public static Class<?> loadDriverClass(String className, CaseInsensitiveMap<String> parameters) throws Exception {
        URLClassLoader classLoader = getUrlClassLoader(parameters);
        // 4. 加载驱动类
        return classLoader.loadClass(className);
    }


    private static URLClassLoader getUrlClassLoader(String monPluginFileValue) throws Exception {
        if (monPluginFileValue == null || monPluginFileValue.isEmpty()) {
            throw new IllegalArgumentException(" file path cannot be null or empty");
        }
        // 1. 判断缓存中是否已经存在该驱动类加载器
        URLClassLoader urlClassLoader = classLoaderMap.get(monPluginFileValue);
        if (urlClassLoader != null) {
            LOG.info("Using the cached class loader, key: " + monPluginFileValue);
            return urlClassLoader;
        }
        //2. 判断文件路径是否为hdfs路径，如果是，则从hdfs下载zip文件到本地临时目录，并返回本地路径
        List<File> jarFiles;
        if (isALocalZip(monPluginFileValue)) {
            jarFiles = loadJarFilesFormZip(monPluginFileValue);
        } else {
            jarFiles = loadJarFilesFromFolder(monPluginFileValue);
        }
        // 3. 构建自定义类加载器
        URLClassLoader classLoader = createClassLoader(jarFiles);
        classLoaderMap.put(monPluginFileValue, classLoader);
        LOG.info("Constructing the driver class loader and writing it to the cache,key:"
                + monPluginFileValue);
        return classLoader;
    }

    /**
     * 判断给定的文件路径是否指向一个有效的本地zip文件。
     * <p>
     * 该函数会检查文件是否存在、是否为目录、是否为常规文件，并且文件扩展名是否为".zip"。
     *
     * @param monPluginFileValue 文件路径字符串，表示需要检查的文件
     * @return 如果文件存在、是常规文件、不是目录，并且以".zip"结尾，则返回true；否则返回false
     */
    private static boolean isALocalZip(String monPluginFileValue) {
        String suffix = ".zip";
       /* try {
            LOG.info("sleeping 100s");
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }*/
        boolean b = monPluginFileValue.endsWith(suffix);
        if (!b) {
            LOG.info("The file is not a zip file:" + monPluginFileValue);
            return false;
        }
        if (monPluginFileValue.startsWith("hdfs://") || monPluginFileValue.startsWith("obs://")) {
            LOG.info("The file is not a local file:" + monPluginFileValue);
            return false;
        }
        String cleanPath = monPluginFileValue.replaceFirst("^file:///", "");
        java.nio.file.Path path = Paths.get(cleanPath);
        boolean exists = Files.exists(path);
        if (!exists) {
            LOG.info("isALocalZip file does not exists:" + monPluginFileValue);
            return false;
        }
        boolean directory = Files.isDirectory(path);
        if (directory) {
            LOG.info("The file is a directory, not a zip file:" + monPluginFileValue);
            return false;
        }
        boolean regularFile = Files.isRegularFile(path);
        if (!regularFile) {
            LOG.info("The file is not a regular file, not a zip file:" +
                    monPluginFileValue);
            return false;
        }
        LOG.info("The file is a zip file:" + monPluginFileValue);
        return true;
    }

    /**
     * 从指定的ZIP文件中加载JAR文件。
     * 该函数首先验证ZIP文件路径的有效性，然后根据路径类型（本地文件或Spark分布式文件）获取ZIP文件的实际路径。
     * 接着，创建一个临时目录用于解压ZIP文件，并返回解压后的文件列表。
     *
     * @param zipFileValue ZIP文件的路径或标识符，可以是本地文件路径或Spark分布式文件路径。
     * @return 解压后的JAR文件列表。
     * @throws URISyntaxException 如果ZIP文件路径的URI格式不正确。
     * @throws IOException        如果ZIP文件无法找到或读取。
     */
    private static List<File> loadJarFilesFormZip(String zipFileValue) throws URISyntaxException, IOException {
        // 1. 获取ZIP文件路径
        if (!isALocalZip(zipFileValue)) {
            throw new IllegalArgumentException("Invalid ZIP file name: " + zipFileValue);
        }
        String zipPath = extra(zipFileValue);


        LOG.info("meritdata mon ZIP file path: " + zipPath);
        // 2. 创建临时解压目录
        File tempDir = crateTempoDirToSparkRootDir();
        return unzipFiles(zipPath, tempDir);
    }

    private static String extra(String zipFileValue) throws URISyntaxException, FileNotFoundException {
        if (new File(zipFileValue).exists()) {
            return zipFileValue;
        }

        if (zipFileValue.startsWith("file:///")) {
            java.nio.file.Path pathRaw = Paths.get(zipFileValue.replace("file:///", "")).normalize();
            URI uri = new URI("file:///" + pathRaw.toString().replace("\\", "/"));
            LOG.info("Method 2 URI: " + uri);
            String path = uri.getPath();
            LOG.info("从本地加载驱动:" + zipFileValue);
            if (!new File(path).exists()) {
                throw new FileNotFoundException("meritdata mon local ZIP file not found: "
                        + path);
            }
            return path;
        }


        LOG.info("未能从本地加载到驱动，从spark dist中加载");
        //从zipFilePath中提取文件名称
        String zipFileName = zipFileValue.substring(zipFileValue.lastIndexOf("/") + 1);
        String zipPath = SparkFiles.get(zipFileName);
        if (zipPath == null) {
            throw new FileNotFoundException("meritdata mon hdfs ZIP file not found: " + zipPath);
        }
        return zipPath;
    }


    private static File crateTempoDirToSparkRootDir() throws IOException {
        String rootDirectory = SparkFiles.getRootDirectory();
        java.nio.file.Path path = Paths.get(rootDirectory);
        return Files.createTempDirectory(path, MERITDATA_MON_SPARK_DRIVERS).toFile();
    }

    private static List<File> loadJarFilesFromFolder(String folderPath) throws IOException, URISyntaxException {
        String jarFolderPath = dealFolder(folderPath);

        LOG.info("从文件夹加载驱动: " + jarFolderPath);

        // 2. 获取文件夹中的所有JAR文件
        File folder = new File(jarFolderPath);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".jar"));

        if (files == null || files.length == 0) {
            throw new FileNotFoundException("No JAR files found in folder: " + jarFolderPath);
        }
        return Arrays.asList(files);
    }

    private static String dealFolder(String folderPath) throws URISyntaxException, IOException {
        // 1. 获取文件夹路径
        if (folderPath.startsWith("file:///")) {
            if (!new File(folderPath).isDirectory()) {
                throw new IllegalArgumentException("Invalid folder path: " + folderPath);
            }
            return getJarFolderPathFroFilePrefix(folderPath);
        }

        if (folderPath.startsWith("hdfs://") || folderPath.startsWith("obs://")) {
            //从hdfs下载到本地
            LOG.info("hdfs 或者 obs协议，从文件夹加载驱动: " + folderPath);
            return downloadFromHdfsToLocal(folderPath);
        }

        if (new File(folderPath).exists() && new File(folderPath).isDirectory()) {
            LOG.info("是本地文件夹，从文件夹加载驱动: " + folderPath);
            return folderPath;
        }

        throw new IllegalArgumentException("不支持的文件协议: " + folderPath);
    }


    private static String downloadFromHdfsToLocal(String hdfsFolderPath) throws IOException {
        Configuration configuration = getHadoopConfigration();
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

    private static Configuration getHadoopConfigration()  {
        return new Configuration();
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
        // 提取数据库类型
        String dbType = extractDbTypeFromUrl(url);
        if (dbType == null) {
            LOG.warning("无法从URL中提取数据库类型: " + url);
            return "";
        }

        // 从DBDriverTypes中获取驱动类名
        DBDriverTypes driverType = DBDriverTypes.of(dbType.toLowerCase());
        if (driverType == null) {
            LOG.warning("未内置驱动类的数据库类型: " + dbType);
            return "";
        }

        return driverType.getDriver();
    }

    /**
     * 从JDBC URL中提取数据库类型。
     * <p>
     * 该方法假设JDBC URL的格式为 "jdbc:dbtype://..." 或 "jdbc:dbtype:..."，
     * 并从URL中提取出数据库类型（dbtype）。
     *
     * @param url JDBC连接URL，格式应为 "jdbc:dbtype://..." 或 "jdbc:dbtype:..."
     * @return 提取出的数据库类型，转换为小写形式。如果URL格式不符合预期，则返回null。
     */
    private static String extractDbTypeFromUrl(String url) {
        // 这里假设URL格式为 jdbc:dbtype://... 或 jdbc:dbtype:...
        if (url.startsWith("jdbc:")) {
            int start = 5; // 跳过 "jdbc:"
            int end = url.indexOf(":", start); // 查找第一个冒号
            if (end != -1) {
                return url.substring(start, end).toLowerCase();
            }
        }
        LOG.warning("URL格式不正确: " + url + "；应该为： \"jdbc:dbtype://...\" 或 \"jdbc:dbtype:...\" 。");
        return null;
    }


    // 解压ZIP文件到指定目录（基于网页3的解压逻辑优化）
    private static List<File> unzipFiles(String zipPath, File outputDir) throws IOException {
        String fileFilter = "*";
        return unzipFilesReturnByFilter(zipPath, outputDir, fileFilter);
    }


    private static List<File> unzipFilesReturnByFilter(String zipPath, File outputDir, String fileFilter) throws IOException {
        if (fileFilter == null || fileFilter.isEmpty()) {
            throw new IllegalArgumentException("fileFilter can not be null or empty");
        }
        LOG.info("meritdata 解压ZIP文件到指定目录 zipPath:" + zipPath + ",\n outputDir:" +
                outputDir + " , fileFilter: " + fileFilter);
        List<File> jarFiles = new ArrayList<>();
        try (ZipFile zipFile = new ZipFile(zipPath)) {
            zipFile.stream().forEach(entry -> {
                try {
                    File outputFile = new File(outputDir, entry.getName());

                    // 新增：显式处理目录条目
                    if (entry.isDirectory()) {
                        boolean mkdirs = outputFile.mkdirs();// 创建目录
                        if (!mkdirs) {
                            LOG.info("1.mkdirs fail: " + outputFile.getAbsolutePath());
                        }
                    } else {
                        // 保留原逻辑：确保父目录存在并写入文件
                        boolean mkdirs = outputFile.getParentFile().mkdirs();
                        if(!mkdirs){
                            LOG.info("2.mkdirs fail: " + outputFile.getParentFile().getAbsolutePath());
                        }
                        try (InputStream is = zipFile.getInputStream(entry);
                             OutputStream os = Files.newOutputStream(outputFile.toPath())) {
                            byte[] buffer = new byte[1024];
                            int len;
                            while ((len = is.read(buffer)) > 0) {
                                os.write(buffer, 0, len);
                            }
                        }

                        // 文件过滤逻辑保持不变
                        if (fileFilter.equals("*")) {
                            LOG.info("meritdata unzip file: " + outputFile.getAbsolutePath());
                            jarFiles.add(outputFile);
                        } else if (outputFile.getName().endsWith(fileFilter)) {
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

    // 创建自定义类加载器
    private static synchronized URLClassLoader createClassLoader(List<File> jarFiles)  {
        URL[] urls = jarFiles.stream().map(f -> {
            try {
                return f.toURI().toURL();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).toArray(URL[]::new);

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
    private static Driver initializeDriver(String url, ClassLoader classLoader, String driverClassName) throws Exception {

        Class<?> driverClass;
        if (driverClassName != null && !driverClassName.isEmpty()) {
            // 显式加载指定驱动类
            driverClass = classLoader.loadClass(driverClassName);
        } else {
            // 自动探测驱动
            ServiceLoader<Driver> loadedDrivers = ServiceLoader.load(Driver.class, classLoader);
            Iterator<Driver> driversIterator = loadedDrivers.iterator();
            while (driversIterator.hasNext()) {
                LOG.info("meritdata 加载驱动类 loading driver class：" + driversIterator.getClass().getName());
                Driver driver = driversIterator.next(); // 触发驱动类初始化
                boolean b = driver.acceptsURL(url);
                if (b) {
                    LOG.info("meritdata 加载驱动类 loading driver class：url：【" + url + "】 匹配成功，找到驱动类"
                            + driver.getClass().getName());
                    return driver;
                }
            }
            return DriverManager.getDriver(url);

        }
        // 实例化驱动类
        return (Driver) driverClass.getDeclaredConstructor().newInstance();
    }

    public static java.util.Enumeration<Driver> getDrivers(JDBCOptions jdbcOptions) throws Exception {
        URLClassLoader classLoader = getUrlClassLoader(jdbcOptions.parameters());
        ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return DriverManager.getDrivers();
        } finally {
            Thread.currentThread().setContextClassLoader(originalLoader);
        }

    }
}
