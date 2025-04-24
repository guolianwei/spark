#!/bin/bash
# 环境变量配置（修正SPARK_HOME赋值方式）
export HADOOP_USER_NAME=hdfs
export HADOOP_CLASSPATH=$(hadoop classpath)
export HADOOP_CONF_DIR=/opt/merit_cloud/file/cloud_mon/DEFAULT_CONFIG
export JAVA_HOME=/root/jdk-21/
export SPARK_HOME=/opt/spark-3.3.0-bin-hadoop3
export SPARK_CONF_DIR=$SPARK_HOME/conf

# 路径定义（移除hdfs://前缀）
local_pluingshome=/opt/merit_cloud/mon-plugins/third_env_plugin/mysql-8.0.29


echo $local_pluingshome
hdfs_userhome=/user/tempodata/
echo $hdfs_userhome
hdfs_spark_libpath=hdfs://${hdfs_userhome}/spark330/lib/jars
echo $hdfs_spark_libpath
hdfs_pluginsfilepath=${hdfs_userhome}mon_plugins/mysql-8.0/
echo $hdfs_pluginsfilepath
# 本地所有配置文件的压缩包
hdfs_plugin_zip_path=hdfs:///user/tempodata/mon_plugins/mysql-8.0/mysql-8.0.29.zip
echo $hdfs_plugin_zip_path

examples_jar_name=spark-examples_2.12-3.3.5-SNAPSHOT-shaded.jar
local_examples_jar_path=${SPARK_HOME}/examples/jars/${examples_jar_name}
echo $local_examples_jar_path
main_class_for_exec="org.apache.spark.examples.mdsql.JavaMDParallelJdbcSQLDataSourceExample"

# 目录清理与重建（添加递归删除参数）
hadoop fs -rm -r -f ${hdfs_spark_libpath}  # 指出需用-r参数删除目录
hadoop fs -rm -r -f ${hdfs_pluginsfilepath}
hadoop fs -mkdir -p ${hdfs_spark_libpath}
hadoop fs -mkdir -p ${hdfs_pluginsfilepath}

# 文件上传（修正路径格式）
hadoop fs -put ${SPARK_HOME}/jars/* ${hdfs_spark_libpath}
echo "从 ${SPARK_HOME}/jars/* 到 ${hdfs_spark_libpath}"
hadoop fs -put ${local_pluingshome}/* ${hdfs_pluginsfilepath}/
echo "从 ${local_pluingshome}/* 到 ${hdfs_pluginsfilepath}/"
hadoop fs -ls ${hdfs_spark_libpath}
# 任务提交优化（调整资源参数）
CMD="${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--conf spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8 \
--conf spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8 \
--conf spark.hadoop.hive.metastore.client.charset=UTF-8 \
--conf spark.yarn.jars=${hdfs_spark_libpath}/*.jar \
--conf spark.yarn.dist.files=${hdfs_plugin_zip_path} \
--num-executors 1 \
--executor-memory 1G \
--executor-cores 1 \
--driver-memory 1G \
--queue default \
--class ${main_class_for_exec} \
${local_examples_jar_path} \
nn1"

echo $CMD
$CMD