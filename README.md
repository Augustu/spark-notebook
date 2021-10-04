# Spark notebook

### Software version

```bash
hadoop-3.2.2
kafka_2.13-3.0.0
spark-3.1.2-bin-hadoop3.2
python-3.9.7
java-11.0.1.2
```



#### Environment variables

```bash
HADOOP_HOME=[HADOOP_PATH]
JAVA_HOME=[JAVA_PATH]

# append path, for windows
path=%HADOOP_HOME%\bin

# for jupter notebook
PYSPARK_DRIVER_PYTHON=jupyter # when use spark-submit, remove this
PYSPARK_PYTHON=python
```



#### Python environment

```bash
pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

pip install pyspark

# install scala jupyter kernel
pip install spylon-kernel
python -m spylon_kernel install

# show if kernel installed
jupyter kernelspec list
```



#### Issues may occur

1. Hadoop start-dfs.sh => Error:JAVA_HOME is not set and could not be found

   hadoop-env.sh

   export JAVA_HOME=$JAVA_HOME

   export JAVA_HOME=[real_java_path]

2. 