Useful links

Install Spark on Ubuntu:  
https://medium.com/beeranddiapers/installing-apache-spark-on-ubuntu-8796bfdd0861

How to set LD_LIBRARY_PATH in Ubuntu
https://stackoverflow.com/questions/13428910/how-to-set-the-environmental-variable-ld-library-path-in-linux

Install Hadoop on ubuntu   
https://linuxconfig.org/how-to-install-hadoop-on-ubuntu-18-04-bionic-beaver-linux
https://stackoverflow.com/questions/26540507/what-is-the-maximum-containers-in-a-single-node-cluster-hadoop - configure yarn

Install Hive on ubuntu + Configure Hive to use Spark
https://data-flair.training/blogs/apache-hive-installation/
https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started

Hive Getting Started:  
https://cwiki.apache.org/confluence/display/Hive/GettingStarted
http://hortonworks.com/wp-content/uploads/2016/05/Hortonworks.CheatSheet.SQLtoHive.pdf
https://www.youtube.com/watch?v=rr17cbPGWGA - Hive Tutorial
https://www.youtube.com/watch?v=oaEOtQwdicg - Hive Partitioning and Bucketing


Horton Hadoop Sandbox   
https://www.cloudera.com/downloads/hortonworks-sandbox/hdp.html   
https://www.cloudera.com/tutorials/learning-the-ropes-of-the-hdp-sandbox.html   
https://www.cloudera.com/tutorials/getting-started-with-hdp-sandbox.html   


Submitting application to Spark
https://spark.apache.org/docs/latest/submitting-applications.html
https://linuxize.com/post/linux-nohup-command/

Spark properties
http://spark.apache.org/docs/latest/configuration.html#application-properties

Debug Spark application   
export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=7777

Spark CSV features   
https://github.com/databricks/spark-csv#features

Spark History Server and monitoring jobs performance
https://luminousmen.com/post/spark-history-server-and-monitoring-jobs-performance   
https://stackoverflow.com/questions/39123314/how-to-add-custom-description-to-spark-job-for-displaying-in-spark-web-ui

Parquet format presentation
https://www.youtube.com/watch?v=_0Wpwj_gvzg

Spark Hive datasource
https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html

Spark books
https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-bucketing.html
https://medium.com/@ntnmathur/cluster-by-and-clustered-by-in-spark-sql-9af7f8b80978 - CLUSTER BY and CLUSTERED BY

Git - how to squach commits on remote brach
https://stackoverflow.com/questions/5667884/how-to-squash-commits-in-git-after-they-have-been-pushed
```git checkout my_branch
   git reset --soft HEAD~4
   git commit
   git push --force origin my_branch```

todo
https://medium.com/teads-engineering/lessons-learned-while-optimizing-spark-aggregation-jobs-f93107f7867f
