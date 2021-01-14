hdfs dfs -cat /user/dlange/working_set_day/rolling_popularity/30/*.csv | gzip > rolling_30.csv.gz
hdfs dfs -cat /user/dlange/working_set_day/rolling_popularity/90/*.csv | gzip > rolling_90.csv.gz
hdfs dfs -cat /user/dlange/working_set_day/rolling_popularity/180/*.csv | gzip > rolling_180.csv.gz
hdfs dfs -cat /user/dlange/working_set_day/popularity_dump2/*.csv | gzip > popularity.csv.gz
hdfs dfs -cat /user/dlange/working_set_day/exceptions_daily/*.csv | gzip > exceptions_daily.csv.gz
