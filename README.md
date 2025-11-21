# Tarea3Distribuidos

docker-compose up --build data_exporter
docker-compose exec pig pig -f /pig_scripts/wordcount.pig
docker-compose exec hadoop-namenode hdfs dfs -cat /output/wc_yahoo/part-r-00000 | Select -First 50
docker-compose exec hadoop-namenode hdfs dfs -cat /output/wc_llm/part-r-00000 | Select -First 50
