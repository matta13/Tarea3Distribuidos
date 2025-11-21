# Tarea3Distribuidos

# Cargar Yahoo
docker-compose exec postgres psql -U admin -d mydatabase -c "\COPY public.yahoo_dataset FROM '/tmp/yahoo.csv' DELIMITER ',' CSV HEADER;"
# Cargar LLM
docker-compose exec postgres psql -U admin -d mydatabase -c "\COPY public.llm_dataset FROM '/tmp/llm.csv' DELIMITER ',' CSV HEADER;"

docker-compose exec pig pig -f /pig_scripts/wordcount.pig
docker-compose exec hadoop-namenode hdfs dfs -cat /output/wc_yahoo/part-r-00000 | Select -First 50
docker-compose exec hadoop-namenode hdfs dfs -cat /output/wc_llm/part-r-00000 | Select -First 50
