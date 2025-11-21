# Tarea3Distribuidos

#Construir y levantar los contenedores
docker compose up --build -d

#Levantar el exporter para que se ejecute
docker-compose up --build data_exporter

#Ejecuta el script contador de palabras
docker-compose exec pig pig -f /pig_scripts/wordcount.pig

#mostrar las 50 palabras más repetidas del dataset de yahoo
docker-compose exec hadoop-namenode hdfs dfs -cat /output/wc_yahoo/part-r-00000 | Select -First 50

#mostrar las 50 palabras más repetidas del dataset del llm
docker-compose exec hadoop-namenode hdfs dfs -cat /output/wc_llm/part-r-00000 | Select -First 50
