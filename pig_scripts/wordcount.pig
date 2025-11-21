-- wordcount.pig
-- Análisis de Frecuencia de Palabras 

-- Configuración
REGISTER /usr/local/pig/contrib/piggybank/java/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVExcelStorage();
SET default_parallel 2;

-- Borrar salidas anteriores
rmf /output/wc_yahoo;
rmf /output/wc_llm;

--Yahoo:

-- Cargar
yahoo_raw = LOAD '/data/yahoo/yahoo_dataset.csv' USING CSVLoader() AS (score:chararray, title:chararray, body:chararray, answer:chararray);

-- Limpiar puntuación
yahoo_clean_text = FOREACH yahoo_raw GENERATE 
    LOWER(REPLACE(answer, '[,\\.;:!\\?¿¡"\\(\\)\\[\\]\\-]', ' ')) AS clean_answer;

-- Tokenizar
yahoo_tokens = FOREACH yahoo_clean_text GENERATE FLATTEN(TOKENIZE(clean_answer)) AS word;

-- Filtrar
yahoo_filtered = FILTER yahoo_tokens BY (SIZE(word) > 2) AND 
    NOT (word MATCHES '(que|los|las|del|por|para|con|una|unos|unas|sus|mis|tus|esa|ese|eso|esta|este|esto|nos|les|son|fue|era|muy|mas|pero|sin|sobre|tras|entre|desde|porque|cuando|como|donde|quien|hay|tiene)');

-- Agrupar y Contar
yahoo_grouped = GROUP yahoo_filtered BY word;
yahoo_counts = FOREACH yahoo_grouped GENERATE group AS word, COUNT(yahoo_filtered) AS frequency;

-- Ordenar y Guardar
yahoo_ordered = ORDER yahoo_counts BY frequency DESC;
STORE yahoo_ordered INTO '/output/wc_yahoo';


--LLM

-- Cargar
llm_raw = LOAD '/data/llm/llm_dataset.csv' USING CSVLoader() AS (score:chararray, title:chararray, body:chararray, answer:chararray);

-- Limpiar puntuación
llm_clean_text = FOREACH llm_raw GENERATE 
    LOWER(REPLACE(answer, '[,\\.;:!\\?¿¡"\\(\\)\\[\\]\\-]', ' ')) AS clean_answer;

-- Tokenizar
llm_tokens = FOREACH llm_clean_text GENERATE FLATTEN(TOKENIZE(clean_answer)) AS word;

-- Filtrar
llm_filtered = FILTER llm_tokens BY (SIZE(word) > 2) AND 
    NOT (word MATCHES '(que|los|las|del|por|para|con|una|unos|unas|sus|mis|tus|esa|ese|eso|esta|este|esto|nos|les|son|fue|era|muy|mas|pero|sin|sobre|tras|entre|desde|porque|cuando|como|donde|quien|hay|tiene)');

-- Agrupar y Contar
llm_grouped = GROUP llm_filtered BY word;
llm_counts = FOREACH llm_grouped GENERATE group AS word, COUNT(llm_filtered) AS frequency;

-- Ordenar y Guardar
llm_ordered = ORDER llm_counts BY frequency DESC;

STORE llm_ordered INTO '/output/wc_llm';
