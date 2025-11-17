import os
import json
import logging
from typing import Optional, Literal

import psycopg2
import redis  # <-- Importar Redis
import httpx  # <-- Importar HTTPX para llamar al otro servicio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

# --- Configuración ---
load_dotenv() # Carga .env.api montado en /app/.env.api
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("API-Orchestrator")

# Conexiones
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin123")
DB_NAME = os.getenv("POSTGRES_DB", "mydatabase")
DB_CONN_STR = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

REDIS_HOST = os.getenv("REDIS_HOST", "redis_cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# URL del nuevo servicio
GEMINI_SERVICE_URL = os.getenv("GEMINI_SERVICE_URL", "http://gemini_svc:8001")

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "3600"))

# --- Modelos Pydantic ---
class Row(BaseModel):
    score: int
    title: str
    body: Optional[str] = None
    answer: str

class AskRequest(BaseModel):
    question: str

class AskResponse(BaseModel):
    source: Literal["cache", "db", "llm"]
    row: Row
    message: str

def fila_a_mensaje(fila: Row) -> str:
    return f"Pregunta: {fila.title}\nPuntaje: {fila.score}\nRespuesta: {fila.answer}"

# --- Conexión Redis ---
try:
    redis_cliente = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_cliente.ping()
    logger.info("Conectado a Redis con éxito.")
except Exception as e:
    logger.error(f"No se pudo conectar a Redis: {e}")
    redis_cliente = None

# --- Lógica de Caché (Redis) ---
def leer_desde_cache(pregunta: str) -> Optional[Row]:
    if not redis_cliente: return None
    try:
        key = f"qa:{pregunta.strip().lower()}"
        cached_data = redis_cliente.get(key)
        if cached_data:
            logger.info("Cache HIT")
            return Row(**json.loads(cached_data))
        logger.info("Cache MISS")
        return None
    except Exception as e:
        logger.warning(f"Error al leer de Redis: {e}")
        return None

def escribir_en_cache(pregunta: str, fila: Row):
    if not redis_cliente: return
    try:
        key = f"qa:{pregunta.strip().lower()}"
        redis_cliente.set(key, fila.model_dump_json(), ex=CACHE_TTL_SECONDS)
    except Exception as e:
        logger.warning(f"Error al escribir en Redis: {e}")

# --- Lógica de Base de Datos (Postgres) ---
def get_db_conn():
    try:
        return psycopg2.connect(DB_CONN_STR)
    except psycopg2.OperationalError as e:
        logger.error(f"Error de conexión a Postgres: {e}")
        raise HTTPException(status_code=503, detail="Error de conexión a la base de datos")

def leer_desde_db(pregunta: str) -> Optional[Row]:
    try:
        with get_db_conn() as conn, conn.cursor() as cur:
            # Tu schema usaba 'public.querys'
            cur.execute(
                "SELECT score, title, body, answer FROM public.querys WHERE UPPER(title) = UPPER(%s)", 
                (pregunta,)
            )
            fila = cur.fetchone()
            if fila:
                logger.info("DB HIT")
                return Row(score=fila[0], title=fila[1], body=fila[2], answer=fila[3])
            logger.info("DB MISS")
            return None
    except Exception as e:
        logger.error(f"Error al leer de DB: {e}")
        return None # Tratar como un 'miss' si la DB falla

def upsert_fila(fila: Row):
    # (Tu lógica de inserción/actualización de DB iría aquí)
    try:
        with get_db_conn() as conn, conn.cursor() as cur:
            # Ejemplo de UPSERT (ignora si el título ya existe, o actualiza)
            sql = """
            INSERT INTO public.querys (score, title, body, answer)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (title) DO UPDATE SET
            score = EXCLUDED.score,
            answer = EXCLUDED.answer,
            body = EXCLUDED.body;
            """
            cur.execute(sql, (fila.score, fila.title, fila.body, fila.answer))
            conn.commit()
    except Exception as e:
        logger.error(f"Error al escribir en DB: {e}")

# --- Lógica de LLM (Llamada al worker) ---
async def consultar_ia_servicio(pregunta: str) -> Row:
    """Llama al servicio de IA (gemini_svc) para obtener una respuesta."""
    url = f"{GEMINI_SERVICE_URL}/generate"
    
    async with httpx.AsyncClient(timeout=120) as cliente:
        try:
            respuesta = await cliente.post(url, json={"question": pregunta})
            
            # Si el servicio gemini_svc responde 429, 503, 502, etc., esto lanzará un error
            respuesta.raise_for_status() 
            
            # El servicio gemini_svc ya devuelve el JSON en el formato Row
            return Row(**respuesta.json())

        except httpx.HTTPStatusError as e:
            # Propagar el error que nos dio el gemini_svc (ej. 429 Cuota, 503 Google caído)
            logger.error(f"Error del servicio Gemini: {e.response.status_code} - {e.response.text}")
            raise HTTPException(status_code=e.response.status_code, detail=f"Error del servicio LLM: {e.response.text}")
        except httpx.RequestError as e:
            logger.error(f"No se pudo conectar a gemini_svc: {e}")
            raise HTTPException(status_code=503, detail=f"Servicio LLM no disponible: {e}")

# --- FastAPI ---
app = FastAPI(title="QA Orchestrator API")

@app.post("/ask", response_model=AskResponse)
async def ask(solicitud: AskRequest):
    pregunta = solicitud.question.strip()
    if not pregunta:
        raise HTTPException(status_code=400, detail="Pregunta vacía")

    # 1. Revisar Cache (Redis)
    fila = leer_desde_cache(pregunta)
    if fila:
        return AskResponse(source="cache", row=fila, message=fila_a_mensaje(fila))

    # 2. Revisar Base de Datos (Postgres)
    fila = leer_desde_db(pregunta)
    if fila:
        escribir_en_cache(pregunta, fila) # <- Actualizar caché si estaba en DB
        return AskResponse(source="db", row=fila, message=fila_a_mensaje(fila))
        
    # 3. Consultar al LLM (Worker)
    logger.info("Llamando al servicio LLM...")
    fila = await consultar_ia_servicio(pregunta)

    # 4. Guardar en DB y Caché
    upsert_fila(fila)
    escribir_en_cache(pregunta, fila)
    
    return AskResponse(source="llm", row=fila, message=fila_a_mensaje(fila))

@app.get("/health")
def health():
    # (Aquí deberías añadir un ping a Postgres y Redis)
    return {"ok": True}