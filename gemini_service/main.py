import os
import json
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Importar el SDK moderno y sus errores
import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted, InternalServerError
# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GeminiService")

# --- Configuración de Gemini ---
API_KEY = os.getenv("GEMINI_API_KEY")
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

if not API_KEY:
    logger.error("La variable de entorno GEMINI_API_KEY no está configurada.")
    # No fallará al inicio, pero sí en la primera llamada
    gemini_model = None
else:
    try:
        genai.configure(api_key=API_KEY)
        gemini_model = genai.GenerativeModel(MODEL_NAME)
        logger.info(f"Cliente de Gemini inicializado para {MODEL_NAME}")
    except Exception as e:
        logger.error(f"Error al inicializar cliente Gemini: {e}")
        gemini_model = None

# --- Prompt (El que fuerza la salida JSON) ---
# (Adaptado de tu lógica de parseo anterior)
PLANTILLA_PROMPT = (
    "Responde la pregunta del usuario y calcula UN solo puntaje final de 1 a 10.\n"
    "Devuelve EXCLUSIVAMENTE un JSON válido en este FORMATO y ORDEN:\n"
    "[\n  final_score_entero_1_a_10,\n  \"<repite la pregunta EXACTAMENTE como la recibiste>\",\n  null,\n  \"<respuesta en texto>\"\n]\n\n"
    "Pregunta:\n"
)

# --- Modelos Pydantic ---
class GenerateRequest(BaseModel):
    question: str

# Este es el formato que la "api" espera recibir
class RowResponse(BaseModel):
    score: int
    title: str
    body: Optional[str] = None
    answer: str

# --- Lógica de Parseo (Manejada aquí) ---
def parsear_respuesta_llm(bruto: str, pregunta_original: str) -> RowResponse:
    parsed = None
    try:
        # Intento 1: Asumir que es JSON perfecto
        parsed = json.loads(bruto)
    except Exception:
        # Intento 2: Buscar el inicio [ y el fin ] (limpieza básica)
        inicio = bruto.find("[")
        fin = bruto.rfind("]")
        if inicio != -1 and fin != -1 and fin > inicio:
            try:
                parsed = json.loads(bruto[inicio:fin+1])
            except Exception:
                pass # Falla y va al error

    if not isinstance(parsed, list) or len(parsed) != 4:
        logger.error(f"Formato inesperado desde Gemini. Se esperaba JSON, se recibió: {bruto}")
        raise HTTPException(
            status_code=502, # Bad Gateway (El LLM nos dio basura)
            detail=f"Formato inesperado desde Gemini: {bruto}"
        )
    
    try:
        puntaje_final = int(round(float(parsed[0])))
    except Exception:
        puntaje_final = 1 # Default en caso de parseo fallido
    
    return RowResponse(
        score=max(1, min(10, puntaje_final)),
        title=pregunta_original, # Usamos la original, no la del LLM
        body=None,
        answer=str(parsed[3]).strip()
    )

# --- API ---
app = FastAPI(title="Gemini Service")

@app.post("/generate", response_model=RowResponse)
async def generate_response(solicitud: GenerateRequest):
    if not gemini_model:
        raise HTTPException(status_code=500, detail="Servicio Gemini no inicializado. Falta API_KEY.")

    prompt_completo = f"{PLANTILLA_PROMPT}{solicitud.question}"
    
    try:
        logger.info("Llamando a Google API...")
        respuesta_gemini = await gemini_model.generate_content_async(prompt_completo)
        texto_bruto = respuesta_gemini.text
    
    # --- Manejo de Errores (Inspirado en tu llm_consumer_producer.py) ---
    except ResourceExhausted as e: # <-- CAMBIO AQUÍ
        # 🔴 Error de CUOTA/Límite de Tarifa
        logger.error(f"Error de CUOTA/RATE-LIMIT de API: {e}")
        raise HTTPException(status_code=429, detail=f"Cuota de API Excedida: {e}")
        
    except InternalServerError as e:
        # 🟡 Error transitorio (sobrecarga)
        logger.error(f"Error TRANSITORIO de API (InternalServerError): {e}")
        raise HTTPException(status_code=503, detail=f"Servicio de Google no disponible (transitorio): {e}")  
    except Exception as e:
        # ⚫ Otro error
        logger.error(f"Error desconocido al llamar a Gemini: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno en Gemini Service: {e}")

    if not texto_bruto:
        raise HTTPException(status_code=502, detail="Respuesta vacía desde Gemini")

    # Parseamos la respuesta de Gemini para adaptarla al formato Row
    return parsear_respuesta_llm(texto_bruto, solicitud.question)

@app.get("/health")
def health():
    return {"ok": True}