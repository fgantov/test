from datetime import datetime
from collections import defaultdict
from typing import List, Tuple
from sys import intern
from stream_json import stream_json

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """Las top 10 fechas donde hay más tweets. Mencionar el usuario (username) que más publicaciones tiene por
    cada uno de esos días."""
    if file_path is None or file_path == "":
        print("Error: File path vacio.")
        return []
    # Un limite para que se detenga la lectura del archivo si hay demasiados errores
    # maxErrorCount se podria poner como variable opcional, o obtenerla de una variable de entorno
    errorCount = 0
    maxErrorCount = 100

    # Utilizaremos un dict para ir creando una key por cada dia, y para el almacenar el contador y sus usernames
    # Esta estructura permite un rapido acceso a los datos cuando se requiera cargar mas en la misma fecha
    # Lo mismo para el contador de username, otro dict.
    tweets_per_day = defaultdict(lambda: {"total": 0, "users": defaultdict(int)})
    
    for tweet in stream_json(file_path):
        try:
            # Utilizamos intern para reducir el uso de memoria (aunque es minima la mejora)
            # Incrementamos el contador de tweets por dia y por usuario
            date = intern(datetime.fromisoformat(tweet["date"]).date().isoformat())
            tweets_per_day[date]["total"] += 1
            
            username = intern(tweet["user"]["username"])
            tweets_per_day[date]["users"][username] += 1

        except Exception as e:
            if isinstance(e, KeyError):
                print(f"Error: Key no encontrado en tweet. Saltando linea. {e}")
            elif isinstance(e, ValueError):
                print(f"Error: Formato invalido en tweet. Saltando linea. {e}")
            elif isinstance(e, TypeError):
                print(f"Error: Tipo invalido en tweet. Saltando linea. {e}")
            else:
                print(f"Error: {e}")
            errorCount += 1
            if (errorCount >= maxErrorCount):
                print(f"Demasiados errores, abortando...")
                break
   
    # Ordenamos las fechas por el total de tweets en order desc y tomamos las 10 primeras
    top_dates = sorted(tweets_per_day.keys(), key=lambda x: tweets_per_day[x]["total"], reverse=True)[:10]
    # Para cada fecha, obtenemos el usuario con mas tweets, y formateamos los datos como se pide
    result = [(date, max(tweets_per_day[date]["users"], key=tweets_per_day[date]["users"].get)) for date in top_dates]
    return result