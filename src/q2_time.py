from collections import defaultdict
from typing import List, Tuple
from extract_emojis import extract_emojis
from stream_json import stream_json

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """Los top 10 emojis más usados con su respectivo conteo."""
    if file_path is None or file_path == "":
        print("Error: File path vacio.")
        return []
    # Un limite para que se detenga la lectura del archivo si hay demasiados errores
    # maxErrorCount se podria poner como variable opcional, o obtenerla de una variable de entorno
    errorCount = 0
    maxErrorCount = 5

    # Utilizaremos un dict para ir creando una key por cada emoji, y para el almacenar el contador
    # Esta estructura permite un rapido acceso a los datos cuando se requiera cargar mas en el mismo emoji
    emoji_counts = defaultdict(int)

    for tweet in stream_json(file_path):
        try:
            # Utilizar intern para content no es util ya que seran muy diferentes
            content = tweet["content"]
            # Incrementamos el contador de emojis
            for em in extract_emojis(content):
                emoji_counts[em] += 1

        except Exception as e:
            if isinstance(e, KeyError):
                print(f"{errorCount} Error: Key no encontrado en tweet. Saltando linea. {e}")
            elif isinstance(e, ValueError):
                print(f"{errorCount} Error: Formato invalido en tweet. Saltando linea. {e}")
            elif isinstance(e, TypeError):
                print(f"{errorCount} Error: Tipo invalido en tweet. Saltando linea. {e}")
            else:
                print(f"{errorCount} Error: {e}")
            errorCount += 1
            if (errorCount >= maxErrorCount):
                print(f"{errorCount} Demasiados errores, abortando...")
                break
    
    # Ordenamos los emojis por cantidad y tomamos los 10 primeros, y retornamos en el formato pedido
    top_emojis = sorted(emoji_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    return top_emojis