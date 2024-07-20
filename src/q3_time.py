from collections import defaultdict
from typing import List, Tuple
from sys import intern
from stream_json import stream_json

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """El top 10 histórico de usuarios (username) más influyentes en función del conteo de las menciones (@) que
    registra cada uno de ellos."""
    if file_path is None or file_path == "":
        print("Error: File path vacio.")
        return []
    # Un limite para que se detenga la lectura del archivo si hay demasiados errores
    # maxErrorCount se podria poner como variable opcional, o obtenerla de una variable de entorno
    errorCount = 0
    maxErrorCount = 100

    # Utilizaremos un dict para ir creando una key por cada user, y para el almacenar el contador
    # Esta estructura permite un rapido acceso a los datos cuando se requiera cargar mas en el mismo user
    user_counts = defaultdict(int)

    for tweet in stream_json(file_path):
        try:
            mentionedUsers = tweet["mentionedUsers"]
            if mentionedUsers is not None:
                for user in mentionedUsers:
                    # Utilizamos intern para reducir el uso de memoria (aunque es minima la mejora)
                    # Incrementamos el contador de users
                    user_counts[intern(user["username"])] += 1

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
    
    # Ordenamos los users por cantidad y tomamos los 10 primeros, y retornamos en el formato pedido
    top_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    return top_users