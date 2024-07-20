import orjson

# Leer los datos linea por linea es la forma mas eficiente ya que el JSON es de tipo Newline-delimited
# Orjson es la libreria mas veloz
def stream_json(file_path: str):
    """Generador leer el archivo JSON y yield una linea a la vez."""
    # Un limite para que se detenga la lectura del archivo si hay demasiados errores
    # maxErrorCount se podria poner como variable opcional, o obtenerla de una variable de entorno
    errorCount = 0
    maxErrorCount = 100

    try:
        with open(file_path, 'rb') as f:
            for line in f:
                try:
                    yield orjson.loads(line)
                except orjson.JSONDecodeError as e:
                    print(f"Error al decodificar JSON. Saltando linea. {e}")
                    errorCount += 1
                    if (errorCount >= maxErrorCount):
                        print("Demasiados errores, deteniendo la lectura del archivo")
                        break
    except FileNotFoundError:
        print(f"Archivo no encontrado: {file_path}")
        return []