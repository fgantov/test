import emoji

# Los emojis en el JSON al parecer estan siempre codificados de forma Surrogate Pairs. En caso de que puedan estar
# codificados de otra forma, se podria mejorar esta funcion, extenderla para incluirlos.
# Por otro lado, si los emojis fueran mas limitados, se podria usar un regex relativamente simple y veloz en lugar
# de la libreria emoji. Pero debido a que los emojis estan en constante evolucion, es mas robusto usarla.
def extract_emojis(text: str) -> list[str]:
    """Extrae los emojis de un string codificados de forma Surrogate Pairs."""
    try:
        return [char for char in text.encode('utf-16', 'surrogatepass').decode('utf-16') if emoji.is_emoji(char)]
    except (UnicodeEncodeError, UnicodeDecodeError) as e:
        print(f"Error al extraer emojis encodificando o decodificando texto: {e}")
        return []  # Return an empty list or handle the error as needed
    except Exception as e:
        print(f"Error al extraer emojis: {e}")
        return []  # Return an empty list or handle the error as needed