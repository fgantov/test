import orjson
import os
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from datetime import datetime
from typing import Generator, Any, Optional
from tempfile import NamedTemporaryFile
from google.cloud.exceptions import NotFound
from gcp_credentials import service_account_path, project_id, dataset_id, table_id
from extract_emojis import extract_emojis

# Unicamente subiremos la parte del JSON que nos interesa para los 3 ejercicios, formateandolos de la forma mas
# conveniente. Esto minimiza el tiempo y ancho de banda gastados en la subida, y el costo de almacenamiento en
# BigQuery. Ademas, los formatos permiten consultarlos mas eficientemente.
def stream_json(file_path: str) -> Generator[dict[str, Any], Any, None]:
    """Generador para preprocesar el archivo JSON y yield una linea a la vez."""
    with open(file_path, 'rb') as f:
        for line in f:
            # Leer los datos linea por linea es la forma mas eficiente ya que el JSON es de tipo Newline-delimited
            # Orjson es la libreria mas veloz
            try:
                obj: dict = orjson.loads(line)
            except orjson.JSONDecodeError as e:
                print(f"Error al decodificar JSON. Saltando linea. {e}")
                continue 
            # Los campos que nos interesan para los 3 ejercicios. Fecha nos interesa sin timestamp, y del content
            # solo nos interesan los emojis. En lugar de obtener los mentioned users analizando la string content
            # buscando regex con @, lo cual es ineficiente, aprovechamos que ya estan dados en mentionedUsers.
            # Los campos default en los .get() evitan errores en caso de que no esten presentes en el JSON.
            date_str: str = obj.get("date", "")
            mentioned_users = obj.get("mentionedUsers") or []
            content: str = obj.get("content", "")
            emojis = []
            for em in extract_emojis(content):
                emojis.append(em)
            try:
                date_obj = datetime.fromisoformat(date_str.rstrip("Z"))
                formatted_date = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                formatted_date = ""
            processed_obj = {
                "date": formatted_date,
                "emojis": emojis,
                "username": obj.get("user", {}).get("username", ""),
                "mentionedUsers": [mentioned_user.get("username", "") for mentioned_user in mentioned_users]
            }
            yield processed_obj

def create_bigquery_client() -> Optional[bigquery.Client]:
    """Attempts to create a BigQuery client using the specified service account JSON file. Returns None if fails."""
    try:
        if not os.path.exists(service_account_path):
            raise FileNotFoundError(f"Archivo de autenticacion no encontrado en {service_account_path}")
        client = bigquery.Client.from_service_account_json(service_account_path)
        return client
    except FileNotFoundError as e:
        print(f"Error al conectarse a BigQuery: {e}")
    except DefaultCredentialsError:
        print("Error al conectarse a BigQuery: El archivo de autenticacion no es valido.")
    return None

def upload_to_big_query(file_path: str) -> None:
    """Preprocesa y sube el archivo JSON a nuestra tabla en GCP BigQuery."""
    # Intentamos conectarnos a Big Query
    client = create_bigquery_client()
    if client is None:
        return

    # No ponemos una llamada a bigQuery para verificar si el dataset existe, ya que eso ralentizaria el proceso
    # En caso de que no exista, directamente arrojaremos un error en client.load_table_from_file()
    # No ponemos una llamada a bigQuery para verificar si la tabla ya existe, ya que eso ralentizaria el proceso
    # En caso de ya exista, el metodo que esta llamando a upload_to_big_query() no lo deberia llamar. Se puede agregar
    # el chequeo de existencia de tabla si no se confia en el metodo que llama a upload_to_big_query().
    full_table_id = f"{dataset_id}.{table_id}"
    table_ref = client.dataset(dataset_id).table(table_id)

    # Configuramos la schema y Job de Big Query
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("emojis", "STRING", mode="REPEATED"),
        bigquery.SchemaField("mentionedUsers", "STRING", mode="REPEATED")
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True  # Esto permite ignorar campos no definidos en el schema
    )

    # Hacemos el pre procesamiento del JSON y lo escribimos en un archivo temporal
    with NamedTemporaryFile(mode='w+b') as temp_file:
        try:
            for json_obj in stream_json(file_path):
                temp_file.write(orjson.dumps(json_obj) + b'\n')
            
            temp_file.seek(0)  # Volver al principio del archivo temporal

            job = client.load_table_from_file(temp_file, table_ref, job_config=job_config)
        except Exception as e:
            print(f"Error al cargar el archivo a BigQuery: {e}")
            return

    # Iniciamos el job y esperamos que complete
    job.result()

    print(f"Cargadas {job.output_rows} filas a {full_table_id}.")

def query_from_big_query(sql: str, file_path: str):
    """Realiza una consulta SQL a una tabla en GCP BigQuery. Si no existe, la crea y carga por primera vez."""
    if sql is None or sql == "":
        print("Error: Consulta SQL vacia.")
        return []
    # Intentamos conectarnos a Big Query
    client = create_bigquery_client()
    if client is None:
        return []

    # No enviamos una consulta para verificar si la tabla ya existe, ya que eso ralentizaria el proceso
    # En caso de que no exista, la intentaremos crear y cargar por primera vez
    try:
        response = client.query(sql)
        rows = response.result()
    except NotFound as e:
        print(f"Tabla no encontrada. Creandola y cargandola por primera vez...")
        if file_path is None or file_path == "":
            print("Error: File path vacio.")
            return []
        upload_to_big_query(file_path)
        try:
            response = client.query(sql)
            rows = response.result()
        except NotFound:
            print("Error: Tabla aun no encontrada tras intentar crear y cargarla.")
            return []
    except Exception as e:
        print(f"Error: {e}")
        return []
    
    return rows