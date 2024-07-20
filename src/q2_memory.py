from typing import List, Tuple
from big_query import query_from_big_query
from gcp_credentials import project_id, dataset_id, table_id

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """Los top 10 emojis más usados con su respectivo conteo."""
    
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Consulta SQL para encontrar los top 10 emojis más usados con su respectivo conteo
    # El CTE emoji_counts desglosa el Array emojis en una fila por cada emoji y da su cantidad
    # Por ultimo buscamos en la tabla anterior los 10 emojis mas usados
    sql = f"""
    WITH emoji_counts AS (
    SELECT emoji, COUNT(1) as count
    FROM `{full_table_id}`,
    UNNEST(emojis) as emoji
    GROUP BY emoji
    )
    SELECT emoji, count
    FROM emoji_counts
    ORDER BY count DESC
    LIMIT 10
    """

    # Realizamos la consulta a BigQuery
    rows = query_from_big_query(sql, file_path)

    result = []
    for row in rows:
        result.append((row.emoji, row.count))

    return result