from typing import List, Tuple
from big_query import query_from_big_query
from gcp_credentials import project_id, dataset_id, table_id

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """El top 10 histórico de usuarios (username) más influyentes en función del conteo de las menciones (@) que
    registra cada uno de ellos."""
    
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Consulta SQL para encontrar los top 10 users más mencionados con su respectivo conteo
    # El CTE user_counts desglosa el Array mentionedUsers en una fila por cada user y da su cantidad
    # Por ultimo buscamos en la tabla anterior los 10 users más mencionados
    sql = f"""
    WITH user_counts AS (
    SELECT user, COUNT(1) as count
    FROM `{full_table_id}`,
    UNNEST(mentionedUsers) as user
    GROUP BY user
    )
    SELECT user, count
    FROM user_counts
    ORDER BY count DESC
    LIMIT 10
    """
    
    # Realizamos la consulta a BigQuery
    rows = query_from_big_query(sql, file_path)

    result = []
    for row in rows:
        result.append((row.user, row.count))

    return result