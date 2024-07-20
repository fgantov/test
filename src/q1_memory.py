from datetime import datetime
from typing import List, Tuple
from big_query import query_from_big_query
from gcp_credentials import project_id, dataset_id, table_id

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """Las top 10 fechas donde hay más tweets. Mencionar el usuario (username) que más publicaciones tiene por
    cada uno de esos días."""

    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Consulta SQL para encontrar las top 10 fechas con más tweets y su usuario con más tweets
    # El CTE TopDates encuentra las top 10 fechas con más tweets. Este es el unico que analiza la tabla completa
    # El CTE RankedTopDates asigna un ranking a cada una de esas fechas
    # El CTE TweetCountsPerDate cuenta los tweets por usuario en cada top 10 fecha. No es la tabla completa, se filtra
    # y se toma solo una parte del total (los con fecha en el top 10)
    # El CTE MaxTweetCountsPerDate encuentra el maximo de tweets por fecha
    # El CTE TopUserPerDate encuentra el usuario con mas tweets por fecha mediante el MaxTweetCountsPerDate
    # Por ultimo unimos las tablas anteriores para obtener el resultado final
    sql = f"""
    WITH TopDates AS (
    SELECT date, COUNT(*) AS TweetCount
    FROM `{full_table_id}`
    GROUP BY date
    ORDER BY COUNT(*) DESC
    LIMIT 10
    ),
    RankedTopDates AS (
    SELECT date, ROW_NUMBER() OVER (ORDER BY TweetCount DESC) AS Rank
    FROM TopDates
    ),
    TweetCountsPerDate AS (
    SELECT
        t.date,
        t.username,
        COUNT(*) AS TweetCount
    FROM `{full_table_id}` t
    INNER JOIN RankedTopDates rtd ON t.date = rtd.date
    GROUP BY date, t.username
    ),
    MaxTweetCountsPerDate AS (
    SELECT
        date,
        MAX(TweetCount) AS MaxTweetCount
    FROM TweetCountsPerDate
    GROUP BY date
    ),
    TopUserPerDate AS (
    SELECT
        tcpd.date,
        tcpd.username,
        tcpd.TweetCount
    FROM TweetCountsPerDate tcpd
    INNER JOIN MaxTweetCountsPerDate mtcpd ON tcpd.date = mtcpd.date AND tcpd.TweetCount = mtcpd.MaxTweetCount
    )
    SELECT
    rtd.date,
    tup.username
    FROM RankedTopDates rtd
    INNER JOIN TopUserPerDate tup ON rtd.date = tup.date
    ORDER BY rtd.Rank;
    """

    # Realizamos la consulta a BigQuery
    rows = query_from_big_query(sql, file_path)

    result = []
    for row in rows:
        result.append((row.date, row.username))

    return result