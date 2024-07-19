from google.cloud import bigquery
from datetime import datetime
import time

def q1_time(s, start_time):
    print('0', time.time() - start_time)
    # Initialize a BigQuery client
    service_account_path = 'C:\\Users\\federico.gantov\\Documents\\Data Arch\\New folder\\try\\src\\sonic-silo-429813-s7-208fd1231147.json'
    client = bigquery.Client.from_service_account_json(service_account_path)

    # Define your BigQuery dataset and table
    dataset_id = 'tweets_fgantov'
    table_id = 'tweets_table_full'  # Update this with your desired table ID
    full_table_id = f"{dataset_id}.{table_id}"

    print('A', time.time() - start_time)
    # Query to find top 10 dates with the most tweets
    sql = f"""
    WITH TopDates AS (
    SELECT LEFT(date, 10) AS Date, COUNT(*) AS TweetCount
    FROM `sonic-silo-429813-s7.tweets_fgantov.tweets_table_full`
    GROUP BY LEFT(date, 10)
    ORDER BY COUNT(*) DESC
    LIMIT 10
    ),
    RankedTopDates AS (
    SELECT Date, ROW_NUMBER() OVER (ORDER BY TweetCount DESC) AS Rank
    FROM TopDates
    ),
    TweetCountsPerDate AS (
    SELECT
        LEFT(t.date, 10) AS Date,
        t.user.username,
        COUNT(*) AS TweetCount
    FROM `sonic-silo-429813-s7.tweets_fgantov.tweets_table_full` t
    INNER JOIN RankedTopDates rtd ON LEFT(t.date, 10) = rtd.Date
    GROUP BY LEFT(t.date, 10), t.user.username
    ),
    MaxTweetCountsPerDate AS (
    SELECT
        Date,
        MAX(TweetCount) AS MaxTweetCount
    FROM TweetCountsPerDate
    GROUP BY Date
    ),
    TopUserPerDate AS (
    SELECT
        tcpd.Date,
        tcpd.username,
        tcpd.TweetCount
    FROM TweetCountsPerDate tcpd
    INNER JOIN MaxTweetCountsPerDate mtcpd ON tcpd.Date = mtcpd.Date AND tcpd.TweetCount = mtcpd.MaxTweetCount
    )
    SELECT
    rtd.date,
    tup.username
    FROM RankedTopDates rtd
    INNER JOIN TopUserPerDate tup ON rtd.Date = tup.Date
    ORDER BY rtd.Rank;
    """
    print('B', time.time() - start_time)
    response = client.query(sql)
    rows = response.result()
    print('C', time.time() - start_time)
    result = []
    for row in rows:
        result.append((datetime.strptime(row.date, '%Y-%m-%d').date(), row.username))
    print('D', time.time() - start_time)
    return result