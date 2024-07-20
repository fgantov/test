import sqlite3
import orjson
from datetime import datetime
import os

db_path = 'tweets.db'

def create_db(db_path: str):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tweets (date TEXT, username TEXT, content TEXT)''')
    # Crear indices para acelerar las consultas
    c.execute('''CREATE INDEX IF NOT EXISTS idx_date ON tweets (date)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_username ON tweets (username)''')
    conn.commit()
    conn.close()

def insert_tweets(file_path: str, db_path: str):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Checkear si la tabla esta vacia
    c.execute("SELECT COUNT(*) FROM tweets")
    count = c.fetchone()[0]
    if count == 0:
        with open(file_path, 'rb') as f:
            for line in f:
                tweet = orjson.loads(line)
                date = datetime.fromisoformat(tweet["date"]).date()
                username = tweet["user"]["username"]
                content = tweet["content"]
                c.execute("INSERT INTO tweets (date, username, content) VALUES (?, ?, ?)", (date, username, content))
        conn.commit()
    else:
        print("Database already populated. Skipping insertion.")
    conn.close()

def q1_time(file_path: str):
    # Checkear si la base de datos existe y tiene datos
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        create_db(db_path)
        insert_tweets(file_path, db_path)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Consulta SQL para encontrar las top 10 fechas con más tweets
    c.execute('''SELECT date, COUNT(*) as cnt FROM tweets GROUP BY date ORDER BY cnt DESC LIMIT 10''')
    top_dates = c.fetchall()
    
    result = []
    for date, _ in top_dates:
        # Para cada fecha, obtenemos el usuario con mas tweets
        c.execute('''SELECT username, COUNT(*) as cnt FROM tweets WHERE date=? GROUP BY username ORDER BY cnt DESC LIMIT 1''', (date,))
        top_user = c.fetchone()
        result.append((date, top_user[0]))
    
    conn.close()
    return result