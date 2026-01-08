import os
import psycopg
from psycopg.rows import dict_row
from flask import Flask, render_template, request

app = Flask(__name__)

DSN = os.getenv("PG_DSN")
if not DSN:
    raise RuntimeError("Brak zmiennej środowiskowej PG_DSN (np. postgresql://postgres:admin@127.0.0.1:5432/db_zgony)")


@app.get("/")
def home():
    return render_template("index.html", query="", results=None)


@app.post("/search")
def search():
    query = (request.form.get("imie_nazwisko") or "").strip()
    results = []

    if query:
        with psycopg.connect(DSN) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        imie_nazwisko,
                        data_miejsce_urodzenia,
                        data_przyczyna_zgonu,
                        dodatkowe_informacje,
                        source_file
                    FROM zgony
                    WHERE imie_nazwisko ILIKE %s
                    ORDER BY id
                    """,
                    (f"%{query}%",),
                )
                results = cur.fetchall()

    return render_template("index.html", query=query, results=results)


if __name__ == "__main__":
    app.run(debug=True)
