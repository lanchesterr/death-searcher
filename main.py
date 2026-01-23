import os
import psycopg
from psycopg.rows import dict_row
from flask import Flask, render_template, request

app = Flask(__name__)

DSN = os.getenv("PG_DSN")
if not DSN:
    raise RuntimeError("Brak zmiennej środowiskowej PG_DSN2 (np. postgresql://postgres:haslo@127.0.0.1:5432/db_zgony)")


def get_parishes():
    """Zwraca listę dostępnych parafii (unikalne wartości z tabeli)."""
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT parafia
                FROM zgony
                WHERE parafia IS NOT NULL
                  AND parafia <> ''
                ORDER BY parafia
                """
            )
            rows = cur.fetchall()
    # rows to lista [(parafia1,), (parafia2,), ...]
    return [r[0] for r in rows]


@app.get("/")
def home():
    parishes = get_parishes()
    return render_template(
        "index.html",
        query="",
        selected_parafia="",
        parishes=parishes,
        results=None,
    )


@app.post("/search")
def search():
    query = (request.form.get("query") or "").strip()
    selected_parafia = (request.form.get("parafia") or "").strip()

    parishes = get_parishes()
    results = []

    # jeśli nic nie podano – zwróć pusty wynik
    if not query and not selected_parafia:
        return render_template(
            "index.html",
            query="",
            selected_parafia="",
            parishes=parishes,
            results=[],
        )

    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            sql = """
                SELECT
                    id,
                    imie_nazwisko,
                    wiek,
                    miejsce_urodzenia,
                    parafia,
                    data_zgonu,
                    przyczyna_zgonu,
                    inne_wazne_informacje,
                    source_file,
                    image_url
                FROM zgony
                WHERE 1=1
            """
            params = []

            if query:
                sql += " AND imie_nazwisko ILIKE %s"
                params.append(f"%{query}%")

            if selected_parafia:
                sql += " AND parafia = %s"
                params.append(selected_parafia)

            sql += " ORDER BY id"

            cur.execute(sql, params)
            results = cur.fetchall()

    return render_template(
        "index.html",
        query=query,
        selected_parafia=selected_parafia,
        parishes=parishes,
        results=results,
    )


if __name__ == "__main__":
    app.run(debug=True)
