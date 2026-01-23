import os
import psycopg
import re
from psycopg.rows import dict_row
from flask import Flask, render_template, request

app = Flask(__name__)

DSN = os.getenv("PG_DSN")
if not DSN:
    raise RuntimeError(
        "Brak zmiennej środowiskowej PG_DSN (np. postgresql://postgres:haslo@127.0.0.1:5432/db_zgony)"
    )


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
    return [r[0] for r in rows]


# podkreślanie wyszukiwanej frazy
def highlight_ci(text: str, query: str) -> str:
    if not text or not query:
        return text or ""

    q = query.strip()
    if len(q) < 2:
        return text

    pattern = re.compile(re.escape(q), re.IGNORECASE)
    return pattern.sub(lambda m: f'<span class="hl">{m.group(0)}</span>', text)


def to_int_or_none(s: str):
    try:
        return int(s) if s != "" else None
    except ValueError:
        return None


@app.get("/")
def home():
    parishes = get_parishes()
    return render_template(
        "index.html",
        query="",
        selected_parafia="",
        parishes=parishes,
        results=None,
        year_from="",
        year_to="",
        age_from="",
        age_to="",
        sort_by="",
        sort_dir="asc",
    )


@app.post("/search")
def search():
    query = (request.form.get("query") or "").strip()
    selected_parafia = (request.form.get("parafia") or "").strip()

    year_from = (request.form.get("year_from") or "").strip()
    year_to = (request.form.get("year_to") or "").strip()
    age_from = (request.form.get("age_from") or "").strip()
    age_to = (request.form.get("age_to") or "").strip()
    sort_by  = (request.form.get("sort_by") or "").strip()
    sort_dir = (request.form.get("sort_dir") or "asc").strip().lower()
    if sort_dir not in ("asc", "desc"):
        sort_dir = "asc"

    parishes = get_parishes()

    y_from = to_int_or_none(year_from)
    y_to = to_int_or_none(year_to)
    a_from = to_int_or_none(age_from)
    a_to = to_int_or_none(age_to)

    # jeśli wpisano tylko jedno pole -> traktuj jako pojedynczą wartość
    if y_from is not None and y_to is None:
        y_to = y_from
    if y_to is not None and y_from is None:
        y_from = y_to

    if a_from is not None and a_to is None:
        a_to = a_from
    if a_to is not None and a_from is None:
        a_from = a_to

    # normalizacja zakresów (gdy user wpisze "od" większe niż "do")
    if y_from is not None and y_to is not None and y_from > y_to:
        y_from, y_to = y_to, y_from

    if a_from is not None and a_to is not None and a_from > a_to:
        a_from, a_to = a_to, a_from

    # jeśli nic nie podano – zwróć pusty stan
    if (
        not query
        and not selected_parafia
        and y_from is None
        and y_to is None
        and a_from is None
        and a_to is None
    ):
        return render_template(
            "index.html",
            query="",
            selected_parafia="",
            parishes=parishes,
            results=None,
            year_from="",
            year_to="",
            age_from="",
            age_to="",
            sort_by=sort_by,
            sort_dir=sort_dir,
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

            # FILTR: ROK ZGONU (wyciągamy rok z tekstu data_zgonu)
            if y_from is not None and y_to is not None:
                sql += """
                    AND CAST(
                        NULLIF(
                            substring(COALESCE(data_zgonu, '') from '((?:18|19|20)[0-9]{2})'),
                            ''
                        ) AS int
                    ) BETWEEN %s AND %s
                """
                params.append(y_from)
                params.append(y_to)

            # FILTR: WIEK (0 oznacza <1 rok)
            if a_from is not None and a_to is not None:
                includes_under1 = (a_from <= 0 <= a_to)

                # <1 rok: ma mies/tyg/dni, ale NIE ma lat/rok
                under1_sql = """
                    (
                        wiek IS NOT NULL
                        AND wiek <> ''
                        AND wiek ~* '(miesi|miesiąc|tygod|dni)'
                        AND wiek !~* '(lat|rok)'
                    )
                """

                # wiek w latach: brak mies/tyg/dni, wyciągamy pierwszą liczbę
                years_sql = """
                    (
                        wiek IS NOT NULL
                        AND wiek <> ''
                        AND wiek !~* '(miesi|miesiąc|tygod|dni)'
                        AND CAST(
                            NULLIF(substring(wiek from '[0-9]+'), '')
                            AS int
                        ) BETWEEN %s AND %s
                    )
                """

                if includes_under1:
                    # 0–0 => tylko <1 rok
                    if a_to >= 1:
                        sql += f" AND ( {under1_sql} OR {years_sql} )"
                        params.append(1)
                        params.append(a_to)
                    else:
                        sql += f" AND {under1_sql}"
                else:
                    sql += f" AND {years_sql}"
                    params.append(a_from)
                    params.append(a_to)


            order_expr = "id"  # default

            if sort_by == "name":
                order_expr = "imie_nazwisko"
            elif sort_by == "year":
                order_expr = """
                    CAST(
                        NULLIF(substring(data_zgonu from '(18|19|20)[0-9]{2}'), '')
                        AS int
                    )
                """
            elif sort_by == "age":
                # tylko tam gdzie wiek w latach (mies/tyg/dni mają NULL w sortowaniu)
                order_expr = """
                    CASE
                    WHEN wiek IS NULL OR wiek = '' THEN NULL
                    WHEN wiek ~* '(miesi|tygod|dni)' THEN NULL
                    ELSE CAST(NULLIF(substring(wiek from '[0-9]+'), '') AS int)
                    END
                """

            # NULLS LAST żeby braki danych lądowały na końcu
            sql += f" ORDER BY {order_expr} {sort_dir.upper()} NULLS LAST, id ASC"

            cur.execute(sql, params)
            results = cur.fetchall()

            # highlight w wynikach
            if query:
                for r in results:
                    r["imie_nazwisko_hl"] = highlight_ci(r.get("imie_nazwisko", ""), query)
            else:
                for r in results:
                    r["imie_nazwisko_hl"] = r.get("imie_nazwisko", "")

    return render_template(
        "index.html",
        query=query,
        selected_parafia=selected_parafia,
        parishes=parishes,
        results=results,
        year_from=year_from,
        year_to=year_to,
        age_from=age_from,
        age_to=age_to,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


if __name__ == "__main__":
    app.run(debug=True)