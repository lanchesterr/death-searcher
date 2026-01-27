import os
import csv
import io
import re

import psycopg
from psycopg.rows import dict_row
from flask import Flask, render_template, request, make_response

app = Flask(__name__)

# ---------- Konfiguracja DB ----------

DSN = os.getenv("PG_DSN")
if not DSN:
    raise RuntimeError(
        "Brak zmiennej środowiskowej PG_DSN "
        "(np. postgresql://postgres:haslo@127.0.0.1:5432/db_zgony)"
    )

# ---------- Współrzędne parafii ----------

PARISH_COORDS = {
    "Parafia Rzymskokatolicka w Sokołowie Małopolskim": (50.22737, 22.11804),
    "Parafia rzymskokatolickiej Albigowa": (50.01461, 22.22941),
    "Parafia rzymskokatolickiej Matki Boskiej Zwycięskiej w Łodzi": (51.75811, 19.44508),
    "Parafia rzymskokatolicka św. Jakuba w Warszawie (Ochota)":(52.21999, 20.98717),
    "Parafia rzymskokatolicka w Mogile": (50.06468, 20.05334)
    # kolejne parafie dopiszesz w miarę potrzeb
}

# ---------- Funkcje pomocnicze ----------


def get_parishes():
    """Zwraca listę parafii z liczbą aktów i współrzędnymi (dla mapy)."""
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    parafia,
                    COUNT(*) AS count
                FROM zgony
                WHERE parafia IS NOT NULL
                  AND TRIM(parafia) <> ''
                GROUP BY parafia
                ORDER BY parafia
                """
            )
            rows = cur.fetchall()

    stats = []
    for r in rows:
        name = r["parafia"]
        coords = PARISH_COORDS.get(name)
        if not coords:
            # pomijamy parafie, dla których nie mamy współrzędnych
            continue
        lat, lng = coords
        stats.append(
            {
                "parafia": name,
                "count": r["count"],
                "lat": lat,
                "lng": lng,
            }
        )
    return stats


def get_parish_names():
    """Lista samych nazw parafii (dla selecta w wyszukiwarce)."""
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


def get_causes():
    """
    Zwraca listę unikalnych, ustandaryzowanych przyczyn zgonu
    (dla selecta w wyszukiwarce).
    """
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT przyczyna_zgonu
            FROM zgony
            WHERE przyczyna_zgonu IS NOT NULL
              AND TRIM(przyczyna_zgonu) <> ''
            ORDER BY 1
            """
        ).fetchall()
    return [r["przyczyna_zgonu"] for r in rows]


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


def normalize_range(v_from, v_to):
    """Ujednolicenie zakresów: pojedyncza wartość, zamiana, gdy from>to."""
    if v_from is not None and v_to is None:
        v_to = v_from
    if v_to is not None and v_from is None:
        v_from = v_to
    if v_from is not None and v_to is not None and v_from > v_to:
        v_from, v_to = v_to, v_from
    return v_from, v_to


def build_filters_sql(query, selected_parafia, cause, y_from, y_to, a_from, a_to):
    """
    Buduje fragment WHERE i listę parametrów.
    cause jest teraz dokładnym wyborem z listy (SELECT),
    więc filtrujemy po równości (=), a nie po fragmencie.
    """
    sql = " WHERE 1=1"
    params = []

    if query:
        sql += " AND imie_nazwisko ILIKE %s"
        params.append(f"%{query}%")

    if selected_parafia:
        sql += " AND parafia = %s"
        params.append(selected_parafia)

    # FILTR: CHOROBA / PRZYCZYNA ZGONU (dokładne dopasowanie)
    if cause:
        sql += " AND przyczyna_zgonu = %s"
        params.append(cause)

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
            # zakres obejmuje 0 lat
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

    return sql, params


# ---------- Routy ----------


@app.get("/")
def home():
    # Do wyszukiwarki potrzebne są nazwy parafii i lista przyczyn
    parish_names = get_parish_names()
    causes = get_causes()
    return render_template(
        "index.html",
        query="",
        selected_parafia="",
        parishes=parish_names,
        results=None,
        year_from="",
        year_to="",
        age_from="",
        age_to="",
        sort_by="",
        sort_dir="asc",
        cause="",
        causes=causes,
    )


@app.route("/search", methods=["GET", "POST"])
def search():
    if request.method == "POST":
        data = request.form
    else:
        data = request.args

    query = (data.get("query") or "").strip()
    selected_parafia = (data.get("parafia") or "").strip()
    cause = (data.get("cause") or "").strip()
    year_from = (data.get("year_from") or "").strip()
    year_to = (data.get("year_to") or "").strip()
    age_from = (data.get("age_from") or "").strip()
    age_to = (data.get("age_to") or "").strip()
    sort_by = (data.get("sort_by") or "").strip()
    sort_dir = (data.get("sort_dir") or "asc").strip().lower()
    if sort_dir not in ("asc", "desc"):
        sort_dir = "asc"

    parish_names = get_parish_names()
    causes = get_causes()

    y_from = to_int_or_none(year_from)
    y_to = to_int_or_none(year_to)
    a_from = to_int_or_none(age_from)
    a_to = to_int_or_none(age_to)

    y_from, y_to = normalize_range(y_from, y_to)
    a_from, a_to = normalize_range(a_from, a_to)

    # jeśli nic nie podano – zwróć pusty stan
    # (UWAGA: samo ustawienie cause traktujemy jako wyszukiwanie)
    if (
        not query
        and not selected_parafia
        and not cause
        and y_from is None
        and y_to is None
        and a_from is None
        and a_to is None
    ):
        return render_template(
            "index.html",
            query="",
            selected_parafia="",
            parishes=parish_names,
            results=None,
            year_from="",
            year_to="",
            age_from="",
            age_to="",
            sort_by=sort_by,
            sort_dir=sort_dir,
            cause="",
            causes=causes,
        )

    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            base_select = """
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
            """

            where_sql, params = build_filters_sql(
                query, selected_parafia, cause, y_from, y_to, a_from, a_to
            )

            # sortowanie
            order_expr = "id"  # domyślnie

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
                order_expr = """
                    CASE
                    WHEN wiek IS NULL OR wiek = '' THEN NULL
                    WHEN wiek ~* '(miesi|tygod|dni)' THEN NULL
                    ELSE CAST(NULLIF(substring(wiek from '[0-9]+'), '') AS int)
                    END
                """

            sql = (
                base_select
                + where_sql
                + f" ORDER BY {order_expr} {sort_dir.upper()} NULLS LAST, id ASC"
            )

            cur.execute(sql, params)
            results = cur.fetchall()

            # highlight w wynikach
            if query:
                for r in results:
                    r["imie_nazwisko_hl"] = highlight_ci(
                        r.get("imie_nazwisko", ""), query
                    )
            else:
                for r in results:
                    r["imie_nazwisko_hl"] = r.get("imie_nazwisko", "")

    return render_template(
        "index.html",
        query=query,
        selected_parafia=selected_parafia,
        parishes=parish_names,
        results=results,
        year_from=year_from,
        year_to=year_to,
        age_from=age_from,
        age_to=age_to,
        sort_by=sort_by,
        sort_dir=sort_dir,
        cause=cause,
        causes=causes,
    )


@app.post("/export")
def export():
    """
    Eksport wyników do CSV na podstawie tych samych filtrów co w /search
    (query, parafia, choroba, rok, wiek).
    """
    query = (request.form.get("query") or "").strip()
    selected_parafia = (request.form.get("parafia") or "").strip()
    cause = (request.form.get("cause") or "").strip()
    year_from = (request.form.get("year_from") or "").strip()
    year_to = (request.form.get("year_to") or "").strip()
    age_from = (request.form.get("age_from") or "").strip()
    age_to = (request.form.get("age_to") or "").strip()

    y_from = to_int_or_none(year_from)
    y_to = to_int_or_none(year_to)
    a_from = to_int_or_none(age_from)
    a_to = to_int_or_none(age_to)

    y_from, y_to = normalize_range(y_from, y_to)
    a_from, a_to = normalize_range(a_from, a_to)

    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            base_select = """
                SELECT
                    imie_nazwisko,
                    wiek,
                    miejsce_urodzenia,
                    data_zgonu,
                    przyczyna_zgonu,
                    inne_wazne_informacje,
                    source_file,
                    image_url,
                    parafia
                FROM zgony
            """

            where_sql, params = build_filters_sql(
                query, selected_parafia, cause, y_from, y_to, a_from, a_to
            )
            sql = base_select + where_sql + " ORDER BY id ASC"

            cur.execute(sql, params)
            rows = cur.fetchall()

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    writer.writerow(
        [
            "imie_nazwisko",
            "wiek",
            "miejsce_urodzenia",
            "data_zgonu",
            "przyczyna_zgonu",
            "inne_wazne_informacje",
            "source_file",
            "image_url",
            "parafia",
        ]
    )

    for r in rows:
        writer.writerow(
            [
                r["imie_nazwisko"],
                r["wiek"],
                r["miejsce_urodzenia"],
                r["data_zgonu"],
                r["przyczyna_zgonu"],
                r["inne_wazne_informacje"],
                r["source_file"],
                r["image_url"],
                r["parafia"],
            ]
        )

    csv_data = output.getvalue()
    bom = "\ufeff"
    response = make_response(bom + csv_data)
    response.headers["Content-Disposition"] = "attachment; filename=zgony.csv"
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    return response


@app.get("/mapa")
def mapa():
    """
    Widok mapy – korzysta ze statystyk parafii (liczba + współrzędne),
    które potem wykorzystuje map.html.
    """
    parishes_stats = get_parishes()
    return render_template("map.html", parishes=parishes_stats)


@app.get("/statystyki")
def statystyki():
    """
    Prosta strona ze statystykami:
    - liczba zgonów w czasie (rocznie),
    - TOP 5 przyczyn zgonu,
    - rozkład wieku (w latach).
    """
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # 1) Zgony wg roku (zakładamy ustandaryzowany format DD.MM.RRRR)
            cur.execute(
                """
                SELECT
                    CAST(SUBSTRING(data_zgonu FROM 7 FOR 4) AS int) AS rok,
                    COUNT(*) AS liczba
                FROM zgony
                WHERE data_zgonu ~ '^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}$'
                GROUP BY rok
                ORDER BY rok
                """
            )
            by_year = cur.fetchall()

            # 2) TOP 5 przyczyn zgonu
            cur.execute(
                """
                SELECT
                    przyczyna_zgonu,
                    COUNT(*) AS liczba
                FROM zgony
                WHERE przyczyna_zgonu IS NOT NULL
                  AND TRIM(przyczyna_zgonu) <> ''
                GROUP BY przyczyna_zgonu
                ORDER BY liczba DESC
                LIMIT 5
                """
            )
            top_causes = cur.fetchall()

            # 3) Rozkład wieku (tylko lata, bez miesięcy / dni)
            cur.execute(
                """
                SELECT
                    CAST(
                        NULLIF(substring(wiek from '[0-9]+'), '')
                        AS int
                    ) AS lata,
                    COUNT(*) AS liczba
                FROM zgony
                WHERE wiek IS NOT NULL
                  AND wiek <> ''
                  AND wiek !~* '(miesi|miesiąc|tygod|dni)'  -- pomijamy opisy typu "miesięcy"
                GROUP BY lata
                ORDER BY lata
                """
            )
            age_hist = cur.fetchall()

    return render_template(
        "statystyki.html",
        by_year=by_year,
        top_causes=top_causes,
        age_hist=age_hist,
    )


if __name__ == "__main__":
    app.run(debug=True)

