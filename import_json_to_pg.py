import json
import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import errors

JSON_PATH = Path("wynik.json")  # plik w tym samym folderze co skrypt


def mask_dsn(dsn: str) -> str:
    """
    Maskuje hasło w DSN typu postgresql://user:pass@host:port/db
    """
    try:
        parts = urlsplit(dsn)
        if parts.username:
            user = parts.username
            host = parts.hostname or ""
            port = f":{parts.port}" if parts.port else ""
            path = parts.path or ""
            # hasło maskujemy zawsze
            netloc = f"{user}:***@{host}{port}"
            return urlunsplit((parts.scheme, netloc, path, parts.query, parts.fragment))
    except Exception:
        pass
    return "<dsn: nie udało się zmaskować>"


def main() -> None:
    dsn = os.getenv("PG_DSN")

    if not dsn:
        raise RuntimeError(
            "Brak zmiennej PG_DSN.\n"
            "Jeśli dopiero dodałeś ją w Zmiennych środowiskowych Windows, zamknij i otwórz ponownie terminal/VS Code.\n"
            "Przykład wartości PG_DSN:\n"
            "postgresql://postgres:admin@127.0.0.1:5432/db_zgony"
        )

    # Informacyjnie (bez hasła)
    print("Używam PG_DSN =", mask_dsn(dsn))

    # Sprawdź plik JSON
    if not JSON_PATH.exists():
        raise FileNotFoundError(
            f"Nie znaleziono pliku: {JSON_PATH.resolve()}\n"
            "Upewnij się, że wynik.json jest w tym samym folderze co skrypt albo zmień JSON_PATH."
        )

    # Wczytaj JSON
    with JSON_PATH.open("r", encoding="utf-8") as f:
        rows = json.load(f)

    if not isinstance(rows, list):
        raise ValueError("JSON musi być listą rekordów (tablicą obiektów).")

    # Przygotuj dane do executemany
    data = []
    for r in rows:
        if not isinstance(r, dict):
            raise ValueError("Każdy element listy JSON musi być obiektem (dict).")
        data.append(
            (
                r.get("imie_nazwisko"),
                r.get("data_miejsce_urodzenia"),
                r.get("data_przyczyna_zgonu"),
                r.get("dodatkowe_informacje"),
                JSON_PATH.name,
            )
        )

    insert_sql = """
        INSERT INTO zgony (
            imie_nazwisko,
            data_miejsce_urodzenia,
            data_przyczyna_zgonu,
            dodatkowe_informacje,
            source_file
        )
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                # Szybki test: czy tabela istnieje i masz do niej dostęp
                cur.execute("SELECT 1 FROM zgony LIMIT 1;")

                # Import hurtowy
                cur.executemany(insert_sql, data)

            conn.commit()

    except errors.InvalidPassword as e:
        raise RuntimeError(
            "Błędne hasło/login w PG_DSN albo łączysz się do innej instancji PostgreSQL niż w pgAdmin.\n"
            f"PG_DSN (zamaskowane): {mask_dsn(dsn)}"
        ) from e

    except errors.InvalidCatalogName as e:
        raise RuntimeError(
            "Baza danych z DSN nie istnieje (np. db_zgony).\n"
            "Sprawdź nazwę bazy w pgAdmin i w PG_DSN."
        ) from e

    except errors.UndefinedTable as e:
        raise RuntimeError(
            "Tabela 'zgony' nie istnieje w bazie, do której się łączysz.\n"
            "Utwórz tabelę lub połącz się do właściwej bazy."
        ) from e

    except Exception as e:
        raise RuntimeError(
            "Nie udało się wykonać importu. Najczęściej to:\n"
            "- terminal/VS Code nie widzi nowego PG_DSN (otwórz ponownie),\n"
            "- zła baza w DSN,\n"
            "- brak uprawnień do tabeli,\n"
            "- hasło ma znaki specjalne i DSN jest niepoprawny.\n"
            f"PG_DSN (zamaskowane): {mask_dsn(dsn)}\n"
            f"Szczegóły: {type(e).__name__}: {e}"
        ) from e

    print(f"Zaimportowano {len(data)} rekordów do tabeli zgony.")


if __name__ == "__main__":
    main()
