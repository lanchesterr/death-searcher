import json
import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import errors

# Plik JSON z danymi (lista rekordów)
JSON_PATH = Path("wynik.json")  # plik w tym samym folderze co skrypt


def mask_dsn(dsn: str) -> str:
    """
    Maskuje hasło w DSN typu:
    postgresql://user:pass@host:port/db -> postgresql://user:***@host:port/db
    """
    try:
        parts = urlsplit(dsn)
        if "@" not in parts.netloc or ":" not in parts.netloc.split("@", 1)[0]:
            # brak user:pass@host – zwróć jak jest
            return dsn
        user_pass, host_part = parts.netloc.split("@", 1)
        user, _ = user_pass.split(":", 1)
        masked_netloc = f"{user}:***@{host_part}"
        masked = urlunsplit(
            (parts.scheme, masked_netloc, parts.path, parts.query, parts.fragment)
        )
        return masked
    except Exception:
        return dsn


def load_json_rows(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Nie znaleziono pliku JSON: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("JSON musi być listą rekordów (tablicą obiektów).")

    return data


def build_row_tuple(r: dict, source_file: str):
    """
    Buduje krotkę danych do INSERT zgodnie z aktualnym schematem tabeli 'zgony'.

    Oczekiwane kolumny:
    imie_nazwisko, wiek, miejsce_urodzenia, parafia,
    data_zgonu, przyczyna_zgonu, inne_wazne_informacje, source_file

    Obsługuje zarówno "nowe" klucze JSON, jak i "stare":
      - miejsce_urodzenia  <- r["miejsce_urodzenia"] lub r["data_miejsce_urodzenia"]
      - data_zgonu         <- r["data_zgonu"] lub r["data_przyczyna_zgonu"]
      - przyczyna_zgonu    <- r["przyczyna_zgonu"] lub r["data_przyczyna_zgonu"]
      - inne_wazne_inf.    <- r["inne_wazne_informacje"] lub r["dodatkowe_informacje"]
    """
    # imię i nazwisko jest wymagane
    imie_nazwisko = (r.get("imie_nazwisko") or "").strip()
    if not imie_nazwisko:
        return None  # sygnał, że rekord ma być pominięty

    wiek = r.get("wiek")

    # próba wzięcia „nowego” pola, a jeśli go nie ma – starego
    miejsce_urodzenia = (
        r.get("miejsce_urodzenia")
        or r.get("data_miejsce_urodzenia")
        or None
    )

    parafia = r.get("parafia")

    data_zgonu = (
        r.get("data_zgonu")
        or r.get("data_przyczyna_zgonu")
        or None
    )

    przyczyna_zgonu = (
        r.get("przyczyna_zgonu")
        or r.get("data_przyczyna_zgonu")
        or None
    )

    inne_wazne_informacje = (
        r.get("inne_wazne_informacje")
        or r.get("dodatkowe_informacje")
        or None
    )

    return (
        imie_nazwisko,
        wiek,
        miejsce_urodzenia,
        parafia,
        data_zgonu,
        przyczyna_zgonu,
        inne_wazne_informacje,
        source_file,
    )


def main():
    # DSN – najpierw PG_DSN, jak nie ma to PG_DSN2
    dsn = os.getenv("PG_DSN2")
    if not dsn:
        raise RuntimeError(
            "Brak zmiennej środowiskowej PG_DSN (ani PG_DSN2).\n"
            "Ustaw np.:\n"
            "  set PG_DSN=postgresql://postgres:HASLO@127.0.0.1:5432/db_zgony\n"
            "lub odpowiednio dla Twojej bazy."
        )

    rows = load_json_rows(JSON_PATH)

    data = []
    skipped = 0

    for r in rows:
        if not isinstance(r, dict):
            raise ValueError("Każdy element listy JSON musi być obiektem (dict).")

        row_tuple = build_row_tuple(r, JSON_PATH.name)
        if row_tuple is None:
            skipped += 1
            print("Pomijam rekord bez imie_nazwisko:", r)
            continue

        data.append(row_tuple)

    if not data:
        print("Brak rekordów do importu (po odfiltrowaniu).")
        if skipped:
            print(f"Pominięto {skipped} rekordów bez imie_nazwisko.")
        return

    insert_sql = """
        INSERT INTO zgony (
            imie_nazwisko,
            wiek,
            miejsce_urodzenia,
            parafia,
            data_zgonu,
            przyczyna_zgonu,
            inne_wazne_informacje,
            source_file
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, data)
            conn.commit()
    except Exception as e:
        # Ładniejszy komunikat z zamaskowanym DSN
        raise RuntimeError(
            "Nie udało się wykonać importu. Najczęściej to:\n"
            "- terminal/VS Code nie widzi nowego PG_DSN (otwórz ponownie),\n"
            "- zła baza w DSN,\n"
            "- brak uprawnień do tabeli,\n"
            "- hasło ma znaki specjalne i DSN jest niepoprawny,\n"
            "- lub dane nie pasują do schematu tabeli.\n"
            f"PG_DSN (zamaskowane): {mask_dsn(dsn)}\n"
            f"Szczegóły: {type(e).__name__}: {e}"
        ) from e

    print(f"Zaimportowano {len(data)} rekordów do tabeli zgony.")
    if skipped:
        print(f"Pominięto {skipped} rekordów bez imie_nazwisko.")


if __name__ == "__main__":
    main()
