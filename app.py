import os
import json
import random
import sys
import time
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import requests
from google import genai
from google.genai import types
from google.genai.errors import ServerError

prompt = (
    """Jesteś asystentem OCR i ekstrakcji danych. Odczytaj treść z przesłanego zdjęcia.

Zasady wejścia (ważne):
Tekst może być ciągły albo w tabeli.
Jeśli jest tabela: każdy wiersz = osobny rekord (jedna osoba).
Tekst może być po polsku albo po łacinie (jeśli łacina — przetłumacz na polski przed ekstrakcją).
Usuń/napraw oczywiste błędy OCR (literówki, urwane słowa, losowe znaki).
Nie dopisuj faktów — bazuj tylko na tym, co jest na obrazie.
Tłumacz imiona na język Polski.

Cel:
Wyodrębnij dla każdej osoby dane w języku polskim:
imie_nazwisko
wiek
miejsce urodzenia
data_zgonu
przyczyna_zgonu
inne_wazne_informacje (podaj wszystko co wiesz)

Braki danych:
Jeśli pola nie da się znaleźć w tekście, wpisz dokładnie: "brak informacji".

Format wyjścia (krytyczne):
Zwróć WYŁĄCZNIE poprawny JSON.
JSON musi być zgodny z poniższym schematem.
Każdy rekord ma mieć identyczny zestaw pól.
Bez komentarzy, bez markdown, bez dodatkowego tekstu.

Schemat JSON (dokładnie taki):
{
"rekordy": [
{
"imie_nazwisko": "",
"wiek": "",
"miejsce urodzenia": "",
"data_zgonu": "",
"przyczyna_zgonu": "",
"inne_wazne_informacje": ""
}
]
}

"""
)




class GeminiOCRProcessor:
    def __init__(self, api_keys=None):
        self.api_keys = api_keys or []
        self.current_key_index = 0
        self.key_usage = {}  # Śledź użycie każdego klucza
        self.key_errors = {}  # Śledź błędy dla każdego klucza
        self.rate_limit_reset = datetime.now()

        # Inicjalizuj śledzenie kluczy
        for key in self.api_keys:
            self.key_usage[key] = 0
            self.key_errors[key] = 0


        # Statystyki
        self.stats = {
            'total_requests': 0,
            'successful': 0,
            'failed_429': 0,
            'failed_503': 0,
            'keys_rotated': 0
        }

    def get_current_key(self):
        """Pobierz aktualny klucz API"""
        if not self.api_keys:
            raise ValueError("Brak dostępnych kluczy API")
        return self.api_keys[self.current_key_index]

    def mark_key_error(self, key, error_code):
        """Oznacz klucz jako mający błąd"""
        if key in self.key_errors:
            self.key_errors[key] += 1
            print(f"Klucz {self._key_name(key)} ma teraz {self.key_errors[key]} błędów")

            # Jeśli klucz ma więcej niż 3 błędy 429, tymczasowo go wyłącz
            if error_code == 429 and self.key_errors[key] >= 3:
                print(f"Klucz {self._key_name(key)} wyłączony (za dużo błędów 429)")

    def get_next_available_key(self):
        """Znajdź następny dostępny klucz API"""
        original_index = self.current_key_index

        for i in range(len(self.api_keys)):
            next_index = (self.current_key_index + i) % len(self.api_keys)
            key = self.api_keys[next_index]

            # Sprawdź czy klucz nie jest wyłączony
            if self.key_errors.get(key, 0) < 3:  # Mniej niż 3 błędy
                self.current_key_index = next_index

                if i > 0:  # Tylko jeśli zmieniliśmy klucz
                    self.stats['keys_rotated'] += 1
                    print(f"Rotacja klucza: {self._key_name(self.api_keys[original_index])} → {self._key_name(key)}")

                return key

        # Jeśli wszystkie klucze mają błędy, wyzeruj index
        self.current_key_index = 0
        key = self.api_keys[self.current_key_index]
        keys_recovery_delay =  300
        print(f"⚠Wszystkie klucze mają błędy, {keys_recovery_delay}s przerwy na odnowienie zasobów")
        keys_recovery_delay =  300
        time.sleep(keys_recovery_delay)
        return key

    def _key_name(self, key):
        """Zwróp przyjazną nazwę klucza (ostatnie 8 znaków)"""
        if not key:
            return "empty"
        return f"...{key[-8:]}" if len(key) > 8 else key

    def wait_for_rate_limit_reset(self):
        """Czekaj na reset rate limitów"""
        now = datetime.now()

        if now < self.rate_limit_reset:
            wait_seconds = (self.rate_limit_reset - now).total_seconds()
            if wait_seconds > 0:
                print(f"⏳ Rate limit reset za {wait_seconds:.0f} sekund...")
                time.sleep(wait_seconds + 1)  # Dodaj 1 sekundę marginesu

        # Zresetuj czas po oczekiwaniu
        self.rate_limit_reset = datetime.now() + timedelta(minutes=1)

    def process_image(self, image_path, max_retries=3):
        """Przetwarzanie pojedynczego obrazu z inteligentną rotacją kluczy"""

        with open(image_path, "rb") as f:
            image_bytes = f.read()

        attempt = 0
        while attempt < max_retries:
            try:
                # Pobierz dostępny klucz
                current_key = self.get_next_available_key()
                client = genai.Client(api_key=current_key)

                print(f"Używam klucza: {self._key_name(current_key)} (próba {attempt + 1}/{max_retries})")

                response = client.models.generate_content(
                    model="gemini-2.5-flash",  # Użyj flash dla oszczędności
                    contents=[
                        types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg"),
                        prompt,
                    ],
                    config={
                        "response_mime_type": "application/json",
                    },
                )

                # Zaktualizuj statystyki
                self.stats['total_requests'] += 1
                self.stats['successful'] += 1
                self.key_usage[current_key] = self.key_usage.get(current_key, 0) + 1

                # Resetuj błędy dla tego klucza po sukcesie
                self.key_errors[current_key] = 0

                response_text = response.text.strip()

                # Oczyszczanie odpowiedzi
                if response_text.startswith('```json'):
                    response_text = response_text[7:-3]
                elif response_text.startswith('```'):
                    response_text = response_text[3:-3]

                # Parsowanie JSON
                try:
                    parsed_data = json.loads(response_text)
                    return parsed_data
                except json.JSONDecodeError as e:
                    return {
                        "rekordy": [],
                        "error": f"Błąd parsowania JSON: {str(e)}",
                        "raw_response": response_text[:500]
                    }

            except ServerError as e:
                error_code = getattr(e, 'code', None)
                current_key = self.get_current_key()

                if error_code == 503:  # Server overloaded
                    self.stats['failed_503'] += 1
                    wait_time = (2 ** attempt) + random.random()
                    print(f"Serwer przeciążony (503), czekam {wait_time:.1f}s")
                    time.sleep(wait_time)
                    attempt += 1
                else:
                    attempt += 1
                    raise

            except Exception as e:
                error_code = getattr(e, 'code', None)
                current_key = self.get_current_key()

                if error_code == 429:  # Quota exhausted
                    print(f"Błąd: {str(e)[:100]}")
                    self.stats['failed_429'] += 1
                    self.mark_key_error(current_key, 429)
                    if attempt == max_retries - 1:
                        print(f"Quota wyczerpane (429) dla klucza {self._key_name(current_key)}")
                        attempt = 0
                    else:
                        attempt += 1

        # Wszystkie próby zawiodły
        return {
            "rekordy": [],
            "error": "Wyczerpano wszystkie próby",
            "status": "failed"
        }


def gather_api_keys():
    """Zbierz wszystkie dostępne klucze API z zmiennych środowiskowych"""
    api_keys = []

    # Sprawdź podstawowy klucz
    main_key = os.environ.get("GEMINI_API_KEY")
    if main_key:
        api_keys.append(main_key)

    # Sprawdź dodatkowe klucze (GEMINI_API_KEY_1, GEMINI_API_KEY_2, ...)
    for i in range(1, 11):  # Sprawdź do 10 dodatkowych kluczy
        key = os.environ.get(f"GEMINI_API_KEY_{i}")
        if key:
            api_keys.append(key)

    # Usuń duplikaty
    unique_keys = []
    for key in api_keys:
        if key and key not in unique_keys:
            unique_keys.append(key)

    return unique_keys


def check_api_key_quota(api_key):
    """Sprawdź stan quota dla klucza API"""
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            return "active"
        elif response.status_code == 429:
            return "quota_exceeded"
        else:
            return f"error_{response.status_code}"
    except:
        return "connection_error"


def process_all_images_with_key_rotation(api_keys, source_root="zgony", target_root="json_zgony"):
    """Przetwarzanie z automatyczną rotacją kluczy API"""

    source_path = Path(source_root)
    target_path = Path(target_root)

    if not source_path.exists():
        print(f"Folder '{source_root}' nie istnieje!")
        return

    if not api_keys:
        print("Brak kluczy API Gemini!")
        print("\nJak dodać klucze:")
        print("1. Otwórz Google AI Studio: https://aistudio.google.com/app/apikey")
        print("2. Stwórz nowy klucz API")
        print("3. Ustaw zmienne środowiskowe:")
        print("   export GEMINI_API_KEY='twój_klucz'")
        print("   export GEMINI_API_KEY_1='kolejny_klucz'  # opcjonalnie")
        return

    print(f"Znaleziono {len(api_keys)} kluczy API")

    # Sprawdź stan każdego klucza
    active_keys = []
    for key in api_keys:
        status = check_api_key_quota(key)
        if status == "active":
            active_keys.append(key)
            print(f"    Klucz ...{key[-8:]}: Aktywny")
        else:
            print(f"    Klucz ...{key[-8:]}: {status}")

    if not active_keys:
        print(" Brak aktywnych kluczy API!")
        print(" Rozwiązania:")
        print("1. Odbierz nowe quota w Google AI Studio")
        print("2. Dodaj więcej kluczy API")
        print("3. Spróbuj później (quota resetuje się codziennie)")
        return

    # Inicjalizacja procesora
    processor = GeminiOCRProcessor(active_keys)

    # Zbierz wszystkie obrazy
    all_images = []
    for root, dirs, files in os.walk(source_path):
        for file in files:
            if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                all_images.append(Path(root) / file)

    total_images = len(all_images)

    if total_images == 0:
        print("Nie znaleziono obrazów do przetworzenia")
        return

    print(f"\nObrazy do przetworzenia: {total_images}")
    print("=" * 50)

    processed = 0
    successful = 0
    errors = 0

    # Dynamiczny delay - zwiększaj gdy są błędy 429
    base_delay = 3  # sekundy

    for i, image_path in enumerate(all_images):
        remaining = total_images - i
        print(f"\nPostęp: {i + 1}/{total_images} (pozostało: {remaining})")
        print(f"Obraz: {image_path.name}")

        # Określ strukturę folderów
        relative_path = image_path.relative_to(source_path)
        if len(relative_path.parts) > 1:
            parafia_name = relative_path.parts[0]
        else:
            parafia_name = "brak_parafii"

        page_name = image_path.stem
        dest_dir = target_path / parafia_name / page_name
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest_json = dest_dir / "data.json"
        dest_image = dest_dir / "image.jpg"

        # Sprawdź czy już przetworzone
        if dest_json.exists():
            print(f"Już przetworzone - pomijam")
            try:
                shutil.move(str(image_path), str(dest_image))
            except:
                pass
            processed += 1
            successful += 1
            continue

        try:
            # Przetwarzanie z OCR
            start_time = time.time()
            data = processor.process_image(str(image_path), max_retries=3)
            processing_time = time.time() - start_time

            # Zapisz wyniki
            with open(dest_json, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            # Przenieś obraz
            shutil.move(str(image_path), str(dest_image))

            # Statystyki
            processed += 1

            if "rekordy" in data and data.get("status") != "failed":
                successful += 1
                records = len(data["rekordy"]) if isinstance(data["rekordy"], list) else 0
                print(f"Sukces ({processing_time:.1f}s)")
                print(f"Rekordów: {records}")
            else:
                errors += 1
                print(f"Częściowy sukces / błąd")
                if "error" in data:
                    print(f"{data['error'][:100]}")

            # Wyświetl statystyki kluczy
            print(f"Statystyki kluczy:")
            for key, usage in processor.key_usage.items():
                if usage > 0:
                    error_count = processor.key_errors.get(key, 0)
                    status = "✅" if error_count == 0 else f"⚠️({error_count})"
                    print(f"      {status} ...{key[-8:]}: {usage} użyć")

            # Dynamicznie dostosuj delay
            current_delay = base_delay
            if processor.stats['failed_429'] > 0:
                current_delay = min(30, base_delay * (processor.stats['failed_429'] + 1))

            if remaining > 0:
                print(f"Oczekiwanie {current_delay:.1f}s...")
                time.sleep(current_delay)

        except Exception as e:
            errors += 1
            print(f"Błąd przetwarzania: {str(e)[:100]}")

            # Zapisz błąd
            error_file = dest_dir / "error.txt"
            with open(error_file, "w", encoding="utf-8") as f:
                f.write(f"Błąd: {str(e)}\nŚcieżka: {image_path}")

            # Nie przenoś obrazu przy błędzie

    # Podsumowanie
    print("\n" + "=" * 50)
    print("🎉 PRZETWARZANIE ZAKOŃCZONE")
    print("=" * 50)
    print(f"   Podsumowanie:")
    print(f"   Obrazy: {total_images}")
    print(f"   Przetworzone: {processed}")
    print(f"   Sukcesy: {successful}")
    print(f"   Błędy: {errors}")
    print(f"   Pozostało w źródle: {total_images - processed}")

    # Statystyki API
    print(f"\nSTATYSTYKI KLUCZY API:")
    for key, usage in processor.key_usage.items():
        error_count = processor.key_errors.get(key, 0)
        print(f"   ...{key[-8:]}: {usage} użyć, {error_count} błędów")

    print(f"\nSTATYSTYKI API:")
    print(f"   Łączne requesty: {processor.stats['total_requests']}")
    print(f"   Sukcesy: {processor.stats['successful']}")
    print(f"   Błędy 429 (quota): {processor.stats['failed_429']}")
    print(f"   Błędy 503 (serwer): {processor.stats['failed_503']}")
    print(f"   Rotacje kluczy: {processor.stats['keys_rotated']}")

    # Zapisz pełne podsumowanie
    summary = {
        "data_przetwarzania": datetime.now().isoformat(),
        "obrazy_łącznie": total_images,
        "przetworzone": processed,
        "sukcesy": successful,
        "błędy": errors,
        "klucze_użyte": len(active_keys),
        "statystyki_api": processor.stats,
        "użycie_kluczy": {f"...{k[-8:]}": v for k, v in processor.key_usage.items()},
        "błędy_kluczy": {f"...{k[-8:]}": v for k, v in processor.key_errors.items()},
        "folder_źródłowy": str(source_path),
        "folder_docelowy": str(target_path)
    }

    summary_file = target_path / f"podsumowanie_{datetime.now():%Y%m%d_%H%M%S}.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n📝 Pełne podsumowanie zapisano: {summary_file}")





if __name__ == "__main__":
    print("Gemini OCR Processor z rotacją kluczy API")
    print("=" * 50)
    print("""Aby program działał poprawnie:
     1.Ustaw nazwy kluczy (zmienne środowiskowe) na 'GEMINI_API_KEY', 'GEMINI_API_KEY_i, ...,
       gdzie i jest liczbą calkowitą od 0.
     2. Stwórz katalog zgony/<nazwa_parafii> w katalogu głównym.""")

    # Sprawdź klucze
    api_keys = gather_api_keys()

    if not api_keys:
        print("Nie znaleziono kluczy API!")
        exit(1)

    print(f"Znaleziono {len(api_keys)} kluczy API")

    # Uruchom przetwarzanie
    try:
        process_all_images_with_key_rotation(
            source_root="zgony",
            target_root="json_zgony",
            api_keys=api_keys
        )
    except KeyboardInterrupt:
        print("\nPrzerwano przez klawisz użytkownika")
    except Exception as e:
        print(f"\nKrytyczny błąd: {e}")
        print(f"\nRozwiązanie problemu 429:")
        print("1. Dodaj więcej kluczy API")
        print("2. Zwiększ delay między requestami")
        print("3. Przetwarzaj mniej obrazów dziennie")
        print("4. Użyj płatnego planu w Google Cloud")