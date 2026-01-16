import os
import json
from google import genai
from google.genai import types

with open("0076-0081.jpg", "rb") as f:
    image_bytes = f.read()

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])  # zamiast wpisywać klucz w kodzie

prompt = (
"""
Jesteś asystentem OCR i ekstrakcji danych. Odczytaj treść z przesłanego zdjęcia.

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

response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents=[
        types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg"),
        prompt,
    ],
    config={
        "response_mime_type": "application/json",
        # opcjonalnie: możesz dodać response_schema (Pydantic/Enum) dla większej kontroli
    },
)

# response.text powinien być JSON-em
data = json.loads(response.text)

with open("wynik.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Zapisano do wynik.json")
