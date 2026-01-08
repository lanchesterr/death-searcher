import os
import json
from google import genai
from google.genai import types

with open("0076-0081.jpg", "rb") as f:
    image_bytes = f.read()

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])  # zamiast wpisywać klucz w kodzie

prompt = (
    "Przepisz tekst z obrazka po polsku, linijka po linijce dokładnie jak na zdjęciu. "
    "Następnie podaj listę rekordów. Dla każdego rekordu wypisz: "
    "imie_nazwisko, data_miejsce_urodzenia, data_przyczyna_zgonu, dodatkowe informacje."
    "Zwróć WYŁĄCZNIE poprawny JSON zgodny ze schematem." \
    "JSON ma zawierać te same pola zmiennych w każdym rekordzie. Oczyść tekst z jakichś niedoczytanych" \
    "wyrazów."
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
