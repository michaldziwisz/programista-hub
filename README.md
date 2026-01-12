# Programista Hub

Backend dla projektu **Programista**: przechowuje zindeksowane ramówki i udostępnia je przez HTTP API (m.in. wyszukiwarka), a dane aktualizuje z paczek dostawców (`programista-providers`) oraz z pozostałych źródeł.

## Bezpieczeństwo

- Repozytorium **nie zawiera** żadnych sekretów ani kluczy dostępowych.
- Sekrety (np. `PROGRAMISTA_HUB_GITHUB_WEBHOOK_SECRET`) muszą być dostarczone wyłącznie przez zmienne środowiskowe / pliki `.env` ignorowane przez git.

## Wymagania

- Python 3.12+
- PostgreSQL 14+ (lokalnie lub w kontenerze)

## Uruchomienie (dev)

1. Zainstaluj zależności:
   - `pip install -r requirements.txt`
2. Ustaw `PROGRAMISTA_HUB_DB_DSN` (np. do lokalnego Postgresa).
3. Uruchom API:
   - `uvicorn programista_hub_api:app --host 127.0.0.1 --port 18080`
4. (opcjonalnie) Uruchom worker:
   - `python programista_hub_worker.py`

## Kluczowe zmienne środowiskowe

- `PROGRAMISTA_HUB_DB_DSN` – DSN do Postgresa.
- `PROGRAMISTA_HUB_REQUIRE_API_KEY` – `1` wymusza `X-Programista-Key` dla większości endpointów.
- `PROGRAMISTA_HUB_GITHUB_WEBHOOK_SECRET` – sekret do weryfikacji webhooka GitHuba (`/webhook/providers`).
- `PROGRAMISTA_HUB_PROVIDERS_BASE_URL` – baza do `latest.json` z `programista-providers` (domyślnie GitHub Releases).

