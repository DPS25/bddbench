# Linting Setup

Hab für das Projekt Linting eingerichtet, damit der Code einheitlich bleibt und wir nicht bei jedem PR über Formatierung diskutieren müssen.

## Was macht das?

Wir nutzen drei Tools:
- **Black** - formatiert den Code automatisch (88 Zeichen pro Zeile)
- **isort** - sortiert die Imports ordentlich
- **Flake8** - checkt ob der Code sauber ist (keine ungenutzten Variablen, etc.)

## Setup (einmalig)

```bash
pip install -r requirements-dev.txt
```

## Wie benutzt man das?

### Code automatisch formatieren

Einfach vor dem Commit laufen lassen:

```bash
black .
isort .
```

Das fixt automatisch alles.

### Nur checken ohne zu ändern

Wenn ihr nur schauen wollt was nicht passt:

```bash
black --check .
isort --check-only .
flake8 .
```

### Pre-commit Hooks (empfohlen!)

Damit ihr nicht vergessen könnt zu linten:

```bash
pip install pre-commit
pre-commit install
```

Jetzt laufen die Checks automatisch bei jedem `git commit`. Wenn was nicht passt, wird der Commit blockiert und ihr könnt es fixen.

Um alle Files auf einmal zu checken:
```bash
pre-commit run --all-files
```

## GitHub Action

Hab ne GitHub Action eingerichtet die bei jedem PR zu main läuft. Wenn der Code nicht gelintet ist, wird der PR blockiert. Also bitte vorher lokal laufen lassen!

Die Action ist in `.github/workflows/lint.yml`

## Config Files

Falls ihr was anpassen wollt:
- `.flake8` - Flake8 Settings
- `pyproject.toml` - Black & isort Settings
- `.pre-commit-config.yaml` - Pre-commit Hook Config
