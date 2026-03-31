# Linting Setup

Linting has been set up for the project to keep code consistent and avoid formatting issues in every PR.

## What does it do?

We use three tools:
- **Black** - automatically formats code (88 characters per line)
- **isort** - sorts imports properly
- **Flake8** - checks if code is clean (no unused variables, etc.)

## Setup (one-time)

```bash
uv pip install -r requirements-dev.txt
```

## How to use it?

### Automatically format code

Simply run before committing:

```bash
black .
isort .
```

This automatically fixes everything.

### Only check without changing

If you just want to see what doesn't match:

```bash
black --check .
isort --check-only .
flake8 .
```

### Pre-commit Hooks (recommended!)

So you don't forget to lint:

```bash
uv pip install pre-commit
pre-commit install
```

Now the checks run automatically on every `git commit`. If something doesn't match, the commit is blocked and you can fix it.

To check all files at once:
```bash
pre-commit run --all-files
```

## GitHub Action

A GitHub Action has been set up that runs on every PR to main. If the code is not linted, the PR will be blocked. So please run locally first!

The action is in `.github/workflows/lint.yml`

## Config Files

If you want to adjust something:
- `.flake8` - Flake8 settings
- `pyproject.toml` - Black & isort settings
- `.pre-commit-config.yaml` - Pre-commit hook config
