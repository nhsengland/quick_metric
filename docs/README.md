# Generating the Documentation

Use [MkDocs](http://www.mkdocs.org/) with [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) to update the documentation.

## Prerequisites

Install the documentation dependencies:

**Using uv (recommended):**

```bash
uv pip install -e .[docs]
```

**Using pip:**

```bash
pip install -e .[docs]
```

## Building Documentation

### Serve Locally

Start a development server with live reloading:

**Using uv (recommended):**

```bash
uv run mkdocs serve
```

**Using mkdocs directly:**

```bash
mkdocs serve
```

The documentation will be available at `http://127.0.0.1:8000`

### Build Static Site

Generate the static documentation site:

**Using uv (recommended):**

```bash
uv run mkdocs build
```

**Using mkdocs directly:**

```bash
mkdocs build
```

The built site will be available in the `site/` directory.

## Deployment

Documentation is automatically deployed to GitHub Pages when changes are pushed to the `main` branch.
