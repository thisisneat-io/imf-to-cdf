# imf-to-neat — NEAT plugin for IMF ontologies

Converts one or more PCA IMF Turtle (`.ttl`) ontology files into a **NEAT physical data model** (views, containers, properties with value types).

## Features

- Aspect-aware view naming (`Plant`, `Product`, `Installed`, `Function`, `Location`)
- Value-type resolution from CFIHOS, RDS (PCA SPARQL), IEC CDD and label heuristics
- Deprecated-term cleaning (`dcterms:replaces`)
- Container optimization: CFIHOS hierarchy grouping, identical-fingerprint merging, subset detection, `AllInstalled` pooling

## Installation

```bash
# From the plugin directory
pip install -e .

# or with uv
uv pip install -e .
```

The plugin and `cognite-neat` must be in the **same Python environment**.

## Usage — NEAT plugin API

```python
from cognite.neat import NeatSession

neat = NeatSession()  # or NeatSession(client) with a CDF client

# Single file
neat.plugin.data_model.read(
    "imf",
    io="imf_ontology.ttl",
    clean=True,
    optimize_containers=True,
    space="my_imf_space",
    external_id="MyIMFDataModel",
    version="v1",
)

# Multiple files merged into one model
neat.plugin.data_model.read(
    "imf",
    io=["types_a.ttl", "types_b.ttl"],
    cfihos_csv="CFIHOS CORE property v2.0.csv",
)
```

### All keyword arguments

| Argument | Type | Default | Description |
|---|---|---|---|
| `io` | `str \| Path \| list` | — | Required. TTL file(s) |
| `clean` | `bool` | `True` | Remove deprecated terms |
| `cfihos_csv` | `Path` | `None` | Local CFIHOS property CSV |
| `cfihos_url` | `str` | CFIHOS ZIP URL | Online CFIHOS data source |
| `cfihos_cache` | `Path` | `None` | JSON cache for online CFIHOS data |
| `rds_cache` | `Path` | `None` | JSON cache for RDS numeric codes |
| `cdd_lookup` | `Path` | `None` | Enriched IEC CDD CSV with `Value type` column |
| `optimize_containers` | `bool` | `False` | Enable container optimization |
| `space` | `str` | `"imf_space"` | CDF space |
| `external_id` | `str` | `"IMFDataModel"` | Model externalId |
| `version` | `str` | `"v1"` | Model version |
| `name` | `str` | `None` | Human-readable name |
| `description` | `str` | `None` | Model description |
| `creator` | `str` | `"NEAT"` | Creator |

## Usage — standalone CLI

After installation the `imf-to-neat` command is available:

```bash
imf-to-neat imf_ontology.ttl --clean --optimize-containers
imf-to-neat a.ttl b.ttl -o merged_model.yaml
imf-to-neat ontology.ttl --cfihos-csv "CFIHOS CORE property v2.0.csv" --rds-sparql
```

## Plugin structure

```
imf-to-neat-plugin/
├── pyproject.toml
├── README.md
└── imf_to_neat/
    ├── __init__.py
    ├── plugin.py       # IMFToNeatPlugin (DataModelImporterPlugin)
    ├── _importer.py    # IMFToNeatImporter (BaseImporter)
    ├── _core.py        # All processing logic
    └── cli.py          # CLI entry point (imf-to-neat command)
```
