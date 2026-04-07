"""
Core processing logic: parse IMF Turtle ontologies and build a NEAT physical
data-model dictionary.  Used by both the plugin (IMFToNeatImporter) and the
standalone CLI (imf_to_neat.py).
"""
from __future__ import annotations

import csv
import io
import json
import re
import sys
import zipfile
from collections import OrderedDict, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from rdflib import BNode, Graph, Namespace, RDF, RDFS, URIRef
from rdflib.namespace import SKOS

# ---------------------------------------------------------------------------
# Namespaces
# ---------------------------------------------------------------------------
IMF    = Namespace("http://ns.imfid.org/imf#")
SHACL  = Namespace("http://www.w3.org/ns/shacl#")
DCTERMS = Namespace("http://purl.org/dc/terms/")

try:
    import requests as _requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False

# ---------------------------------------------------------------------------
# CFIHOS value-type lookup  (local CSV  *or*  online ZIP / Excel)
# ---------------------------------------------------------------------------
_CFIHOS_CODE_RE = re.compile(r"(CFIHOS-\d+)", re.IGNORECASE)
_cfihos_type_map: dict[str, str] = {}

_cfihos_tag_hier:   dict[str, dict] = {}
_cfihos_equip_hier: dict[str, dict] = {}
_cfihos_tag_by_len:   list[tuple[str, str]] = []
_cfihos_equip_by_len: list[tuple[str, str]] = []

_CFIHOS_ZIP_URL  = "https://www.jip36-cfihos.org/wp-content/uploads/2024/10/CORE-CFIHOS-CSV-v2.0.zip"
_CFIHOS_XLSX_URL = "https://www.jip36-cfihos.org/wp-content/uploads/2025/12/C-ST-001-Extended-Reference-Data-Library-1.xlsx"


def _csv_number_type(dimension: str) -> str:
    return "int32" if dimension.strip().upper() == "COUNT" else "float64"


def _parse_cfihos_rows(rows: list[dict]) -> None:
    for row in rows:
        code      = (row.get("CFIHOS unique code") or "").strip()
        data_type = (row.get("property data type") or "").strip().lower()
        if not code or not data_type:
            continue
        if data_type == "number":
            neat_type = _csv_number_type(row.get("unit of measure dimension code") or "")
        elif data_type == "boolean":
            neat_type = "boolean"
        else:
            neat_type = "text"
        _cfihos_type_map[code] = neat_type


def load_cfihos_csv(csv_path: Path) -> None:
    with open(csv_path, newline="", encoding="cp1252") as f:
        _parse_cfihos_rows(list(csv.DictReader(f)))
    load_cfihos_hierarchy(csv_path.parent)


def _load_cfihos_from_zip(raw_bytes: bytes) -> None:
    z = zipfile.ZipFile(io.BytesIO(raw_bytes))
    names = z.namelist()
    csv_name = next(
        (n for n in names
         if re.search(r"CFIHOS CORE property v", n, re.IGNORECASE)
         and "class property" not in n.lower()
         and "grouping" not in n.lower()), None
    )
    if not csv_name:
        raise ValueError("Could not find CFIHOS property CSV inside ZIP archive")
    with z.open(csv_name) as f:
        text = f.read().decode("cp1252")
    _parse_cfihos_rows(list(csv.DictReader(io.StringIO(text))))

    def _parse_hier(pattern, code_col, name_col, parent_col):
        entry = next((n for n in names if re.search(pattern, n, re.IGNORECASE)), None)
        if not entry:
            return {}
        with z.open(entry) as f:
            try:
                text = f.read().decode("utf-8-sig")
            except UnicodeDecodeError:
                text = f.read().decode("cp1252")
        rows = list(csv.DictReader(io.StringIO(text)))
        return {
            row[code_col]: {"name": row[name_col].strip(), "parent": row[parent_col].strip()}
            for row in rows if row.get(code_col)
        }

    global _cfihos_tag_hier, _cfihos_equip_hier, _cfihos_tag_by_len, _cfihos_equip_by_len
    tag_hier   = _parse_hier(r"CFIHOS CORE tag class v",
                             "CFIHOS unique code", "tag class name", "parent tag class name")
    equip_hier = _parse_hier(r"CFIHOS CORE equipment class v",
                             "equipment class CFIHOS unique code",
                             "equipment class name", "parent equipment class name")
    if tag_hier:
        _cfihos_tag_hier = tag_hier
        _cfihos_tag_by_len = sorted(
            [(_cfihos_norm(v["name"]), k) for k, v in _cfihos_tag_hier.items()],
            key=lambda x: -len(x[0].split()),
        )
    if equip_hier:
        _cfihos_equip_hier = equip_hier
        _cfihos_equip_by_len = sorted(
            [(_cfihos_norm(v["name"]), k) for k, v in _cfihos_equip_hier.items()],
            key=lambda x: -len(x[0].split()),
        )


def _load_cfihos_from_xlsx(raw_bytes: bytes) -> None:
    try:
        import openpyxl
    except ImportError:
        raise ImportError("openpyxl is required: pip install openpyxl")
    wb = openpyxl.load_workbook(io.BytesIO(raw_bytes), read_only=True)
    ws = wb["property"]
    row_iter = ws.iter_rows(values_only=True)
    headers = [str(h).strip() if h is not None else "" for h in next(row_iter)]

    def _val(row, col):
        idx = headers.index(col) if col in headers else -1
        return str(row[idx]).strip() if idx >= 0 and row[idx] is not None else ""

    _parse_cfihos_rows([
        {"CFIHOS unique code":          _val(r, "CFIHOS unique code"),
         "property data type":          _val(r, "property data type"),
         "unit of measure dimension code": _val(r, "unit of measure dimension code")}
        for r in row_iter
    ])
    wb.close()


def load_cfihos_online(url: str = _CFIHOS_ZIP_URL,
                       cache_file: Path | None = None,
                       force: bool = False) -> None:
    global _cfihos_type_map
    if not force and cache_file and cache_file.exists():
        _cfihos_type_map = json.loads(cache_file.read_text(encoding="utf-8"))
        print(f"CFIHOS type map loaded from cache: {len(_cfihos_type_map)} codes ({cache_file})")
        return
    if not _REQUESTS_AVAILABLE:
        print("Warning: 'requests' not installed — CFIHOS online lookup skipped.", file=sys.stderr)
        return
    print(f"Downloading CFIHOS property data from:\n  {url}")
    try:
        r = _requests.get(url, timeout=120)
        r.raise_for_status()
        if ".zip" in url.lower():
            _load_cfihos_from_zip(r.content)
        elif ".xlsx" in url.lower() or ".xls" in url.lower():
            _load_cfihos_from_xlsx(r.content)
        else:
            raise ValueError(f"Cannot determine file type from URL: {url}")
        print(f"  Loaded {len(_cfihos_type_map)} CFIHOS property type mappings")
        if cache_file:
            cache_file.write_text(json.dumps(_cfihos_type_map, indent=2), encoding="utf-8")
            print(f"  Cached to: {cache_file}")
    except Exception as exc:
        print(f"Warning: CFIHOS online fetch failed ({exc}).", file=sys.stderr)


# ---------------------------------------------------------------------------
# RDS numeric lookup
# ---------------------------------------------------------------------------
_RDS_CODE_RE  = re.compile(r"RDS\d+")
_RDS_SPARQL   = "https://data.posccaesar.org/rdl/sparql"
_RDS_QUERY    = """SELECT DISTINCT ?subject WHERE {
  ?subject a <http://rds.posccaesar.org/2008/02/OWL/ISO-15926-2_2003#SinglePropertyDimension>.
}"""
_rds_numeric_set: set[str] = set()


def load_rds_numeric_set(cache_file: Path | None = None) -> None:
    global _rds_numeric_set
    if cache_file and cache_file.exists():
        _rds_numeric_set = set(json.loads(cache_file.read_text(encoding="utf-8")))
        print(f"RDS numeric set loaded from cache: {len(_rds_numeric_set)} codes ({cache_file})")
        return
    if not _REQUESTS_AVAILABLE:
        print("Warning: 'requests' not installed — RDS numeric lookup skipped.", file=sys.stderr)
        return
    print("Fetching RDS numeric attribute list from PCA SPARQL endpoint...")
    try:
        r = _requests.get(_RDS_SPARQL, params={"query": _RDS_QUERY, "format": "csv"}, timeout=30)
        r.raise_for_status()
        codes = {m.group() for line in r.text.splitlines()[1:] if (m := _RDS_CODE_RE.search(line))}
        _rds_numeric_set = codes
        print(f"  Retrieved {len(_rds_numeric_set)} numeric RDS codes")
        if cache_file:
            cache_file.write_text(json.dumps(sorted(_rds_numeric_set), indent=2), encoding="utf-8")
    except Exception as exc:
        print(f"Warning: RDS SPARQL fetch failed ({exc}).", file=sys.stderr)


# ---------------------------------------------------------------------------
# IEC CDD lookup
# ---------------------------------------------------------------------------
_cdd_type_map: dict[str, str] = {}
_CDD_URI_RE = re.compile(r"cdd\.iec\.ch", re.IGNORECASE)


def _cdd_native_to_neat(raw: str) -> str:
    r = raw.strip()
    if not r:
        return "text"
    if "REAL_MEASURE_TYPE" in r or r == "REAL_TYPE":
        return "float64"
    if "INT_MEASURE_TYPE" in r or r == "INT_TYPE":
        return "int32"
    if r in ("STRING_TYPE", "TRANSLATABLE_STRING_TYPE", "NON_TRANSLATABLE_STRING_TYPE"):
        return "text"
    if r.startswith("ENUM_CODE_TYPE") or r in ("ITEM_CLASS",) or r.startswith("CLASS_REFERENCE_TYPE"):
        return "text"
    return "text"


def load_cdd_lookup(csv_path: Path) -> None:
    loaded = skipped = 0
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            uri = row.get("full_uri", "").strip()
            vt  = row.get("Value type", "").strip()
            if not uri:
                continue
            if not vt:
                skipped += 1
                continue
            _cdd_type_map[uri] = _cdd_native_to_neat(vt)
            loaded += 1
    print(f"Loaded {loaded} IEC CDD type mappings from: {csv_path}"
          + (f" ({skipped} rows skipped)" if skipped else ""))


# ---------------------------------------------------------------------------
# Label heuristics (fallback)
# ---------------------------------------------------------------------------
_BOOL_WORDS = frozenset({"REQUIRED", "ENABLED", "ACTIVATED", "AVAILABLE", "APPLICABLE",
                          "REVERSIBLE", "INTERCHANGEABLE", "CERTIFIED"})
_INT_PREFIXES = ("NO_OF_", "NUMBER_OF_", "NR_OF_", "NUM_OF_")
_INT_SUFFIXES = ("_COUNT", "_POLES", "_PHASES", "_PULSES", "_CONNECTIONS",
                 "_TERMINALS", "_GENERATORS", "_CABLES")
_TEXT_TOKENS = frozenset({"MODE", "CATEGORY", "CLASS", "STANDARD", "CODE", "TYPE",
                           "COLOUR", "COLOR", "ACTION", "SCHEDULE", "VERSION",
                           "PROTOCOL", "APPLICATION", "REQUIREMENT", "CLASSIFICATION",
                           "FACING", "FINISH", "STATUS", "GRADE", "INDICATOR",
                           "CONDITION", "CONDITIONS", "DESCRIPTION", "SPECIFICATION",
                           "PROTECTION"})
_FLOAT_WORDS = frozenset({"LENGTH", "DIAMETER", "RADIUS", "HEIGHT", "WIDTH", "DEPTH",
                           "AREA", "VOLUME", "DISTANCE", "THICKNESS", "CLEARANCE",
                           "DIMENSION", "WEIGHT", "MASS", "FORCE", "TORQUE", "LOAD",
                           "PRESSURE", "VELOCITY", "SPEED", "FLOW", "ANGLE", "DEFLECTION",
                           "SIZE", "MESH", "TEMPERATURE", "TEMP", "HUMIDITY", "ALTITUDE",
                           "CURRENT", "VOLTAGE", "POWER", "RESISTANCE", "IMPEDANCE",
                           "FREQUENCY", "CAPACITANCE", "INDUCTANCE", "DENSITY", "VISCOSITY",
                           "CONDUCTIVITY", "CONCENTRATION", "ENERGY", "CAPACITY",
                           "EFFICIENCY", "OVERPRESSURE", "RATIO", "FACTOR", "COEFFICIENT",
                           "CONSTANT", "RANGE", "VALUE", "SIGNAL", "MINIMUM", "MAXIMUM",
                           "MIN", "MAX", "RATING", "LOADING", "STABILITY", "DRIFT",
                           "TIME", "DURATION", "PERIOD", "DELAY", "DEADBAND", "BAND",
                           "REPEATABILITY", "ACCURACY", "PRECISION"})
_FLOAT_NAME_PREFIXES = ("OPER_", "DESIGN_", "RATED_", "NOMINAL_", "BASE_",
                         "MAX_", "MIN_", "SIGNAL_", "MEASUREMENT_")


def _heuristic_type_from_label(label: str) -> str:
    primary = label.split(";")[0].strip().upper()
    normalised = re.sub(r"[\s\-]+", "_", primary)
    for word in _BOOL_WORDS:
        if word in normalised:
            return "boolean"
    if any(normalised.startswith(pfx) for pfx in _INT_PREFIXES):
        return "int32"
    if any(normalised.endswith(sfx) for sfx in _INT_SUFFIXES):
        return "int32"
    tokens = set(normalised.split("_"))
    if tokens & _TEXT_TOKENS:
        return "text"
    if any(normalised.startswith(pfx) for pfx in _FLOAT_NAME_PREFIXES):
        return "float64"
    if tokens & _FLOAT_WORDS:
        return "float64"
    return "text"


# ---------------------------------------------------------------------------
# Combined value-type resolver
# ---------------------------------------------------------------------------
def resolve_value_type(predicate_uri: str, label: str | None = None) -> str:
    cfihos_m = _CFIHOS_CODE_RE.search(predicate_uri)
    if cfihos_m:
        return _cfihos_type_map.get(cfihos_m.group(1).upper(), "text")
    rds_m = _RDS_CODE_RE.search(predicate_uri)
    if rds_m and rds_m.group() in _rds_numeric_set:
        return "float64"
    if _cdd_type_map and _CDD_URI_RE.search(predicate_uri):
        if predicate_uri in _cdd_type_map:
            return _cdd_type_map[predicate_uri]
    if label:
        return _heuristic_type_from_label(label)
    return "text"


# ---------------------------------------------------------------------------
# CFIHOS class hierarchy helpers
# ---------------------------------------------------------------------------
def _cfihos_norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def load_cfihos_hierarchy(base_dir: Path) -> bool:
    global _cfihos_tag_hier, _cfihos_equip_hier, _cfihos_tag_by_len, _cfihos_equip_by_len
    tag_path   = base_dir / "CFIHOS CORE tag class v2.0.csv"
    equip_path = base_dir / "CFIHOS CORE equipment class v2.0.csv"
    if not tag_path.exists() or not equip_path.exists():
        return False

    def _read(path, code_col, name_col, parent_col, enc="utf-8-sig"):
        hier: dict[str, dict] = {}
        try:
            with open(path, encoding=enc) as f:
                for row in csv.DictReader(f):
                    hier[row[code_col]] = {"name": row[name_col].strip(),
                                           "parent": row[parent_col].strip()}
        except UnicodeDecodeError:
            with open(path, encoding="cp1252") as f:
                for row in csv.DictReader(f):
                    hier[row[code_col]] = {"name": row[name_col].strip(),
                                           "parent": row[parent_col].strip()}
        return hier

    _cfihos_tag_hier   = _read(tag_path,   "CFIHOS unique code",
                               "tag class name", "parent tag class name")
    _cfihos_equip_hier = _read(equip_path, "equipment class CFIHOS unique code",
                               "equipment class name", "parent equipment class name")
    _cfihos_tag_by_len = sorted(
        [(_cfihos_norm(v["name"]), k) for k, v in _cfihos_tag_hier.items()],
        key=lambda x: -len(x[0].split()),
    )
    _cfihos_equip_by_len = sorted(
        [(_cfihos_norm(v["name"]), k) for k, v in _cfihos_equip_hier.items()],
        key=lambda x: -len(x[0].split()),
    )
    return True


def _cfihos_ancestors(code: str, hier: dict[str, dict]) -> list[str]:
    if not code or code not in hier:
        return []
    chain = [code]
    seen: set[str] = {code}
    for _ in range(20):
        parent_name = hier[chain[-1]]["parent"]
        if not parent_name:
            break
        parent_norm = _cfihos_norm(parent_name)
        by_len = _cfihos_tag_by_len if hier is _cfihos_tag_hier else _cfihos_equip_by_len
        parent_code = next((c for n, c in by_len if n == parent_norm), None)
        if not parent_code or parent_code in seen:
            break
        chain.append(parent_code)
        seen.add(parent_code)
    return chain


def _match_to_cfihos(label: str, is_plant: bool) -> str | None:
    by_len = _cfihos_tag_by_len if is_plant else _cfihos_equip_by_len
    n = _cfihos_norm(label)
    for cname, code in by_len:
        if cname == n or cname in n:
            return code
    return None


def _cfihos_to_camel(name: str) -> str:
    return "".join(w.capitalize() for w in re.sub(r"[^a-z0-9 ]+", " ", name.lower()).split())


def _cfihos_container_name(hier: dict[str, dict], code: str, aspect: str, depth: int = 1) -> str | None:
    chain = _cfihos_ancestors(code, hier)
    if len(chain) <= depth:
        return None
    anc_code = chain[depth]
    return _cfihos_to_camel(hier[anc_code]["name"]) + aspect


# ---------------------------------------------------------------------------
# Deprecated-term removal  (in-memory)
# ---------------------------------------------------------------------------
def remove_replaced_terms(g: Graph) -> tuple[int, int]:
    replaced = {o for _, _, o in g.triples((None, DCTERMS.replaces, None)) if isinstance(o, URIRef)}
    subj_triples = [t for subj in replaced for t in g.triples((subj, None, None))]
    shape_bnodes: set[BNode] = set()
    for subj in replaced:
        for s, _, _ in g.triples((None, SHACL.path, subj)):
            if isinstance(s, BNode):
                shape_bnodes.add(s)
    shape_triples = [t for bn in shape_bnodes
                     for t in list(g.triples((bn, None, None))) + list(g.triples((None, None, bn)))]
    for triple in subj_triples + shape_triples:
        g.remove(triple)
    return len(replaced), len(shape_bnodes)


# ---------------------------------------------------------------------------
# Graph helpers
# ---------------------------------------------------------------------------
def load_ontology(*ttl_paths: str | Path) -> Graph:
    g = Graph()
    for path in ttl_paths:
        g.parse(str(path), format="turtle")
    return g


def get_local_name(uri: str) -> str:
    s = str(uri)
    return s.split("#")[-1] if "#" in s else s.split("/")[-1]


def get_label(g: Graph, subject) -> object | None:
    return g.value(subject, RDFS.label) or g.value(subject, SKOS.prefLabel)


def clean_view_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", name)


_ASPECT_RE = re.compile(r"\bin\s+(\w+)\s+aspect\b", re.IGNORECASE)


def extract_aspect(definition: str | None) -> str | None:
    if not definition:
        return None
    m = _ASPECT_RE.search(definition)
    return m.group(1).capitalize() if m else None


def clean_property_name(name: str) -> str:
    words = re.split(r"[\s_\-]+", name.strip())
    return words[0].lower() + "".join(w.capitalize() for w in words[1:]) if words else name


def clean_text(text: str | None) -> str | None:
    if text is None:
        return None
    cleaned = re.sub(r"[\r\n]+", " ", text)
    return cleaned.replace("'", "").replace('"', "").replace("`", "")


_CONTAINER_PROP_DESC_MAX = 1024


def build_container_prop_desc(prop_name: str, prop_uri: str | None,
                               predicate_uri: str | None, definition: str | None) -> str:
    sep = " | "
    parts = [prop_name]
    if prop_uri:
        parts.append(prop_uri)
    if predicate_uri:
        parts.append(predicate_uri)
    prefix = sep.join(parts) + sep
    budget = _CONTAINER_PROP_DESC_MAX - len(prefix)
    if definition and budget > 0:
        defn = clean_text(definition) or ""
        if len(defn) > budget:
            cut = defn[:budget - 1].rsplit(" ", 1)[0]
            defn = cut + "…"
        return prefix + defn
    return prefix[:_CONTAINER_PROP_DESC_MAX]


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
def _normalize_label_for_key(label: str) -> str:
    """Title-case a label so that word-equivalent labels produce the same view key.

    'wave level transmitter' and 'Wave Level Transmitter' both become
    'Wave Level Transmitter', which clean_view_name() then turns into
    'WaveLevelTransmitter'.  This prevents CDF from receiving two view /
    container externalIds that differ only in capitalisation.
    """
    return label.title()


def _build_type_dict(g: Graph, rdf_type) -> dict:
    """Generic extractor for BlockType and TerminalType."""
    result: dict[str, dict] = {}
    # key_lower_seen maps lowercase(base_key) -> canonical base_key already used.
    # This catches case-insensitive collisions that survive title-casing
    # (e.g. acronyms like 'IMF' vs 'Imf').
    key_lower_seen: dict[str, str] = {}
    key_counts: dict[str, int] = {}

    for node in g.subjects(RDF.type, rdf_type):
        label_str = str(get_label(g, node) or get_local_name(node))
        defn_str  = str(g.value(node, SKOS.definition)) if g.value(node, SKOS.definition) else None
        aspect    = extract_aspect(defn_str)

        # Normalise to title case so labels that are the same words with
        # different capitalisation resolve to the same base_key.
        normalized = _normalize_label_for_key(label_str)
        base_key   = clean_view_name(normalized) + (aspect or "")
        lower_key  = base_key.lower()

        if lower_key in key_lower_seen:
            # Case-insensitive collision: use the canonical casing already chosen
            canonical = key_lower_seen[lower_key]
            n = key_counts.get(canonical, 1) + 1
            key_counts[canonical] = n
            key = f"{canonical}_{n}"
        elif base_key in result:
            # Exact collision (shouldn't happen after title-case normalisation,
            # but keep as safety net)
            n = key_counts.get(base_key, 1) + 1
            key_counts[base_key] = n
            key = f"{base_key}_{n}"
        else:
            key = base_key
            key_lower_seen[lower_key] = base_key

        result[key] = {"uri": str(node), "label": label_str,
                       "definition": defn_str, "aspect": aspect}
    return result


def extract_block_types(g: Graph) -> dict:
    return _build_type_dict(g, IMF.BlockType)


def extract_terminal_types(g: Graph) -> dict:
    return _build_type_dict(g, IMF.TerminalType)


def extract_attribute_types(g: Graph) -> dict:
    attributes: dict[str, dict] = {}
    for attr in g.subjects(RDF.type, IMF.AttributeType):
        label     = get_label(g, attr)
        defn      = g.value(attr, SKOS.definition)
        predicate = g.value(attr, IMF.predicate)
        attributes[str(attr)] = {
            "uri":       str(attr),
            "label":     str(label) if label else get_local_name(attr),
            "definition": str(defn) if defn else None,
            "predicate": str(predicate) if predicate else None,
        }
    return attributes


def extract_shacl_properties(g: Graph, shape_uri) -> list:
    props = []
    for prop_node in g.objects(shape_uri, SHACL.property):
        path       = g.value(prop_node, SHACL.path)
        min_count  = g.value(prop_node, SHACL.minCount)
        max_count  = g.value(prop_node, SHACL.maxCount)
        qual_shape = g.value(prop_node, SHACL.qualifiedValueShape)
        target_cls = g.value(qual_shape, SHACL["class"]) if qual_shape else None
        if path:
            props.append({
                "path":        str(path),
                "minCount":    int(min_count) if min_count else 0,
                "maxCount":    int(max_count) if max_count else None,
                "targetClass": str(target_cls) if target_cls else None,
            })
    return props


# ---------------------------------------------------------------------------
# Container optimizer
# ---------------------------------------------------------------------------
def optimize_containers(properties: list[dict],
                        all_items: dict) -> tuple[list[dict], list[dict]]:
    view_props: dict[str, list[dict]] = defaultdict(list)
    for p in properties:
        view_props[p["View"]].append(p)

    def _fp(props):
        return frozenset(
            (p["Container Property"], p["Value Type"])
            for p in props if not p.get("Connection")
        )

    view_fp = {v: _fp(ps) for v, ps in view_props.items()}

    # Strategy 0: CFIHOS hierarchy grouping
    cfihos_container: dict[str, str] = {}
    cfihos_shared_fp: dict[str, frozenset] = {}
    synthetic_containers: list[dict] = []

    if _cfihos_tag_hier or _cfihos_equip_hier:
        _MIN_GROUP = 2
        cfihos_groups: dict[str, list[str]] = defaultdict(list)
        for view_key, item in all_items.items():
            aspect = item.get("aspect", "")
            if aspect not in ("Plant", "Product"):
                continue
            code = _match_to_cfihos(item["label"], aspect == "Plant")
            if code is None:
                continue
            hier = _cfihos_tag_hier if aspect == "Plant" else _cfihos_equip_hier
            cname = _cfihos_container_name(hier, code, aspect, depth=1)
            if cname:
                cfihos_groups[cname].append(view_key)

        for cname, members in cfihos_groups.items():
            if len(members) < _MIN_GROUP:
                continue
            fps = [view_fp.get(v, frozenset()) for v in members]
            non_empty = [f for f in fps if f]
            shared = (frozenset.intersection(*non_empty)
                      if len(non_empty) > 1 else (non_empty[0] if non_empty else frozenset()))
            cfihos_shared_fp[cname] = shared
            for v in members:
                cfihos_container[v] = cname

        for cname in sorted(set(cfihos_container.values())):
            aspect_suffix = ("Plant" if cname.endswith("Plant")
                             else "Product" if cname.endswith("Product") else "")
            hier = _cfihos_tag_hier if aspect_suffix == "Plant" else _cfihos_equip_hier
            base = cname[: -len(aspect_suffix)] if aspect_suffix else cname
            cfihos_name = next(
                (v["name"] for v in hier.values() if _cfihos_to_camel(v["name"]) == base), base)
            aspect_label = f" - {aspect_suffix}" if aspect_suffix else ""
            synthetic_containers.append({
                "Container":   cname,
                "Name":        f"{cfihos_name}{aspect_label}",
                "Description": (f"Shared CFIHOS-hierarchy container for all "
                                 f"'{cfihos_name}' {aspect_suffix} views."),
                "Used For": "node",
            })

        n_matched = len(cfihos_container)
        n_groups  = len(set(cfihos_container.values()))
        print(f"    CFIHOS hierarchy groups  : {n_groups} groups, {n_matched} views matched")

    # Strategy 1: Identical fingerprint grouping
    fp_to_views: dict[frozenset, list[str]] = defaultdict(list)
    for v in sorted(view_fp):
        fp_to_views[view_fp[v]].append(v)
    canonical: dict[str, str] = {}
    for fp_set, views in fp_to_views.items():
        canon = sorted(views)[0]
        for v in views:
            canonical[v] = canon

    canon_names = sorted(set(canonical.values()))
    canon_fp    = {c: view_fp[c] for c in canon_names}

    # Strategy 2: Subset / inheritance detection
    def _aspect_of(k):
        for suf in ("Plant", "Product", "Installed", "Function", "Location"):
            if k.endswith(suf) or f"_{suf}" in k:
                return suf
        return ""

    canon_aspects = {c: _aspect_of(c) for c in canon_names}
    storage: dict[str, dict[tuple, str]] = {}
    saved_subset = 0

    for c in sorted(canon_names, key=lambda x: len(canon_fp[x])):
        storage[c] = {}
        c_aspect = canon_aspects.get(c, "")
        best_parent, best_size = None, -1
        for other in canon_names:
            if other == c:
                continue
            if c_aspect and canon_aspects.get(other, "") not in (c_aspect, ""):
                continue
            if canon_fp[other] < canon_fp[c] and len(canon_fp[other]) > best_size:
                best_size = len(canon_fp[other])
                best_parent = other
        if best_parent:
            for prop_key, stored_in in storage[best_parent].items():
                storage[c][prop_key] = stored_in
            saved_subset += len(storage[best_parent])
        for prop_key in canon_fp[c]:
            if prop_key not in storage[c]:
                storage[c][prop_key] = c

    # Re-assign Container per property row
    new_properties: list[dict] = []
    for p in properties:
        v = p["View"]
        new_p = dict(p)
        if p.get("Connection"):
            new_p["Container"] = v
        elif v in cfihos_container:
            prop_key  = (p["Container Property"], p["Value Type"])
            cname     = cfihos_container[v]
            shared_fp = cfihos_shared_fp.get(cname, frozenset())
            if prop_key in shared_fp:
                new_p["Container"] = cname
            else:
                c = canonical[v]
                new_p["Container"] = storage.get(c, {}).get(prop_key, c)
        else:
            c = canonical[v]
            prop_key  = (p["Container Property"], p["Value Type"])
            new_p["Container"] = storage.get(c, {}).get(prop_key, c)
        new_properties.append(new_p)

    # Pool all Installed-aspect properties into AllInstalled
    _ALL_INSTALLED = "AllInstalled"
    installed_views = {p["View"] for p in new_properties if p["View"].endswith("Installed")}
    n_installed = len(installed_views)
    if installed_views:
        for p in new_properties:
            if p["View"] in installed_views and not p.get("Connection"):
                p["Container"] = _ALL_INSTALLED

    # Rebuild containers list
    referenced = {p["Container"] for p in new_properties}
    cfihos_synthetic_names = {sc["Container"] for sc in synthetic_containers}

    new_containers: list[dict] = []
    for sc in synthetic_containers:
        if sc["Container"] in referenced:
            new_containers.append(sc)
    for key, item in all_items.items():
        if key in referenced and key not in cfihos_synthetic_names:
            aspect = item.get("aspect")
            display_name = (f"{clean_text(item['label'])} - {aspect}"
                            if aspect else clean_text(item["label"]))
            base_desc = clean_text(item["definition"]) or f"Container for {display_name}"
            new_containers.append({"Container": key, "Name": display_name,
                                   "Description": f"{base_desc} [URI: {item['uri']}]",
                                   "Used For": "node"})
    if installed_views and _ALL_INSTALLED in referenced:
        new_containers.append({"Container": _ALL_INSTALLED, "Name": "All Installed Types",
                               "Description": "Shared container holding all properties from Installed-aspect views.",
                               "Used For": "node"})

    # Stats
    identical_groups = sum(1 for vs in fp_to_views.values() if len(vs) > 1)
    views_in_groups  = sum(len(vs) for vs in fp_to_views.values() if len(vs) > 1)
    print(f"  Container optimization:")
    if cfihos_container:
        print(f"    CFIHOS groups        : {len(set(cfihos_container.values()))} containers, "
              f"{len(cfihos_container)} views matched")
    print(f"    Identical-fp groups  : {identical_groups} groups covering {views_in_groups} views")
    print(f"    Subset pairs applied : {saved_subset} property reassignments")
    print(f"    Installed views pooled: {n_installed} views -> 1 container ({_ALL_INSTALLED!r})")
    print(f"    Containers before    : {len(all_items)}")
    print(f"    Containers after     : {len(referenced)}")
    print(f"    Reduction            : {len(all_items) - len(referenced)} "
          f"({100*(len(all_items)-len(referenced))//len(all_items)}%)")

    return new_properties, new_containers


# ---------------------------------------------------------------------------
# Container name deduplication (case-insensitive)
# ---------------------------------------------------------------------------
def _deduplicate_containers(
    properties: list[dict], containers: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Merge containers whose externalIds are identical when lowercased.

    CDF rejects deployments that contain containers whose names differ only in
    casing (e.g. ``WaveLevelTransmitterPlant`` vs ``WaveleveltransmitterPlant``).
    This happens when the CFIHOS hierarchy optimizer names a shared container
    from a lowercased CFIHOS class name, while the corresponding IMF view key
    was generated from a properly-cased label.

    Strategy: for each case-insensitive collision group, pick the name with the
    most uppercase letters (the most correctly-CamelCased variant) as canonical,
    then remap every other name in both the containers list and the Container
    column of the properties list.
    """
    from collections import defaultdict

    # Group actual container names by their lowercased form
    name_groups: dict[str, list[str]] = defaultdict(list)
    for c in containers:
        cname = c.get("Container", "")
        if cname:
            name_groups[cname.lower()].append(cname)

    # Build remap: non-canonical → canonical
    remap: dict[str, str] = {}
    for lower_name, names in name_groups.items():
        unique = list(dict.fromkeys(names))  # preserve first-seen order, deduplicate
        if len(unique) <= 1:
            continue
        # Canonical = the variant with the most uppercase letters (best CamelCase)
        canonical = max(unique, key=lambda n: sum(1 for ch in n if ch.isupper()))
        for name in unique:
            if name != canonical:
                remap[name] = canonical

    if not remap:
        return properties, containers

    print(f"  Deduplicating {len(remap)} case-conflicting container name(s):")
    for old, new in remap.items():
        print(f"    {old!r} -> {new!r}")

    # Remap containers list — drop non-canonical duplicates
    seen: set[str] = set()
    new_containers: list[dict] = []
    for c in containers:
        canonical_name = remap.get(c.get("Container", ""), c.get("Container", ""))
        if canonical_name not in seen:
            seen.add(canonical_name)
            entry = dict(c)
            entry["Container"] = canonical_name
            new_containers.append(entry)

    # Remap Container column in every property row
    new_properties: list[dict] = []
    for p in properties:
        row = dict(p)
        if row.get("Container") in remap:
            row["Container"] = remap[row["Container"]]
        new_properties.append(row)

    return new_properties, new_containers


# ---------------------------------------------------------------------------
# View name deduplication (case-insensitive)
# ---------------------------------------------------------------------------
def _deduplicate_views(
    properties: list[dict], views: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Merge views whose externalIds are identical when lowercased.

    CDF rejects deployments with view externalIds that differ only in casing.
    This can happen when the ontology contains two block/terminal types whose
    labels are the same words but with different capitalisation (e.g.
    'Wave Level Transmitter' vs 'wave level transmitter').

    Strategy: for each case-insensitive group, keep the most-CamelCased name
    as canonical; remap the View column and (for relationship properties) the
    Value Type column in the properties list.  The duplicate view entry is
    dropped, and its properties are re-attributed to the canonical view.
    """
    from collections import defaultdict

    name_groups: dict[str, list[str]] = defaultdict(list)
    for v in views:
        vname = v.get("View", "")
        if vname:
            name_groups[vname.lower()].append(vname)

    remap: dict[str, str] = {}
    for lower_name, names in name_groups.items():
        unique = list(dict.fromkeys(names))
        if len(unique) <= 1:
            continue
        canonical = max(unique, key=lambda n: sum(1 for ch in n if ch.isupper()))
        for name in unique:
            if name != canonical:
                remap[name] = canonical

    if not remap:
        return properties, views

    print(f"  Deduplicating {len(remap)} case-conflicting view name(s):")
    for old, new in remap.items():
        print(f"    {old!r} -> {new!r}")

    # Drop non-canonical duplicate views
    seen: set[str] = set()
    new_views: list[dict] = []
    for v in views:
        canonical_name = remap.get(v.get("View", ""), v.get("View", ""))
        if canonical_name not in seen:
            seen.add(canonical_name)
            entry = dict(v)
            entry["View"] = canonical_name
            new_views.append(entry)

    # Remap View column and Value Type column (relationship targets) in properties
    new_properties: list[dict] = []
    for p in properties:
        row = dict(p)
        if row.get("View") in remap:
            row["View"] = remap[row["View"]]
        # Relationships reference other views via Value Type
        if row.get("Value Type") in remap:
            row["Value Type"] = remap[row["Value Type"]]
        # Also fix the Container column if it was named after the view
        if row.get("Container") in remap:
            row["Container"] = remap[row["Container"]]
        new_properties.append(row)

    return new_properties, new_views


# ---------------------------------------------------------------------------
# Container-property normalization
# ---------------------------------------------------------------------------
_CONTAINER_COLS = (
    "Value Type", "Min Count", "Max Count", "Connection",
    "Container Property Description", "Container Property Name",
    "Default", "Index", "Constraint", "Auto Increment",
)

def _normalize_container_properties(properties: list[dict]) -> list[dict]:
    """Ensure every (Container, Container Property) pair has identical container-level
    column values across all rows.

    NEAT validation requires that when multiple views map to the same container
    property (which happens after container optimization), the container-level
    metadata (Value Type, Min/Max Count, Container Property Description, etc.)
    must be identical in every row.  We resolve conflicts by:

    * Keeping the first non-None value seen for each column.
    * For ``Container Property Description`` we use a short generic text built
      from the container-property name so it is always consistent.
    """
    from collections import defaultdict

    # First pass: collect canonical values per (container, container_property)
    canonical: dict[tuple, dict] = {}
    for row in properties:
        key = (row.get("Container"), row.get("Container Property"))
        if key[0] is None or key[1] is None:
            continue
        if key not in canonical:
            canonical[key] = {col: row.get(col) for col in _CONTAINER_COLS}
        else:
            # Fill in any column that is still None with the first non-None value
            for col in _CONTAINER_COLS:
                if canonical[key][col] is None and row.get(col) is not None:
                    canonical[key][col] = row[col]

    # Normalize Container Property Description: strip view-specific content
    # so it is the same regardless of which view contributed the row.
    for key, vals in canonical.items():
        container, prop = key
        # Build a stable, generic description from the container + property names.
        vals["Container Property Description"] = (
            f"{prop} property of container {container}"
        )

    # Second pass: apply canonical values to every row
    conflicts = 0
    for row in properties:
        key = (row.get("Container"), row.get("Container Property"))
        if key not in canonical:
            continue
        for col in _CONTAINER_COLS:
            canon_val = canonical[key][col]
            if row.get(col) != canon_val:
                conflicts += 1
                row[col] = canon_val

    if conflicts:
        print(f"  Normalized {conflicts} inconsistent container-property column values.")

    return properties


# ---------------------------------------------------------------------------
# Core build function — returns a Python dict (no file I/O)
# ---------------------------------------------------------------------------
def build_neat_dict(
    g: Graph,
    *,
    space: str = "imf_space",
    external_id: str = "IMFDataModel",
    version: str = "v1",
    name: str | None = None,
    description: str | None = None,
    creator: str = "NEAT",
    optimize: bool = False,
) -> dict[str, Any]:
    """Build and return a NEAT physical data-model dict from an RDF graph.

    The returned dict is compatible with
    ``UnverifiedPhysicalDataModel.load(raw_data)`` (legacy NEAT plugin API).
    """
    print("Extracting BlockTypes...")
    blocks = extract_block_types(g)
    print(f"  Found {len(blocks)} unique BlockTypes")
    print("Extracting TerminalTypes...")
    terminals = extract_terminal_types(g)
    print(f"  Found {len(terminals)} unique TerminalTypes")
    print("Extracting AttributeTypes...")
    attributes = extract_attribute_types(g)
    print(f"  Found {len(attributes)} AttributeTypes")

    uri_to_view_key = {info["uri"]: key for key, info in {**blocks, **terminals}.items()}

    views_with_properties: set[str] = set()
    properties: list[dict] = []
    seen_props: set[tuple] = set()

    print("Extracting SHACL properties...")
    for i, block_uri in enumerate(g.subjects(RDF.type, IMF.BlockType), 1):
        label     = get_label(g, block_uri)
        view_name = uri_to_view_key.get(
            str(block_uri),
            clean_view_name(str(label)) if label else clean_view_name(get_local_name(block_uri)),
        )
        for prop in extract_shacl_properties(g, block_uri):
            path_uri = prop["path"]
            if path_uri in attributes:
                attr = attributes[path_uri]
                prop_key = (view_name, attr["label"])
                if prop_key not in seen_props:
                    seen_props.add(prop_key)
                    prop_name  = clean_property_name(attr["label"])
                    value_type = resolve_value_type(attr["predicate"] or path_uri,
                                                    label=attr["label"])
                    base_desc  = clean_text(attr["definition"]) or clean_text(attr["label"])
                    properties.append({
                        "View":             view_name,
                        "View Property":    prop_name,
                        "Name":             clean_text(attr["label"]),
                        "Description":      f"{base_desc} [URI: {attr['uri']}]",
                        "Value Type":       value_type,
                        "Min Count":        prop["minCount"],
                        "Max Count":        prop["maxCount"] if prop["maxCount"] else 1,
                        "Connection":       None,
                        "Container":        view_name,
                        "Container Property": prop_name,
                        "Container Property Description": build_container_prop_desc(
                            prop_name, attr["uri"], attr["predicate"] or path_uri,
                            attr["definition"]),
                    })
                    views_with_properties.add(view_name)
            elif "hasTerminal" in path_uri or "hasPart" in path_uri:
                rel_type = "hasTerminal" if "hasTerminal" in path_uri else "hasPart"
                target   = prop["targetClass"]
                if target:
                    target_label = next(
                        (k for k, b in {**blocks, **terminals}.items() if b["uri"] == target), None)
                    if target_label:
                        prop_key = (view_name, f"{rel_type}{target_label}")
                        if prop_key not in seen_props:
                            seen_props.add(prop_key)
                            rel_name = f"{rel_type.lower()}{target_label}"
                            properties.append({
                                "View":             view_name,
                                "View Property":    rel_name,
                                "Name":             f"{rel_type} to {target_label}",
                                "Description":      f"Relationship from {view_name} to {target_label}",
                                "Connection":       f"direct(source={view_name},target={target_label})",
                                "Value Type":       target_label,
                                "Min Count":        prop["minCount"],
                                "Max Count":        prop["maxCount"] if prop["maxCount"] else 1,
                                "Container":        view_name,
                                "Container Property": rel_name,
                                "Container Property Description": build_container_prop_desc(
                                    rel_name, path_uri, None, None),
                            })
                            views_with_properties.add(view_name)
        if i % 100 == 0:
            print(f"  Processed {i} blocks, {len(properties)} properties so far...")

    print(f"  Total properties extracted: {len(properties)}")
    print("Adding default properties for views without SHACL properties...")
    for collection in (blocks, terminals):
        for key, item in collection.items():
            if key not in views_with_properties:
                for pname, pdesc in [("name", "Name"), ("description", "Description")]:
                    properties.append({
                        "View":             key, "View Property": pname, "Name": pname,
                        "Description":      f"{pdesc} of the {clean_text(item['label'])}",
                        "Value Type":       "text", "Min Count": 0, "Max Count": 1,
                        "Connection":       None, "Container": key,
                        "Container Property": pname,
                        "Container Property Description": build_container_prop_desc(
                            pname, item["uri"], None, None),
                    })
                views_with_properties.add(key)

    views = []
    for key, item in {**blocks, **terminals}.items():
        type_label   = "BlockType" if key in blocks else "TerminalType"
        base_desc    = clean_text(item["definition"]) or f"IMF {type_label}: {clean_text(item['label'])}"
        aspect       = item.get("aspect")
        display_name = f"{clean_text(item['label'])} - {aspect}" if aspect else clean_text(item["label"])
        views.append({"View": key, "Name": display_name,
                      "Description": f"{base_desc} [URI: {item['uri']}]"})

    containers = []
    for key, item in {**blocks, **terminals}.items():
        aspect       = item.get("aspect")
        display_name = f"{clean_text(item['label'])} - {aspect}" if aspect else clean_text(item["label"])
        base_desc    = clean_text(item["definition"]) or f"Container for {display_name}"
        containers.append({"Container": key, "Name": display_name,
                           "Description": f"{base_desc} [URI: {item['uri']}]",
                           "Used For": "node"})

    if optimize:
        print("Optimizing containers...")
        properties, containers = optimize_containers(properties, {**blocks, **terminals})

    properties, containers = _deduplicate_containers(properties, containers)
    properties, views      = _deduplicate_views(properties, views)
    properties = _normalize_container_properties(properties)

    metadata = [
        {"Key": "space",       "Value": space},
        {"Key": "externalId",  "Value": external_id},
        {"Key": "version",     "Value": version},
        {"Key": "name",        "Value": name or external_id.replace("_", " ").title()},
        {"Key": "description", "Value": description or "IMF-based data model"},
        {"Key": "creator",     "Value": creator},
    ]

    print(f"\n{'='*60}")
    print("NEAT Data Model Build Complete!")
    print(f"{'='*60}")
    print(f"  BlockTypes  (Views)       : {len(blocks)}")
    print(f"  TerminalTypes (Views)     : {len(terminals)}")
    print(f"  Total Views               : {len(views)}")
    print(f"  AttributeTypes            : {len(attributes)}")
    print(f"  Properties (deduplicated) : {len(properties)}")
    print(f"  Containers                : {len(containers)}" + (" (optimized)" if optimize else ""))

    return {
        "Metadata":   metadata,
        "Properties": properties,
        "Views":      views,
        "Containers": containers,
    }


# ---------------------------------------------------------------------------
# YAML writer — used by the standalone CLI only
# ---------------------------------------------------------------------------
def write_neat_yaml(neat_dict: dict[str, Any], output_path: str | Path) -> None:
    """Write the dict returned by build_neat_dict() to a NEAT YAML file."""
    import yaml

    out = OrderedDict([
        ("Metadata",   neat_dict["Metadata"]),
        ("Properties", neat_dict["Properties"]),
        ("Views",      neat_dict["Views"]),
        ("Containers", neat_dict["Containers"]),
    ])

    def represent_ordereddict(dumper, data):
        return dumper.represent_mapping("tag:yaml.org,2002:map", data.items())
    yaml.add_representer(OrderedDict, represent_ordereddict)

    print(f"\nWriting YAML to: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(dict(out), f, default_flow_style=False, allow_unicode=True,
                  sort_keys=False, width=10 ** 9)
    print(f"Output: {output_path}")
