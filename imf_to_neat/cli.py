"""CLI entry point for the imf-to-neat plugin.

After installing the plugin (``pip install -e .``), run::

    imf-to-neat file.ttl --clean --optimize-containers
    imf-to-neat a.ttl b.ttl -o merged.yaml

This module re-uses all processing logic from ``_core`` and adds only the
argument-parsing and file-I/O glue that the standalone script needs.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import re

from . import _core


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="imf-to-neat",
        description="Convert one or more PCA IMF Turtle ontologies to NEAT YAML format",
    )
    parser.add_argument("ttl_files", nargs="+", metavar="ttl_file",
                        help="Turtle (.ttl) files to merge and convert")
    parser.add_argument("-o", "--output", help="Output YAML path (defaults to <input>.yaml)")
    parser.add_argument("--clean", action="store_true",
                        help="Remove deprecated/replaced terms before conversion")
    parser.add_argument("--save-cleaned", metavar="PATH",
                        help="Save the cleaned graph to this TTL path")
    parser.add_argument("--cfihos-csv",
                        default=str(Path(__file__).parent.parent / "CFIHOS CORE property v2.0.csv"),
                        help="Path to local CFIHOS property CSV")
    parser.add_argument("--cfihos-url", default=_core._CFIHOS_ZIP_URL,
                        help="URL of the CFIHOS property data ZIP or Excel")
    parser.add_argument("--cfihos-cache",
                        default=str(Path(__file__).parent.parent / "cfihos_online_cache.json"),
                        help="JSON cache for online CFIHOS data")
    parser.add_argument("--refresh-cfihos", action="store_true",
                        help="Force re-download of CFIHOS data")
    parser.add_argument("--rds-sparql", action="store_true",
                        help="Fetch RDS numeric list from PCA SPARQL endpoint")
    parser.add_argument("--rds-cache",
                        default=str(Path(__file__).parent.parent / "rds_numeric_cache.json"),
                        help="JSON cache for RDS numeric attribute set")
    parser.add_argument("--cdd-lookup",
                        default=str(Path(__file__).parent.parent / "iec_cdd_references.csv"),
                        help="Enriched IEC CDD reference CSV with 'Value type' column")
    parser.add_argument("--optimize-containers", action="store_true",
                        help="Reduce container count via CFIHOS grouping, identical-fp merging, "
                             "subset detection and AllInstalled pooling")
    parser.add_argument("--cfihos-hierarchy-dir", metavar="DIR",
                        help="Override directory for CFIHOS hierarchy CSVs "
                             "(auto-loaded from --cfihos-csv directory by default)")
    # Metadata overrides
    parser.add_argument("--space",       help="Override: CDF space name")
    parser.add_argument("--external-id", help="Override: model externalId")
    parser.add_argument("--model-name",  help="Override: model display name")
    parser.add_argument("--description", help="Override: model description")
    parser.add_argument("--creator",     help="Override: creator")

    args = parser.parse_args(argv)

    ttl_paths = [Path(p) for p in args.ttl_files]
    missing = [p for p in ttl_paths if not p.exists()]
    if missing:
        for p in missing:
            print(f"Error: file not found: {p}", file=sys.stderr)
        sys.exit(1)

    # Determine output path
    if args.output:
        yaml_path = Path(args.output)
    elif len(ttl_paths) == 1:
        stem = ttl_paths[0].stem
        if args.optimize_containers:
            stem += "_optimized"
        yaml_path = ttl_paths[0].with_name(stem + ".yaml")
    else:
        print("Error: -o/--output is required when multiple TTL files are given.", file=sys.stderr)
        sys.exit(1)

    # CFIHOS
    csv_path = Path(args.cfihos_csv)
    if csv_path.exists() and not args.refresh_cfihos:
        _core.load_cfihos_csv(csv_path)
        print(f"Loaded {len(_core._cfihos_type_map)} CFIHOS types from: {csv_path}")
    else:
        if not csv_path.exists():
            print(f"Local CFIHOS CSV not found — trying online: {args.cfihos_url}")
        _core.load_cfihos_online(url=args.cfihos_url, cache_file=Path(args.cfihos_cache),
                                 force=args.refresh_cfihos)

    # RDS
    rds_cache = Path(args.rds_cache)
    if args.rds_sparql or rds_cache.exists():
        _core.load_rds_numeric_set(rds_cache)
    else:
        print("RDS numeric lookup not active (use --rds-sparql to enable).")

    # CDD
    cdd_path = Path(args.cdd_lookup)
    if cdd_path.exists():
        _core.load_cdd_lookup(cdd_path)
    else:
        print(f"IEC CDD lookup not found — heuristics used for all CDD predicates.")

    # CFIHOS hierarchy override
    if args.cfihos_hierarchy_dir:
        loaded = _core.load_cfihos_hierarchy(Path(args.cfihos_hierarchy_dir))
        print(f"CFIHOS hierarchy {'loaded' if loaded else 'NOT loaded'} "
              f"from: {args.cfihos_hierarchy_dir}")
    if _core._cfihos_tag_hier or _core._cfihos_equip_hier:
        print(f"CFIHOS hierarchy ready: {len(_core._cfihos_tag_hier)} tag classes, "
              f"{len(_core._cfihos_equip_hier)} equipment classes")
    else:
        print("CFIHOS hierarchy not loaded — CFIHOS-group optimization will be skipped.")

    # Load graph
    print(f"Loading ontology from: {ttl_paths[0]}" if len(ttl_paths) == 1
          else f"Merging {len(ttl_paths)} Turtle files:")
    for p in ttl_paths:
        if len(ttl_paths) > 1:
            print(f"  {p}")
    g = _core.load_ontology(*ttl_paths)
    print(f"Loaded {len(g)} triples")

    # Clean
    if args.clean:
        n_replaced, n_shapes = _core.remove_replaced_terms(g)
        print(f"Removed {n_replaced} replaced subjects, {n_shapes} SHACL shapes")
        if args.save_cleaned:
            g.serialize(destination=args.save_cleaned, format="turtle")
            print(f"Saved cleaned graph to: {args.save_cleaned}")

    # Metadata overrides
    stem  = yaml_path.stem
    space = re.sub(r"[^a-z0-9_]", "_", stem.lower())
    meta_overrides = {
        "space":      args.space       or space,
        "externalId": args.external_id or stem,
        "version":    "1.0.0",
        "name":       args.model_name  or stem.replace("_", " ").title(),
        "description": args.description or f"IMF-based data model generated from {yaml_path.name}",
        "creator":    args.creator     or "POSC Caesar Association",
    }

    # Build
    neat_dict = _core.build_neat_dict(
        g,
        space=meta_overrides["space"],
        external_id=meta_overrides["externalId"],
        version=meta_overrides["version"],
        name=meta_overrides["name"],
        description=meta_overrides["description"],
        creator=meta_overrides["creator"],
        optimize=args.optimize_containers,
    )

    _core.write_neat_yaml(neat_dict, yaml_path)


if __name__ == "__main__":
    main()
