"""IMFToNeatPlugin — NEAT v1 PhysicalDataModelReaderPlugin.

Registered under entry-point key ``"imf"`` so it is callable as::

    neat.physical_data_model.read.imf(
        io="path/to/imf_ontology.ttl",
        clean=True,
        optimize_containers=True,
        space="my_space",
        external_id="MyIMFModel",
        version="v1",
    )

Multiple TTL files can be passed as a list::

    neat.physical_data_model.read.imf(
        io=["file_a.ttl", "file_b.ttl"],
        clean=True,
    )

Prerequisites
-------------
Install the plugin (once)::

    pip install -e /path/to/imf-to-neat-plugin

Then in your NeatSession setup::

    config.alpha.enable_plugins = True
"""
from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from cognite.neat._plugin_adapter import DMSImporter, PhysicalDataModelReaderPlugin

from . import _core


class IMFToNeatPlugin(PhysicalDataModelReaderPlugin):
    """Plugin that converts IMF Turtle ontologies to a NEAT physical data model.

    After installation and enabling plugins, call via::

        neat.physical_data_model.read.imf(io="file.ttl", clean=True)
    """

    method_name: ClassVar[str] = "imf"

    def configure(
        self,
        io: str | Path | list[str | Path] | None = None,
        clean: bool = True,
        cfihos_csv: str | Path | None = None,
        cfihos_url: str | None = None,
        cfihos_cache: str | Path | None = None,
        rds_cache: str | Path | None = None,
        cdd_lookup: str | Path | None = None,
        optimize_containers: bool = False,
        space: str = "imf_space",
        external_id: str = "IMFDataModel",
        version: str = "v1",
        name: str | None = None,
        description: str | None = None,
        creator: str = "POSC Caesar Association",
    ) -> DMSImporter:
        """Convert one or more IMF Turtle ontology files to a NEAT physical data model.

        Parameters
        ----------
        io : str | Path | list[str | Path]
            Path to a single ``.ttl`` file, or a list of paths to merge into one
            graph before conversion.  Example::

                io=r"C:/neat/imf_ontology.ttl"
                io=[r"C:/neat/part_a.ttl", r"C:/neat/part_b.ttl"]

        clean : bool, default True
            Remove deprecated / replaced terms before conversion.  Subjects
            referenced by ``dcterms:replaces`` triples are stripped from the graph
            along with their associated SHACL shapes.

        cfihos_csv : str | Path | None, default None
            Path to a local ``CFIHOS CORE property v2.0.csv`` file.  When
            provided, CFIHOS property value types are resolved from this file
            instead of downloading from the internet.  Example::

                cfihos_csv=r"C:/neat/CFIHOS CORE property v2.0.csv"

        cfihos_url : str | None, default None
            Override URL for the online CFIHOS property data ZIP.  Defaults to
            the official JIP36 release URL when *cfihos_csv* is not supplied.

        cfihos_cache : str | Path | None, default None
            Path to a JSON file used to cache the downloaded CFIHOS data so
            subsequent runs skip the network request.

        rds_cache : str | Path | None, default None
            Path to a JSON cache of the POSC Caesar RDS numeric-attribute set.
            When present, the SPARQL endpoint is not queried.  Generate with
            ``--rds-sparql`` on the CLI.

        cdd_lookup : str | Path | None, default None
            Path to an enriched ``iec_cdd_references.csv`` containing a
            ``Value type`` column.  Used to resolve value types for IEC CDD
            predicate references.

        optimize_containers : bool, default False
            Apply multi-level container optimisation:

            * **CFIHOS-group containers** — group Plant/Product views by their
              CFIHOS tag/equipment class hierarchy into shared containers
              (e.g. all flow transmitters share ``FlowTransmitterPlant``).
            * **Identical-fingerprint merging** — views with exactly the same
              property set share one container.
            * **Subset/inheritance detection** — views whose properties are a
              superset of another view's properties (e.g. CentrifugalPump ⊇ Pump)
              inherit the parent container.
            * **AllInstalled pooling** — all Installed-aspect views share a
              single ``AllInstalled`` container.

            Output file name gets an ``_optimized`` suffix when using the CLI.

        space : str, default "imf_space"
            CDF space name for the generated data model.

        external_id : str, default "IMFDataModel"
            External ID of the generated data model.

        version : str, default "v1"
            Version string for the generated data model.

        name : str | None, default None
            Human-readable display name.  Defaults to a title-cased version of
            *external_id*.

        description : str | None, default None
            Description of the generated data model.

        creator : str, default "POSC Caesar Association"
            Creator / author string stored in the model metadata.
        """
        from cognite.neat._data_model.importers import DMSTableImporter

        if io is None:
            raise ValueError("'io' is required: provide a path (or list of paths) to .ttl files.")

        if isinstance(io, (str, Path)):
            filepaths = [Path(io)]
        else:
            filepaths = [Path(p) for p in io]

        missing = [p for p in filepaths if not p.exists()]
        if missing:
            raise FileNotFoundError(f"File(s) not found: {', '.join(str(p) for p in missing)}")

        # ── CFIHOS value-type data ───────────────────────────────────────────
        if cfihos_csv:
            csv_path = Path(cfihos_csv)
            if csv_path.exists():
                _core.load_cfihos_csv(csv_path)
                print(f"Loaded {len(_core._cfihos_type_map)} CFIHOS types from: {csv_path}")
            else:
                _core.load_cfihos_online(url=cfihos_url or _core._CFIHOS_ZIP_URL,
                                         cache_file=Path(cfihos_cache) if cfihos_cache else None)
        else:
            _core.load_cfihos_online(url=cfihos_url or _core._CFIHOS_ZIP_URL,
                                     cache_file=Path(cfihos_cache) if cfihos_cache else None)

        # ── RDS numeric set ──────────────────────────────────────────────────
        if rds_cache:
            rds_path = Path(rds_cache)
            if rds_path.exists():
                _core.load_rds_numeric_set(rds_path)

        # ── IEC CDD lookup ───────────────────────────────────────────────────
        if cdd_lookup:
            cdd_path = Path(cdd_lookup)
            if cdd_path.exists():
                _core.load_cdd_lookup(cdd_path)

        # ── Parse + clean graph ──────────────────────────────────────────────
        g = _core.load_ontology(*filepaths)
        print(f"Loaded {len(g)} triples from {len(filepaths)} file(s)")

        if clean:
            n_replaced, n_shapes = _core.remove_replaced_terms(g)
            if n_replaced:
                print(f"Cleaned graph: removed {n_replaced} replaced subjects, {n_shapes} SHACL shapes")

        # ── Build table dict ─────────────────────────────────────────────────
        tables = _core.build_neat_dict(
            g,
            space=space,
            external_id=external_id,
            version=version,
            name=name or external_id.replace("_", " ").title(),
            description=description or f"IMF-based data model generated from {filepaths[0].name}",
            creator=creator,
            optimize=optimize_containers,
        )

        return DMSTableImporter(tables)
