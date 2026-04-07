"""IMFToNeatImporter — BaseImporter[UnverifiedPhysicalDataModel]."""
from __future__ import annotations

from pathlib import Path

from rdflib import Graph

from cognite.neat.core._data_model._shared import ImportedDataModel
from cognite.neat.core._data_model.importers._base import BaseImporter
from cognite.neat.core._data_model.models.physical._unverified import UnverifiedPhysicalDataModel
from cognite.neat.core._issues import IssueList
from cognite.neat.core._issues.errors import FileReadError

from . import _core


class IMFToNeatImporter(BaseImporter[UnverifiedPhysicalDataModel]):
    """Convert one or more IMF Turtle ontology files to a NEAT physical data model.

    Workflow
    --------
    1. Parse all TTL files into a single merged RDFlib Graph.
    2. Optionally remove deprecated terms (``dcterms:replaces``).
    3. Optionally load CFIHOS / RDS / IEC-CDD reference data for value-type resolution.
    4. Build the ``UnverifiedPhysicalDataModel`` and return it.

    Usage via the NEAT plugin system
    ---------------------------------
    After installing the plugin (``pip install -e imf-to-neat-plugin/``)::

        from cognite.neat import NeatSession
        neat = NeatSession()
        neat.plugin.data_model.read(
            "imf",
            io="path/to/imf_ontology.ttl",
            clean=True,
            optimize_containers=True,
        )
    """

    def __init__(
        self,
        issue_list: IssueList,
        graph: Graph,
        *,
        space: str = "imf_space",
        external_id: str = "IMFDataModel",
        version: str = "v1",
        name: str | None = None,
        description: str | None = None,
        creator: str = "NEAT",
        optimize_containers: bool = False,
    ) -> None:
        self.issue_list        = issue_list
        self.graph             = graph
        self.space             = space
        self.external_id       = external_id
        self.version           = version
        self.name              = name
        self.description       = description
        self.creator           = creator
        self.optimize_containers = optimize_containers

    @property
    def description(self) -> str:  # type: ignore[override]
        return "IMF ontology importer (physical data model)"

    @classmethod
    def from_files(
        cls,
        filepaths: list[Path],
        *,
        clean: bool = True,
        cfihos_csv: Path | None = None,
        cfihos_url: str = _core._CFIHOS_ZIP_URL,
        cfihos_cache: Path | None = None,
        rds_cache: Path | None = None,
        cdd_lookup: Path | None = None,
        optimize_containers: bool = False,
        space: str = "imf_space",
        external_id: str = "IMFDataModel",
        version: str = "v1",
        name: str | None = None,
        description: str | None = None,
        creator: str = "NEAT",
    ) -> "IMFToNeatImporter":
        """Create an importer from one or more Turtle files.

        Parameters
        ----------
        filepaths:
            One or more ``.ttl`` files.  Multiple files are merged into one graph.
        clean:
            Remove deprecated/replaced terms (``dcterms:replaces``) before
            processing (default: ``True``).
        cfihos_csv:
            Path to a local ``CFIHOS CORE property v2.0.csv``.  When present
            it is preferred over the online source.
        cfihos_url:
            URL of the CFIHOS property data ZIP or Excel file.  Used only
            when *cfihos_csv* is absent and no cache exists.
        cfihos_cache:
            JSON cache file for the online CFIHOS data (avoids re-downloading).
        rds_cache:
            JSON cache for the RDS numeric-attribute set.  When present the
            SPARQL endpoint is not queried.
        cdd_lookup:
            Path to an enriched ``iec_cdd_references.csv`` with a
            ``Value type`` column.
        optimize_containers:
            Apply container-optimization strategies (CFIHOS groups, identical-
            fingerprint merging, subset detection, AllInstalled pooling).
        space / external_id / version / name / description / creator:
            Metadata overrides for the generated data model.
        """
        issue_list = IssueList(title="IMFToNeatImporter")

        # Load graph
        graph = Graph()
        for fp in filepaths:
            try:
                graph.parse(str(fp), format="turtle")
            except Exception as exc:
                issue_list.append(FileReadError(fp, str(exc)))

        if issue_list.has_errors:
            return cls(issue_list, graph, space=space, external_id=external_id,
                       version=version, name=name, description=description,
                       creator=creator, optimize_containers=optimize_containers)

        # Optional cleaning
        if clean:
            n_replaced, n_shapes = _core.remove_replaced_terms(graph)
            if n_replaced:
                print(f"Cleaned graph: removed {n_replaced} replaced subjects, "
                      f"{n_shapes} SHACL shapes")

        # CFIHOS value-type data
        if cfihos_csv and cfihos_csv.exists():
            _core.load_cfihos_csv(cfihos_csv)
            print(f"Loaded {len(_core._cfihos_type_map)} CFIHOS types from: {cfihos_csv}")
        else:
            _core.load_cfihos_online(url=cfihos_url, cache_file=cfihos_cache)

        # RDS numeric set
        if rds_cache and rds_cache.exists():
            _core.load_rds_numeric_set(rds_cache)

        # IEC CDD lookup
        if cdd_lookup and cdd_lookup.exists():
            _core.load_cdd_lookup(cdd_lookup)

        return cls(
            issue_list, graph,
            space=space, external_id=external_id, version=version,
            name=name, description=description, creator=creator,
            optimize_containers=optimize_containers,
        )

    def to_data_model(self) -> ImportedDataModel[UnverifiedPhysicalDataModel]:
        """Build and return the physical data model."""
        if self.issue_list.has_errors:
            self.issue_list.trigger_warnings()
            from cognite.neat.core._issues import MultiValueError
            raise MultiValueError(self.issue_list.errors)

        raw = _core.build_neat_dict(
            self.graph,
            space=self.space,
            external_id=self.external_id,
            version=self.version,
            name=self.name,
            description=self.description,
            creator=self.creator,
            optimize=self.optimize_containers,
        )

        data_model = UnverifiedPhysicalDataModel.load(raw)
        self.issue_list.trigger_warnings()
        return ImportedDataModel(data_model, {})
