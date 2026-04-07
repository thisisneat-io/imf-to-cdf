"""imf_to_neat — NEAT v1 plugin for IMF Turtle ontology import.

After installing this package, enable plugins in your NeatSession::

    config.alpha.enable_plugins = True
    neat = NeatSession(client, config)

Then read IMF ontology files directly::

    neat.physical_data_model.read.imf(
        io="path/to/imf_ontology.ttl",
        clean=True,
        optimize_containers=True,
    )

``IMFToNeatPlugin`` is imported lazily so the CLI (``imf-to-neat``)
works even without the full cognite-neat SDK installed.
"""
from __future__ import annotations

__all__ = ["IMFToNeatPlugin"]


def __getattr__(name: str):
    if name == "IMFToNeatPlugin":
        from .plugin import IMFToNeatPlugin
        return IMFToNeatPlugin
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
