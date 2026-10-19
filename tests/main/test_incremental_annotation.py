import ast
from pathlib import Path

import dlt

from omniload.source.dynamodb.adapter import dynamodb_table


class _Table:
    def __init__(self, items):
        self.items = items
        self.scan_args = []

    def scan(self, **kwargs):
        self.scan_args.append(kwargs)
        return {"Items": self.items}


def test_dynamodb_resource_binds_an_incremental(tmp_path):
    table = _Table([{"id": 1, "updated_at": 1}, {"id": 2, "updated_at": 2}])
    resource = dlt.resource(dynamodb_table, name="items", primary_key="id")
    pipeline = dlt.pipeline(
        pipeline_name="dynamodb_incremental",
        destination=dlt.destinations.duckdb(str(tmp_path / "load.duckdb")),
        pipelines_dir=str(tmp_path),
        dataset_name="data",
    )

    info = pipeline.run(resource(table, dlt.sources.incremental("updated_at")))

    assert not info.has_failed_jobs
    assert table.scan_args == [{}]


def _is_bare_incremental(node):
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "incremental"
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "sources"
    )


def test_project_source_parameterizes_incremental_annotations():
    """dlt 1.31 fails to bind an incremental to a parameter annotated as bare
    `dlt.sources.incremental`, so every annotation carries a type argument."""
    source_root = Path(__file__).parents[2] / "src" / "omniload"
    matches = []
    for path in source_root.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                not isinstance(node, (ast.arg, ast.AnnAssign))
                or node.annotation is None
            ):
                continue
            if any(_is_bare_incremental(sub) for sub in ast.walk(node.annotation)):
                if not any(
                    isinstance(sub, ast.Subscript) and _is_bare_incremental(sub.value)
                    for sub in ast.walk(node.annotation)
                ):
                    matches.append(f"{path.relative_to(source_root)}:{node.lineno}")

    assert matches == []
