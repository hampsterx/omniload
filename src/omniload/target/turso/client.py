"""dlt's sqlalchemy destination, narrowed to what Turso loads without losing rows.

Imported only when dlt builds the destination, so loading the registry stays cheap.
"""

from __future__ import annotations

from typing import Callable, List, Optional, Type

import dlt
from dlt.common.destination.exceptions import WriteDispositionNotSupported
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.utils import get_inherited_table_hint, get_root_table
from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import (
    SqlalchemyJobClient,
)


class TursoJobClient(SqlalchemyJobClient):
    """Refuse a merge table, nested ones included, before the load writes anything."""

    # The base client defines this in an `__init__` the sqlalchemy client never calls,
    # so dlt's own merge check fails with an AttributeError instead of refusing.
    allow_merge_to_append_fallback = False

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        # Only `truncate-and-insert` is offered, but dlt reports a configured
        # `insert-from-staging` through a bare `assert` in the base method, which
        # `python -O` strips.
        replace_strategy = self.config.replace_strategy
        if replace_strategy not in (None, "truncate-and-insert"):
            disposition = get_inherited_table_hint(
                self.schema.tables, table_name, "write_disposition", allow_none=True
            )
            if disposition == "replace":
                raise WriteDispositionNotSupported(
                    "replace",
                    f"Turso cannot replace with the `{replace_strategy}` strategy, "
                    "which loads through a staging dataset. Use "
                    "`truncate-and-insert`.",
                )
        return super().prepare_load_table(table_name)

    def verify_schema(self, *args, **kwargs) -> List[PreparedTableSchema]:
        tables = super().verify_schema(*args, **kwargs)
        # dlt checks root tables only, but a nested table carrying its own merge hint
        # is still loaded through a staging dataset. By then a `replace` parent has
        # been truncated, so refusing here, ahead of any DDL, is what keeps its rows.
        # The load also walks every table in a loaded table's chain that has ever held
        # data, including nested tables that got no rows this run, so those are
        # checked as well. A table that never held data is not loaded or staged.
        schema_tables = self.schema.tables
        loaded = [name for table in tables if (name := table.get("name"))]
        roots = {
            get_root_table(schema_tables, name)["name"]
            for name in loaded
            if name in schema_tables
        }
        chain = [
            self.prepare_load_table(name)
            for table in self.schema.data_tables(seen_data_only=True)
            if (name := table.get("name"))
            and get_root_table(schema_tables, name)["name"] in roots
        ]
        for table in [*tables, *chain]:
            if table.get("write_disposition") == "merge":
                raise WriteDispositionNotSupported(
                    "merge",
                    f"Turso cannot load table `{table.get('name')}` with the `merge` "
                    "write disposition. Use `append` or `replace`.",
                )
        return tables


class turso_sqlalchemy(dlt.destinations.sqlalchemy):
    """The sqlalchemy destination with a client that refuses merge tables.

    ``_creator``, when set, opens every connection. It is set on the resolved
    credentials rather than passed as engine options, because engine options a user
    configured for the sqlalchemy destination replace those, and a remote Turso
    connection that skipped it would go out without its token.
    """

    #: Set after construction, not passed to `__init__`: dlt reads the factory's
    #: configuration from its `__init__` arguments.
    _creator: Optional[Callable] = None

    @property
    def client_class(self) -> Type[SqlalchemyJobClient]:
        return TursoJobClient

    def configuration(self, initial_config, accept_partial: bool = False):
        config = super().configuration(initial_config, accept_partial=accept_partial)
        credentials = getattr(config, "credentials", None)
        if self._creator is not None and credentials is not None:
            # Keep the user's other engine options, including any still given under
            # the deprecated `engine_args`, but always connect through the creator.
            options = {
                **(credentials.engine_args or {}),
                **(credentials.engine_kwargs or {}),
            }
            credentials.engine_args = None
            credentials.engine_kwargs = {**options, "creator": self._creator}
        return config
