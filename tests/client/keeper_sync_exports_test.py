"""Lock the keeper-sync handler response surface to ``__all__``.

Every Pydantic model used as ``response_model=`` in
``docverse.handlers.orgs.keeper_sync.router`` — and every Pydantic
model or :class:`enum.Enum` reachable from those models' field
annotations — must be importable from :mod:`docverse.client.models`.
Handler-side subclasses (in ``handlers/orgs/keeper_sync_models.py``
and ``handlers/orgs/job_models.py``) are accepted because their client-
package base class is exported; the handler subclasses themselves
must not be re-exported.

The test also confirms ``__all__`` is sorted so additions do not
silently regress lexical ordering.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, get_args, get_origin

from pydantic import BaseModel

import docverse.client.models as client_models
from docverse.handlers.orgs import job_models as queue_handler_models
from docverse.handlers.orgs import keeper_sync as keeper_sync_handlers
from docverse.handlers.orgs import keeper_sync_models as handler_models


def _exported_classes() -> set[type]:
    out: set[type] = set()
    for name in client_models.__all__:
        obj = getattr(client_models, name)
        if isinstance(obj, type):
            out.add(obj)
    return out


def _is_pydantic_or_enum(t: Any) -> bool:
    if not isinstance(t, type):
        return False
    return issubclass(t, BaseModel) or issubclass(t, Enum)


def _walk_type(t: Any, seen: set[int], acc: set[type]) -> None:
    """Collect every Pydantic model / Enum reachable from ``t``."""
    if id(t) in seen:
        return
    seen.add(id(t))
    origin = get_origin(t)
    if origin is not None:
        for arg in get_args(t):
            _walk_type(arg, seen, acc)
        return
    if _is_pydantic_or_enum(t):
        acc.add(t)
        if issubclass(t, BaseModel):
            for field in t.model_fields.values():
                _walk_type(field.annotation, seen, acc)


def _collect_route_response_models() -> list[type[BaseModel]]:
    out: list[type[BaseModel]] = []
    for route in keeper_sync_handlers.router.routes:
        response_model = getattr(route, "response_model", None)
        if response_model is None:
            continue
        origin = get_origin(response_model)
        if origin is not None:
            out.extend(
                arg
                for arg in get_args(response_model)
                if isinstance(arg, type) and issubclass(arg, BaseModel)
            )
        elif isinstance(response_model, type) and issubclass(
            response_model, BaseModel
        ):
            out.append(response_model)
    return out


def _reachable_via_export(t: type, exported: set[type]) -> bool:
    if t in exported:
        return True
    if issubclass(t, BaseModel):
        return any(base in exported for base in t.__mro__[1:])
    return False


def test_all_is_sorted() -> None:
    """``__all__`` is sorted so future additions do not regress order."""
    assert list(client_models.__all__) == sorted(client_models.__all__)


def test_every_response_model_is_in_client_export_surface() -> None:
    """Every ``response_model`` and nested model/enum is exported."""
    exported = _exported_classes()
    reachable: set[type] = set()
    for model in _collect_route_response_models():
        _walk_type(model, seen=set(), acc=reachable)

    assert reachable, (
        "expected the handler module to expose some response_models"
    )

    missing = sorted(
        f"{t.__module__}.{t.__qualname__}"
        for t in reachable
        if not _reachable_via_export(t, exported)
    )
    assert missing == [], (
        "These types are referenced (directly or transitively) by"
        " ``handlers/orgs/keeper_sync.py`` response models but are not"
        " importable from ``docverse.client.models``:\n  - "
        + "\n  - ".join(missing)
    )


def test_handler_side_subclasses_are_not_exported() -> None:
    """Handler-side subclasses must not be re-exposed from the client.

    The handler subclasses add request-bound URL minting and must stay
    in the server-only handler tree; exporting them from the client
    package would leak the server's URL builders into downstream
    consumers.
    """
    exported_names = set(client_models.__all__)
    # ``KeeperSyncEditionStatus`` and ``KeeperSyncRun`` no longer have
    # handler-side subclasses: their handlers use the client model
    # directly as ``response_model`` and build it via module-level
    # ``*_from_domain`` functions, so only genuinely-distinct subclasses
    # remain here.
    handler_subclasses = {
        handler_models.KeeperSyncProjectRefreshAccepted,
        handler_models.KeeperSyncProjectStatus,
        handler_models.KeeperSyncRunCreated,
        queue_handler_models.QueueJob,
    }
    for cls in handler_subclasses:
        exported = getattr(client_models, cls.__name__, None)
        assert cls.__name__ in exported_names, (
            f"expected base class {cls.__name__!r} to be exported"
        )
        assert exported is not cls, (
            f"{cls.__module__}.{cls.__name__} is a handler-side"
            " subclass; ``docverse.client.models`` must export the"
            " client-package base, not the handler subclass."
        )
