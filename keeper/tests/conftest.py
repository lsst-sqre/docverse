"""py.test fixtures available to all test modules without explicit import."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from keeper.appfactory import create_flask_app
from keeper.models import Permission, User, db
from keeper.testutils import TestClient

if TYPE_CHECKING:
    import flask
    from _pytest.fixtures import FixtureRequest


DEFAULT_USERNAME = "hipster"
DEFAULT_PASSWORD = "pug"

# additional usernames with specific roles
PRODUCT_ADMIN_USERNAME = "product_admin"
EDITION_ADMIN_USERNAME = "edition_admin"
BUILD_UPLOADER_USERNAME = "build_uploader"
BUILD_DEPRECATOR_USERNAME = "build_deprecator"


@pytest.fixture
def empty_app(request: FixtureRequest) -> flask.Flask:
    """An application with only a single user, but otherwise empty"""
    app = create_flask_app(profile="testing")
    ctx = app.app_context()
    ctx.push()
    db.drop_all()
    db.create_all()

    # Creates users with each of the permissions
    u = User(
        username=DEFAULT_USERNAME, permissions=Permission.full_permissions()
    )
    u.set_password(DEFAULT_PASSWORD)
    db.session.add(u)

    u = User(
        username=PRODUCT_ADMIN_USERNAME, permissions=Permission.ADMIN_PRODUCT
    )
    u.set_password(DEFAULT_PASSWORD)
    db.session.add(u)

    u = User(
        username=EDITION_ADMIN_USERNAME, permissions=Permission.ADMIN_EDITION
    )
    u.set_password(DEFAULT_PASSWORD)
    db.session.add(u)

    u = User(
        username=BUILD_UPLOADER_USERNAME, permissions=Permission.UPLOAD_BUILD
    )
    u.set_password(DEFAULT_PASSWORD)
    db.session.add(u)

    u = User(
        username=BUILD_DEPRECATOR_USERNAME,
        permissions=Permission.DEPRECATE_BUILD,
    )
    u.set_password(DEFAULT_PASSWORD)
    db.session.add(u)

    db.session.commit()

    def fin() -> None:
        db.session.remove()
        db.drop_all()
        ctx.pop()

    request.addfinalizer(fin)
    return app


@pytest.fixture
def basic_client(empty_app: flask.Flask) -> TestClient:
    """Client with username/password auth, using the `app` application."""
    client = TestClient(empty_app, DEFAULT_USERNAME, DEFAULT_PASSWORD)
    return client


@pytest.fixture
def client(empty_app: flask.Flask) -> TestClient:
    """Client with token-based auth, using the `app` application."""
    _c = TestClient(empty_app, DEFAULT_USERNAME, DEFAULT_PASSWORD)
    r = _c.get("/token")
    client = TestClient(empty_app, r.json["token"])
    return client


@pytest.fixture
def anon_client(empty_app: flask.Flask) -> TestClient:
    """Anonymous client."""
    client = TestClient(empty_app, "", "")
    return client


@pytest.fixture
def product_client(empty_app: flask.Flask) -> TestClient:
    """Client with token-based auth with ADMIN_PRODUCT permissions."""
    _c = TestClient(empty_app, PRODUCT_ADMIN_USERNAME, DEFAULT_PASSWORD)
    r = _c.get("/token")
    client = TestClient(empty_app, r.json["token"])
    return client


@pytest.fixture
def edition_client(empty_app: flask.Flask) -> TestClient:
    """Client with token-based auth with ADMIN_EDITION permissions."""
    _c = TestClient(empty_app, EDITION_ADMIN_USERNAME, DEFAULT_PASSWORD)
    r = _c.get("/token")
    client = TestClient(empty_app, r.json["token"])
    return client


@pytest.fixture
def upload_build_client(empty_app: flask.Flask) -> TestClient:
    """Client with token-based auth with UPLOAD_BUILD permissions."""
    _c = TestClient(empty_app, BUILD_UPLOADER_USERNAME, DEFAULT_PASSWORD)
    r = _c.get("/token")
    client = TestClient(empty_app, r.json["token"])
    return client


@pytest.fixture
def deprecate_build_client(empty_app: flask.Flask) -> TestClient:
    """Client with token-based auth with DEPRECATE_BUILD permissions."""
    _c = TestClient(empty_app, BUILD_DEPRECATOR_USERNAME, DEFAULT_PASSWORD)
    r = _c.get("/token")
    client = TestClient(empty_app, r.json["token"])
    return client
