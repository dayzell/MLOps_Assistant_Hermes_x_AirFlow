"""pytest fixtures (helpers live in harness.py)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
import respx
from harness import _DATA_DIR, BASE_URL, SECRETS, FakeContext, make_context

# --------------------------------------------------------------------------- fixtures


@pytest.fixture(autouse=True)
def _isolated_host(tmp_path: Path) -> Iterator[None]:
    _DATA_DIR["root"] = tmp_path / "plugin-data"
    SECRETS.clear()
    SECRETS.update({"AIRFLOW_USERNAME": "admin", "AIRFLOW_PASSWORD": "s3cr3t-pw"})
    yield
    SECRETS.clear()


@pytest.fixture
def secrets() -> dict[str, str]:
    return SECRETS


@pytest.fixture(params=["v1", "v2"])
def api_version(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def airflow(api_version: str) -> Iterator[respx.MockRouter]:
    """respx router mounted on the API prefix of the parametrised version (auth handled)."""
    with respx.mock(base_url=f"{BASE_URL}/api/{api_version}", assert_all_called=False) as router:
        # JWT endpoint (v2 default auth); harmless for v1.
        router.post(f"{BASE_URL}/auth/token").respond(201, json={"access_token": "jwt-abc.def.ghi"})
        yield router


@pytest.fixture
def ctx(api_version: str, airflow: respx.MockRouter) -> FakeContext:
    auth_mode = "jwt" if api_version == "v2" else "basic"
    return make_context(api_version=api_version, auth_mode=auth_mode)
