"""Behaviour while another SQLite connection holds the write lock.

A second ``sqlite3`` connection plays the role of the engineer's other
session: it holds ``BEGIN IMMEDIATE`` while we verify that

* catalogue / progress reads keep working (WAL readers never wait for a
  writer);
* cancel is *accepted* immediately and from that moment bars the round
  from writing or publishing (in-memory tombstone), reporting
  ``cancelling`` until the state is durable in SQLite;
* the background finaliser completes the cancel/fail finalisation once
  the lock is released — no process restart, re-harvest allowed;
* late writes and the old round's finalisation never touch a new round.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.harvest import HarvestConfig, Harvester
from app.main import create_app
from app.oai import NoRecordsMatch, OaiPage, OaiRecord
from app.store import Store, is_lock_contention


@contextmanager
def external_write_lock(db_path):
    """Hold BEGIN IMMEDIATE on a second connection (the 'other session')."""
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    finally:
        conn.execute("ROLLBACK")
        conn.close()


def make_store(tmp_path: Path, busy_ms: int = 100) -> Store:
    return Store(tmp_path / "locked.sqlite3", busy_timeout_ms=busy_ms)


class HangingClient:
    """A source whose page never arrives (until the task is cancelled)."""

    def __init__(self) -> None:
        self.page_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def identify_time(self):
        return "2026-01-01T00:00:10Z"

    async def list_records(self, *, from_, until, token=None):
        self.page_calls += 1
        await asyncio.sleep(30)
        raise AssertionError("page fetch should have been cancelled")


class SlowIdentifyClient:
    """Identify takes a moment; the page itself is immediate."""

    def __init__(self, identify_delay: float = 0.3) -> None:
        self.identify_delay = identify_delay

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def identify_time(self):
        await asyncio.sleep(self.identify_delay)
        return "2026-01-01T00:00:10Z"

    async def list_records(self, *, from_, until, token=None):
        return OaiPage(
            records=[
                OaiRecord(
                    identifier="oai:lock:1",
                    datestamp="2026-01-01T00:00:05Z",
                    deleted=False,
                    title="锁测试记录",
                )
            ]
        )


class EmptyClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def identify_time(self):
        return "2026-01-01T00:00:20Z"

    async def list_records(self, *, from_, until, token=None):
        raise NoRecordsMatch("空时间窗")


def make_harvester(store: Store, client, **kwargs) -> Harvester:
    return Harvester(
        store,
        HarvestConfig(oai_url="scripted", max_attempts=3, retry_backoff_ms=0),
        client_factory=lambda: client,
        finalize_retry_interval=kwargs.pop("finalize_retry_interval", 0.05),
        **kwargs,
    )


async def wait_done(control, timeout: float = 10.0) -> str:
    try:
        await asyncio.wait_for(control.task, timeout)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    await asyncio.wait_for(control.done.wait(), timeout)
    return control.outcome or "unknown"


# --------------------------------------------------------------------- unit
def test_lock_contention_classification():
    assert is_lock_contention(sqlite3.OperationalError("database is locked"))
    assert is_lock_contention(sqlite3.OperationalError("database table is locked"))
    assert not is_lock_contention(sqlite3.OperationalError("disk I/O error"))
    assert not is_lock_contention(sqlite3.OperationalError("simulated disk error"))
    assert not is_lock_contention(ValueError("database is locked"))


# ------------------------------------------------------------- store level
def test_reads_succeed_while_write_locked(tmp_path):
    store = make_store(tmp_path)
    run = store.create_run()
    store.set_window(run["id"], "2026-01-01T00:00:00Z")
    store.stage_records(
        run["id"],
        [
            {
                "identifier": "oai:read:1",
                "title": "锁下可读",
                "creator": "作者",
                "datestamp": "2025-12-31T00:00:00Z",
                "deleted": False,
            }
        ],
        0,
    )
    assert store.commit_run(run["id"])["committed"]

    with external_write_lock(store.db_path):
        started = time.monotonic()
        page = store.catalog_page(page_size=10)
        state = store.published_state()
        active = store.get_active_run()
        runs = store.list_runs()
        fetched = store.get_run(run["id"])
        elapsed = time.monotonic() - started
        assert elapsed < 1.0, f"reads stalled behind the write lock ({elapsed:.2f}s)"
        assert page["total"] == 1 and page["records"][0]["title"] == "锁下可读"
        assert state["watermark"] == "2026-01-01T00:00:00Z"
        assert active is None
        assert runs and fetched["status"] == "succeeded"
    store.close()


def test_tombstone_bars_writes_and_commit_even_before_cancel_is_durable(tmp_path):
    store = make_store(tmp_path)
    run = store.create_run()
    run_id = run["id"]
    store.set_window(run_id, "2026-01-01T00:00:00Z")
    store.stage_records(
        run_id,
        [
            {
                "identifier": "oai:tomb:1",
                "title": "不应发布",
                "creator": None,
                "datestamp": "2025-12-31T00:00:00Z",
                "deleted": False,
            }
        ],
        0,
    )

    with external_write_lock(store.db_path):
        # The cancel cannot reach the database, but it is accepted in memory.
        with pytest.raises(sqlite3.OperationalError):
            store.cancel_run(run_id, "锁占用期间取消")
        assert store.cancel_pending(run_id)
        # In-flight writes stop immediately, without touching the database.
        assert store.stage_records(run_id, [{"identifier": "oai:tomb:2",
                                             "title": "x", "creator": None,
                                             "datestamp": "2025-12-31T00:00:00Z",
                                             "deleted": False}], 100) == 0
        assert store.set_window(run_id, "2026-01-02T00:00:00Z") is False
        assert store.reset_stage(run_id) is False
        assert store.update_progress(
            run_id, pages_fetched=9, records_seen=9, token_restarts=0,
            attempt=1, complete_list_size=None,
        ) is False
        # The row is honestly still 'running': nothing was persisted yet.
        assert store.get_run(run_id)["status"] == "running"

    # Lock released: a late commit attempt is still refused by the tombstone.
    assert store.commit_run(run_id)["committed"] is False
    assert store.finish_cancelled(run_id, "锁占用期间取消")
    final = store.get_run(run_id)
    assert final["status"] == "cancelled"
    assert final["staged_count"] == 0
    assert store.published_state()["watermark"] is None
    assert store.catalog_page(page_size=10)["total"] == 0
    store.close()


# --------------------------------------------------------- harvester level
async def test_cancel_accepted_under_lock_finalises_after_release(tmp_path):
    store = make_store(tmp_path)
    hanging = HangingClient()
    harvester = make_harvester(store, hanging)

    control = await harvester.start_run()
    for _ in range(100):
        if hanging.page_calls:
            break
        await asyncio.sleep(0.02)
    assert hanging.page_calls == 1  # round is stuck on the never-arriving page

    with external_write_lock(store.db_path):
        started = time.monotonic()
        outcome = await harvester.cancel_run(control.run_id, "锁占用取消", "user")
        elapsed = time.monotonic() - started
        assert outcome == "cancelling"
        assert elapsed < 1.5, f"cancel acceptance stalled ({elapsed:.2f}s)"
        assert store.cancel_pending(control.run_id)
        # Not durable yet: the row honestly still shows 'running'.
        assert store.get_run(control.run_id)["status"] == "running"
        # In-flight writes are refused by the tombstone.
        assert store.stage_records(control.run_id, [], 0) == 0
        await asyncio.sleep(0.3)  # finaliser retries but cannot land yet
        assert store.get_run(control.run_id)["status"] == "running"

    # Lock released: the background finaliser completes the cancellation.
    assert await wait_done(control) == "cancelled"
    final = store.get_run(control.run_id)
    assert final["status"] == "cancelled"
    assert final["staged_count"] == 0
    assert store.get_active_run() is None
    # A late commit attempt by the old round can never publish.
    assert store.commit_run(control.run_id)["committed"] is False
    assert store.published_state()["watermark"] is None

    # Re-harvest without restarting the process.
    harvester._client_factory = lambda: EmptyClient()
    second = await harvester.start_run()
    assert await wait_done(second) == "succeeded"
    assert store.published_state()["watermark"] == "2026-01-01T00:00:20Z"
    store.close()


async def test_failed_round_finalises_after_lock_release(tmp_path):
    store = make_store(tmp_path)
    client = SlowIdentifyClient(identify_delay=0.3)
    harvester = make_harvester(store, client)

    control = await harvester.start_run()
    await asyncio.sleep(0.05)
    with external_write_lock(store.db_path):
        # set_window hits the external lock, the round fails, and the
        # failure finalisation itself cannot land while the lock is held.
        await asyncio.sleep(0.8)
        assert store.get_run(control.run_id)["status"] == "running"

    assert await wait_done(control) == "failed"
    final = store.get_run(control.run_id)
    assert final["status"] == "failed"
    assert final["staged_count"] == 0
    assert "locked" in (final["last_error"] or "")
    assert store.get_active_run() is None
    assert store.published_state()["watermark"] is None

    harvester._client_factory = lambda: EmptyClient()
    second = await harvester.start_run()
    assert await wait_done(second) == "succeeded"
    store.close()


# ---------------------------------------------------------------- web level
@pytest.fixture
def web_locked_app(tmp_path: Path, demo_app, repo):
    def factory():
        from app.oai import OaiClient

        return OaiClient(
            "http://demo.local/oai",
            transport=httpx.ASGITransport(app=demo_app),
        )

    db_path = tmp_path / "web-locked.sqlite3"
    app = create_app(
        db_path=db_path,
        oai_url="http://demo.local/oai",
        busy_timeout_ms=100,
        client_factory=factory,
    )
    return app, db_path


def poll_until(client: TestClient, predicate, attempts: int = 200) -> dict:
    payload = {}
    for _ in range(attempts):
        payload = client.get("/api/status").json()
        if predicate(payload):
            return payload
        time.sleep(0.05)
    raise AssertionError(f"condition not met; last={payload}")


def test_web_stays_responsive_while_locked_and_cancel_completes_after_release(
    web_locked_app, repo
):
    app, db_path = web_locked_app
    with TestClient(app) as client:
        # Baseline: one successful round.
        assert client.post("/api/harvest").status_code == 202
        baseline = poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])
        baseline_catalog = client.get("/api/catalog").json()

        # Slow round, then an external session grabs the write lock.
        repo.set_meta("slow_seconds", "5")
        assert client.post("/api/harvest").status_code == 202
        poll_until(client, lambda p: p["active"] is not None)

        with external_write_lock(db_path):
            # Catalogue and progress stay readable.
            started = time.monotonic()
            status = client.get("/api/status")
            catalog = client.get("/api/catalog")
            elapsed = time.monotonic() - started
            assert status.status_code == 200 and catalog.status_code == 200
            assert elapsed < 2.0, f"web reads stalled ({elapsed:.2f}s)"
            assert catalog.json()["records"] == baseline_catalog["records"]

            # Cancel is accepted at once; not yet durable -> "cancelling".
            started = time.monotonic()
            resp = client.post("/api/harvest/cancel")
            elapsed = time.monotonic() - started
            assert resp.status_code == 200
            assert resp.json()["status"] == "cancelling"
            assert elapsed < 2.0, f"cancel acceptance stalled ({elapsed:.2f}s)"

            shown = client.get("/api/status").json()["active"]
            assert shown["phase"] == "cancelling"
            assert shown["cancel_pending"] is True

            # A new round cannot start before finalisation lands.
            assert client.post("/api/harvest").status_code == 409

        # Lock released: the background finaliser completes the cancel.
        settled = poll_until(client, lambda p: p["active"] is None)
        assert settled["last_run"]["status"] == "cancelled"
        # Published content is untouched.
        after = client.get("/api/status").json()
        assert after["published"]["watermark"] == baseline["published"]["watermark"]
        assert client.get("/api/catalog").json()["records"] == baseline_catalog["records"]

        # Re-harvest without restarting the process.
        repo.set_meta("slow_seconds", "0")
        assert client.post("/api/harvest").status_code == 202
        done = poll_until(
            client,
            lambda p: p["active"] is None
            and p["published"]["success_count"]
            > baseline["published"]["success_count"],
        )
        assert done["last_run"]["status"] == "succeeded"


def test_restart_while_locked_conflicts_then_works_after_release(
    web_locked_app, repo, monkeypatch
):
    monkeypatch.setattr("app.main.RESTART_FINALIZE_WAIT_SECONDS", 0.5)
    app, db_path = web_locked_app
    with TestClient(app) as client:
        repo.set_meta("slow_seconds", "5")
        assert client.post("/api/harvest").status_code == 202
        poll_until(client, lambda p: p["active"] is not None)

        with external_write_lock(db_path):
            started = time.monotonic()
            resp = client.post("/api/harvest/restart")
            elapsed = time.monotonic() - started
            assert resp.status_code == 409
            assert "收尾" in resp.json()["detail"]
            assert elapsed < 2.5

        poll_until(client, lambda p: p["active"] is None)
        repo.set_meta("slow_seconds", "0")
        again = client.post("/api/harvest/restart")
        assert again.status_code == 202
        poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])
