"""同一持久化 SQLite 库并发启动升级与中断恢复测试。

覆盖：
* 两个/多个进程（线程模拟与真实子进程）同时启动：只发生一次有效升级，
  后到者读到版本标记直接服务，不重复回填、不烧号；
* 升级任一步中断（异常/被杀死）：单条 IMMEDIATE 事务整体回滚，库恢复
  升级前结构，再次启动升级到一致结构；部分升级的库不对外提供服务；
* 历史同号多份资料仍列出冲突修订号并中止，不自动挑选或改写；
* 无冲突旧库：修订计数、实绩原值（version/orig_*）、卸箱生效区间与
  封签版本回填正确，重复启动不重复回填；
* 旧版本“多事务迁移”留下的半升级库可被继续补齐到一致结构；
* 升级后 HTTP API 输入/响应/状态码不变，可正常调用。
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading

import pytest
from sqlalchemy import create_engine, inspect, text

from app import database


# ---------------------------------------------------------------------------
# 旧库构造
# ---------------------------------------------------------------------------


def _create_old_db_with_business_data(path: str) -> object:
    """构造一个“本轮版本之前”的持久化旧库（无迁移新增列/表）。

    含：资料修订（slots 1/2/3、manifests 1/2）、装船实绩与事件、卸箱执行
    与冻结港区间、封签清单与放行凭证；缺：revision_counters、
    unloading_effective_ports、各新增列与唯一索引。
    手建 DDL 不设外键，使其余未建表由 create_all 按最新结构补齐，最终库
    结构与当前模型一致（迁移聚焦表按完整旧形状建出）。
    """
    eng = create_engine(f"sqlite:///{path}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE slot_revisions ("
            "id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, "
            "min_moment FLOAT NOT NULL, max_moment FLOAT NOT NULL, "
            "created_at DATETIME)"
        ))
        conn.execute(text(
            "CREATE TABLE manifest_revisions ("
            "id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, "
            "created_at DATETIME)"
        ))
        conn.execute(text(
            "CREATE TABLE loading_actuals ("
            "id INTEGER PRIMARY KEY, plan_id VARCHAR(64) NOT NULL, "
            "baseline_crane_count INTEGER NOT NULL, "
            "baseline_schedule_id INTEGER, baseline_frozen_at DATETIME, "
            "created_at DATETIME)"
        ))
        conn.execute(text(
            "CREATE TABLE loading_actual_events ("
            "id INTEGER PRIMARY KEY, actuals_id INTEGER NOT NULL, "
            "seq INTEGER NOT NULL, event_type VARCHAR(8) NOT NULL, "
            "container_no VARCHAR(64) NOT NULL, crane_no INTEGER NOT NULL, "
            "minute INTEGER NOT NULL, registered_at DATETIME)"
        ))
        conn.execute(text(
            "CREATE TABLE unloading_executions ("
            "id INTEGER PRIMARY KEY, plan_id VARCHAR(64) NOT NULL, "
            "frozen_sequence_id INTEGER, frozen_plan_order TEXT NOT NULL, "
            "initial_moment FLOAT NOT NULL, remaining_moment FLOAT NOT NULL, "
            "progress_version INTEGER NOT NULL, "
            "frozen_at DATETIME, created_at DATETIME)"
        ))
        conn.execute(text(
            "CREATE TABLE unloading_frozen_ports ("
            "id INTEGER PRIMARY KEY, execution_id INTEGER NOT NULL, "
            "discharge_order INTEGER NOT NULL, "
            "min_remaining_moment FLOAT NOT NULL, "
            "max_remaining_moment FLOAT NOT NULL)"
        ))
        conn.execute(text(
            "CREATE TABLE seal_manifests ("
            "id INTEGER PRIMARY KEY, plan_id VARCHAR(64) NOT NULL, "
            "revision_no INTEGER NOT NULL, container_count INTEGER NOT NULL, "
            "content_hash VARCHAR(64) NOT NULL, scan_version INTEGER NOT NULL, "
            "created_at DATETIME)"
        ))
        conn.execute(text(
            "CREATE TABLE seal_releases ("
            "id INTEGER PRIMARY KEY, plan_id VARCHAR(64) NOT NULL, "
            "release_no VARCHAR(128) NOT NULL, handover_id VARCHAR(64) NOT NULL, "
            "handover_no VARCHAR(128) NOT NULL, cutoff_minute INTEGER NOT NULL, "
            "expected_scan_version INTEGER NOT NULL, scan_version INTEGER NOT NULL, "
            "revision_no INTEGER NOT NULL, container_count INTEGER NOT NULL, "
            "max_check_minute INTEGER NOT NULL, "
            "scans_fingerprint VARCHAR(64) NOT NULL, "
            "actuals_fingerprint VARCHAR(64) NOT NULL, created_at DATETIME)"
        ))

        for rev in (1, 2, 3):
            conn.execute(text(
                "INSERT INTO slot_revisions (revision, min_moment, max_moment) "
                "VALUES (:r, -100, 100)"), {"r": rev})
        for rev in (1, 2):
            conn.execute(text(
                "INSERT INTO manifest_revisions (revision) VALUES (:r)"), {"r": rev})

        conn.execute(text(
            "INSERT INTO loading_actuals "
            "(id, plan_id, baseline_crane_count, baseline_schedule_id, "
            "baseline_frozen_at, created_at) "
            "VALUES (1, 'p-actual', 2, NULL, '2026-01-01 00:00:00', "
            "'2026-01-01 00:00:00')"))
        for seq, etype, crane, minute in (
            (1, "start", 2, 5),
            (2, "finish", 2, 10),
        ):
            conn.execute(text(
                "INSERT INTO loading_actual_events "
                "(actuals_id, seq, event_type, container_no, crane_no, minute, "
                "registered_at) VALUES (1, :s, :e, 'C001', :c, :m, "
                "'2026-01-01 00:00:00')"),
                {"s": seq, "e": etype, "c": crane, "m": minute})

        conn.execute(text(
            "INSERT INTO unloading_executions "
            "(id, plan_id, frozen_sequence_id, frozen_plan_order, initial_moment, "
            "remaining_moment, progress_version, frozen_at, created_at) "
            "VALUES (1, 'p-exec', NULL, '[]', 100.0, 80.0, 0, "
            "'2026-01-01 00:00:00', '2026-01-01 00:00:00')"))
        for order, lo, hi in ((1, -100.0, 100.0), (2, 0.0, 50.0)):
            conn.execute(text(
                "INSERT INTO unloading_frozen_ports "
                "(execution_id, discharge_order, min_remaining_moment, "
                "max_remaining_moment) VALUES (1, :o, :lo, :hi)"),
                {"o": order, "lo": lo, "hi": hi})

        conn.execute(text(
            "INSERT INTO seal_manifests "
            "(id, plan_id, revision_no, container_count, content_hash, "
            "scan_version, created_at) "
            "VALUES (1, 'p-seal', 1, 1, 'x', 0, '2026-01-01 00:00:00')"))
        conn.execute(text(
            "INSERT INTO seal_releases "
            "(id, plan_id, release_no, handover_id, handover_no, cutoff_minute, "
            "expected_scan_version, scan_version, revision_no, container_count, "
            "max_check_minute, scans_fingerprint, actuals_fingerprint, created_at) "
            "VALUES (1, 'p-seal', 'rel-1', 'h-1', 'ho-1', 100, 0, 0, 1, 1, 10, "
            "'sf', 'af', '2026-01-01 00:00:00')"))
    return eng


def _columns(eng, table: str) -> set[str]:
    return {c["name"] for c in inspect(eng).get_columns(table)}


def _assert_migrated_once(eng) -> None:
    """升级结果断言：回填正确且只发生一次。"""
    with eng.begin() as conn:
        # 版本标记唯一
        assert conn.execute(text(
            f"SELECT version FROM {database._SCHEMA_VERSION_TABLE} ORDER BY version"
        )).scalars().all() == [database.SCHEMA_VERSION]

        # 计数器各类别一行、回填为历史最大修订号
        counters = dict(conn.execute(text(
            "SELECT category, last_allocated FROM revision_counters"
        )).all())
        assert counters == {"slots": 3, "manifests": 2}

        # 实绩版本与原始值保留
        assert conn.execute(text(
            "SELECT version FROM loading_actuals WHERE id = 1"
        )).scalar_one() == 1
        events = conn.execute(text(
            "SELECT seq, minute, orig_minute, crane_no, orig_crane_no, corrected "
            "FROM loading_actual_events ORDER BY seq"
        )).all()
        assert events == [
            (1, 5, 5, 2, 2, 0),
            (2, 10, 10, 2, 2, 0),
        ]

        # 卸箱区间版本与生效区间（=冻结原定区间），恰好每个港一行
        assert conn.execute(text(
            "SELECT window_version FROM unloading_executions WHERE id = 1"
        )).scalar_one() == 0
        ports = conn.execute(text(
            "SELECT discharge_order, min_remaining_moment, max_remaining_moment "
            "FROM unloading_effective_ports ORDER BY discharge_order"
        )).all()
        assert ports == [(1, -100.0, 100.0), (2, 0.0, 50.0)]

        # 封签版本
        assert conn.execute(text(
            "SELECT seal_version FROM seal_manifests WHERE id = 1"
        )).scalar_one() == 0
        assert conn.execute(text(
            "SELECT seal_version FROM seal_releases WHERE id = 1"
        )).scalar_one() == 0

        # 唯一索引生效
        indexes = {ix["name"] for ix in inspect(eng).get_indexes("slot_revisions")}
        assert "uq_slot_revision_no" in indexes


# ---------------------------------------------------------------------------
# 单次升级：完整回填 + 重复启动不重复
# ---------------------------------------------------------------------------


def test_migrates_old_db_with_all_backfills(tmp_path, monkeypatch):
    """无冲突旧库：计数/实绩原值/生效区间/封签版本全部回填正确。"""
    db_path = tmp_path / "old.db"
    eng = _create_old_db_with_business_data(str(db_path))
    eng.dispose()

    eng = create_engine(f"sqlite:///{db_path}")
    monkeypatch.setattr(database, "engine", eng)
    try:
        database.init_db()
        _assert_migrated_once(eng)
        # 再启动两次：跳过升级，回填不重复、计数不回退
        database.init_db()
        database.init_db()
        _assert_migrated_once(eng)
    finally:
        eng.dispose()


def test_repeated_startup_does_not_rebackfill_or_reset_counter(tmp_path, monkeypatch):
    """升级后计数器已被业务推进：重复启动绝不回退计数器（不烧号）。"""
    db_path = tmp_path / "old.db"
    eng = _create_old_db_with_business_data(str(db_path))
    eng.dispose()

    eng = create_engine(f"sqlite:///{db_path}")
    monkeypatch.setattr(database, "engine", eng)
    try:
        database.init_db()
        with eng.begin() as conn:
            conn.execute(text(
                "UPDATE revision_counters SET last_allocated = 5 "
                "WHERE category = 'slots'"))
        database.init_db()
        with eng.begin() as conn:
            assert conn.execute(text(
                "SELECT last_allocated FROM revision_counters "
                "WHERE category = 'slots'"
            )).scalar_one() == 5
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# 并发启动：线程（SQLite 文件锁跨线程即跨连接）
# ---------------------------------------------------------------------------


def test_concurrent_startup_upgrades_once(tmp_path):
    """多个启动并发：均成功返回，只发生一次有效升级、回填不重复。"""
    db_path = tmp_path / "old.db"
    builder = _create_old_db_with_business_data(str(db_path))
    builder.dispose()

    target = create_engine(f"sqlite:///{db_path}")
    originals = database.engine
    database.engine = target
    errors: list[Exception] = []

    def startup():
        try:
            database.init_db()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    try:
        threads = [threading.Thread(target=startup) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)
        assert errors == []
        _assert_migrated_once(target)
    finally:
        database.engine = originals
        target.dispose()


# ---------------------------------------------------------------------------
# 真实两个子进程同时启动
# ---------------------------------------------------------------------------


def test_two_processes_start_concurrently_upgrade_once(tmp_path):
    """两个服务进程对同一库同时启动：二者都成功，升级只生效一次。"""
    db_path = tmp_path / "old.db"
    builder = _create_old_db_with_business_data(str(db_path))
    builder.dispose()

    code = (
        "from app.database import init_db; "
        "init_db(); "
        "print('started')"
    )
    env = dict(os.environ)
    env["STOWAGE_DATABASE_URL"] = f"sqlite:///{db_path}"
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in [os.getcwd(), env.get("PYTHONPATH")] if p
    )

    procs = [
        subprocess.Popen(
            [sys.executable, "-c", code],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            cwd=os.getcwd(),
        )
        for _ in range(2)
    ]
    results = [p.communicate(timeout=120) for p in procs]
    for p, (out, err) in zip(procs, results):
        assert p.returncode == 0, (
            f"进程启动失败 rc={p.returncode}\nSTDOUT={out!r}\nSTDERR={err!r}"
        )
        assert b"started" in out

    eng = create_engine(f"sqlite:///{db_path}")
    try:
        _assert_migrated_once(eng)
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# 中断恢复
# ---------------------------------------------------------------------------


def test_failure_mid_upgrade_rolls_back_and_restart_completes(tmp_path, monkeypatch):
    """升级途中失败：DDL 一并回滚到升级前结构；再次启动升级成功。"""
    db_path = tmp_path / "old.db"
    builder = _create_old_db_with_business_data(str(db_path))
    builder.dispose()

    eng = create_engine(f"sqlite:///{db_path}")
    monkeypatch.setattr(database, "engine", eng)

    real_migrate = database._apply_lightweight_migrations

    def failing_migrate(conn):
        # create_all 已在同一事务执行过：强制失败必须连同建表一起回滚
        raise RuntimeError("模拟升级进程被杀死")

    monkeypatch.setattr(database, "_apply_lightweight_migrations", failing_migrate)
    with pytest.raises(RuntimeError, match="模拟升级进程被杀死"):
        database.init_db()

    # 事务整体回滚：版本标记、计数器/生效区间表均不存在；旧表与数据仍在
    with eng.begin() as conn:
        names = set(inspect(eng).get_table_names())
        assert database._SCHEMA_VERSION_TABLE not in names
        assert "revision_counters" not in names
        assert "unloading_effective_ports" not in names
        assert conn.execute(text(
            "SELECT COUNT(*) FROM slot_revisions"
        )).scalar_one() == 3
        # 旧库仍无唯一索引
        indexes = {ix["name"] for ix in inspect(eng).get_indexes("slot_revisions")}
        assert "uq_slot_revision_no" not in indexes

    # 恢复：继续同一套升级，结构一致
    monkeypatch.setattr(database, "_apply_lightweight_migrations", real_migrate)
    database.init_db()
    _assert_migrated_once(eng)
    eng.dispose()


def test_partially_upgraded_library_from_old_code_is_completed(tmp_path, monkeypatch):
    """旧版本多事务迁移遗留的半升级库：重启继续补齐、不重复回填。

    模拟：补列已各自提交（window_version、seal_version、orig_minute 列在，
    但 orig_minute 尚未回填），create_all 已建新表但计数器/生效区间为空。
    """
    db_path = tmp_path / "old.db"
    builder = _create_old_db_with_business_data(str(db_path))
    builder.dispose()

    # 模拟旧版本逐事务迁移：create_all 已建新表（计数器/生效区间等），
    # 旧表补列已提交，但回填尚未执行（orig_minute 全为默认 0）。
    pre = create_engine(f"sqlite:///{db_path}")
    from app import models  # noqa: F401  ensure metadata registered

    database.Base.metadata.create_all(bind=pre, checkfirst=True)
    with pre.begin() as conn:
        # create_all 不动已存在的旧表；补列与回填在旧版本是分开的事务
        conn.execute(text(
            "ALTER TABLE loading_actual_events ADD COLUMN orig_minute "
            "INTEGER NOT NULL DEFAULT 0"))
        # 注意：orig_minute 未回填（保持默认 0），模拟补列后崩溃
    pre.dispose()

    eng = create_engine(f"sqlite:///{db_path}")
    monkeypatch.setattr(database, "engine", eng)
    try:
        database.init_db()
        _assert_migrated_once(eng)
        database.init_db()  # 再启动不重复
        _assert_migrated_once(eng)
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# 冲突仍中止（不挑选、不改写）
# ---------------------------------------------------------------------------


def test_conflicting_history_aborts_inside_upgrade_and_changes_nothing(
    tmp_path, monkeypatch
):
    """同号多份：升级事务内列出冲突修订号中止，库不被改动。"""
    db_path = tmp_path / "old.db"
    eng = create_engine(f"sqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE slot_revisions ("
            "id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, "
            "min_moment FLOAT NOT NULL, max_moment FLOAT NOT NULL, "
            "created_at DATETIME)"))
        conn.execute(text(
            "CREATE TABLE manifest_revisions ("
            "id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, "
            "created_at DATETIME)"))
        for rev in (2, 2, 3, 3):
            conn.execute(text(
                "INSERT INTO slot_revisions (revision, min_moment, max_moment) "
                "VALUES (:r, -1, 1)"), {"r": rev})
        conn.execute(text(
            "INSERT INTO manifest_revisions (revision) VALUES (5)"))
        conn.execute(text(
            "INSERT INTO manifest_revisions (revision) VALUES (5)"))

    monkeypatch.setattr(database, "engine", eng)
    try:
        with pytest.raises(RuntimeError) as exc_info:
            database.init_db()
        msg = str(exc_info.value)
        assert "同号多份" in msg and "槽位资料" in msg and "2、3" in msg
        assert "箱清单" in msg and "5" in msg and "不任取其一" in msg

        # 未写版本标记、未建唯一索引、资料原样保留（不挑选/不改写）
        with eng.begin() as conn:
            names = set(inspect(eng).get_table_names())
            assert database._SCHEMA_VERSION_TABLE not in names
            assert conn.execute(text(
                "SELECT COUNT(*) FROM slot_revisions WHERE revision = 2"
            )).scalar_one() == 2
            indexes = {ix["name"] for ix in inspect(eng).get_indexes("slot_revisions")}
            assert "uq_slot_revision_no" not in indexes
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# 升级后 HTTP API 兼容可调用
# ---------------------------------------------------------------------------


def _create_simple_old_db(path: str):
    """仅有旧版资料表的旧库（其余表由 create_all 建最新结构）。"""
    eng = create_engine(f"sqlite:///{path}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE slot_revisions ("
            "id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, "
            "min_moment FLOAT NOT NULL, max_moment FLOAT NOT NULL, "
            "created_at DATETIME)"))
        conn.execute(text(
            "CREATE TABLE manifest_revisions ("
            "id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, "
            "created_at DATETIME)"))
        for rev in (1, 2, 3):
            conn.execute(text(
                "INSERT INTO slot_revisions (revision, min_moment, max_moment) "
                "VALUES (:r, -100000, 100000)"), {"r": rev})
        for rev in (1, 2):
            conn.execute(text(
                "INSERT INTO manifest_revisions (revision) VALUES (:r)"),
                {"r": rev})
    return eng


def test_api_serves_after_upgrade_and_revision_continues(tmp_path, monkeypatch):
    """升级后服务正常：health 200，录入取得历史最大号 +1，查询兼容。"""
    from fastapi.testclient import TestClient

    from app.main import app

    db_path = tmp_path / "old.db"
    builder = _create_simple_old_db(str(db_path))
    builder.dispose()

    eng = create_engine(f"sqlite:///{db_path}")
    monkeypatch.setattr(database, "engine", eng)
    # 让 API 请求会话也指向升级后的同一库；结束后还原
    original_bind = database.SessionLocal.kw["bind"]
    database.SessionLocal.configure(bind=eng)
    try:
        with TestClient(app) as client:
            assert client.get("/health").status_code == 200

            payload = {
                "slots": [
                    {"code": "A01", "stack": "A", "tier": 1,
                     "capacity": 100, "arm": 2.0},
                ],
                "adjacency": [],
                "incompatible_classes": [],
                "min_moment": -100000,
                "max_moment": 100000,
            }
            r = client.post("/api/slot-revisions", json=payload)
            assert r.status_code == 201, r.text
            # 新录入沿用历史最大号 3 继续递增到 4（计数器回填正确、不烧号）
            assert r.json()["slots_revision"] == 4

            got = client.get("/api/slot-revisions/4")
            assert got.status_code == 200
            assert [s["code"] for s in got.json()["slots"]] == ["A01"]

            # 历史修订仍可按号查询
            assert client.get("/api/slot-revisions/1").status_code == 200

            # 再次启动：识别已完成状态后直接服务
            database.init_db()
            assert client.get("/health").status_code == 200
    finally:
        database.SessionLocal.configure(bind=original_bind)
        eng.dispose()
