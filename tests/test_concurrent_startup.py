"""并发启动与中断恢复的回归测试。

需求：同一份持久化 SQLite 旧库被两个服务进程同时启动时——

- 只发生一次有效升级：先到进程在库级写事务内完成建表/冲突检查/补列回填，
  后到进程在写锁上等待，提交后按结构与迁移台账识别“已完成”直接服务；
- 任一步骤中断（进程被杀/失败回滚）后再次启动，库恢复到一致结构后才
  对外服务；部分升级的库不对外提供服务；
- 历史同号多份资料仍须列出冲突修订号并中止，不自动挑选或改写；
- 无冲突库的修订计数、实绩原值、卸箱生效区间与封签版本保留正确值，
  重复启动不重复回填、不烧号。
"""

from __future__ import annotations

import os
import queue as queue_mod
import sys
import threading
import time

import pytest
from sqlalchemy import create_engine, text

from app import database


# ---------------------------------------------------------------------------
# 旧库构造
# ---------------------------------------------------------------------------


def _create_rich_old_db(path: str) -> None:
    """构造“升级前”的丰富旧库：含修订、实绩、事件、已开始卸箱执行与封签。

    缺所有新版本列（version/orig_*/corrected/window_version/seal_version）、
    缺唯一索引、无 revision_counters 与生效区间表。
    """
    eng = create_engine(f"sqlite:///{path}")
    with eng.begin() as c:
        c.execute(text(
            "CREATE TABLE slot_revisions ("
            "id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, "
            "min_moment FLOAT, max_moment FLOAT, created_at DATETIME)"
        ))
        c.execute(text(
            "CREATE TABLE manifest_revisions ("
            "id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, created_at DATETIME)"
        ))
        for rev in (5, 2, 7):
            c.execute(
                text("INSERT INTO slot_revisions (revision,min_moment,max_moment) "
                     "VALUES (:r,-1,1)"), {"r": rev},
            )
        for rev in (3, 6):
            c.execute(text("INSERT INTO manifest_revisions (revision) VALUES (:r)"),
                      {"r": rev})

        c.execute(text("CREATE TABLE loading_actuals "
                       "(id INTEGER PRIMARY KEY, plan_id VARCHAR(64))"))
        c.execute(text("INSERT INTO loading_actuals (plan_id) VALUES ('p1'),('p2')"))

        c.execute(text(
            "CREATE TABLE loading_actual_events ("
            "id INTEGER PRIMARY KEY, minute INTEGER NOT NULL, crane_no INTEGER NOT NULL)"
        ))
        c.execute(text(
            "INSERT INTO loading_actual_events (minute, crane_no) "
            "VALUES (10, 2), (20, 3)"))

        c.execute(text("CREATE TABLE unloading_executions "
                       "(id INTEGER PRIMARY KEY, plan_id VARCHAR(64))"))
        c.execute(text("INSERT INTO unloading_executions (plan_id) VALUES ('p1'),('p2')"))
        c.execute(text(
            "CREATE TABLE unloading_frozen_ports ("
            "id INTEGER PRIMARY KEY, execution_id INTEGER, discharge_order INTEGER, "
            "min_remaining_moment FLOAT, max_remaining_moment FLOAT)"))
        c.execute(text(
            "INSERT INTO unloading_frozen_ports "
            "(execution_id, discharge_order, min_remaining_moment, max_remaining_moment) "
            "VALUES (1,1,100,200),(1,2,0,50)"))

        c.execute(text("CREATE TABLE seal_manifests "
                       "(id INTEGER PRIMARY KEY, plan_id VARCHAR(64))"))
        c.execute(text("INSERT INTO seal_manifests (plan_id) VALUES ('p1')"))
        c.execute(text("CREATE TABLE seal_releases "
                       "(id INTEGER PRIMARY KEY, plan_id VARCHAR(64))"))
        c.execute(text("INSERT INTO seal_releases (plan_id) VALUES ('p1')"))
    eng.dispose()


def _open_db(path: str):
    con = create_engine(f"sqlite:///{path}")
    return con


def _table_columns(eng, table: str) -> set[str]:
    with eng.begin() as c:
        return {r[1] for r in c.execute(text(f"PRAGMA table_info({table})")).all()}


def _migration_ledger(eng) -> set[str]:
    with eng.begin() as c:
        return {r[0] for r in c.execute(
            text("SELECT revision FROM schema_migrations")).all()}


def _run_init_with_engine(db_path: str, lock_timeout_ms: int | None = None) -> None:
    """以指向指定库的临时引擎执行一次 init_db（独立引擎，不污染全局）。"""
    from sqlalchemy import event

    eng = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    startup_timeout = (
        lock_timeout_ms
        if lock_timeout_ms is not None
        else database.settings.startup_lock_timeout_ms
    )
    phase = {"startup": True}

    @event.listens_for(eng, "connect")
    def _pragmas(dbapi_conn, _record):  # 与生产一致的连接 PRAGMA 与超时分阶段
        cur = dbapi_conn.cursor()
        cur.execute(
            f"PRAGMA busy_timeout={startup_timeout if phase['startup'] else 5000}"
        )
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    orig_engine = database.engine
    orig_timeout = database.settings.startup_lock_timeout_ms
    database.engine = eng
    if lock_timeout_ms is not None:
        database.settings.startup_lock_timeout_ms = lock_timeout_ms
    try:
        database.init_db()
        phase["startup"] = False
        eng.dispose()
    finally:
        phase["startup"] = False
        database.settings.startup_lock_timeout_ms = orig_timeout
        database.engine = orig_engine
        eng.dispose()


# ---------------------------------------------------------------------------
# 单进程：旧库迁移值正确 + 重复启动不重复回填/烧号
# ---------------------------------------------------------------------------


def test_migrates_rich_old_db_and_preserves_values(tmp_path):
    """无冲突丰富旧库：各类回填值正确，计数器=历史最大修订号。"""
    db_path = str(tmp_path / "old.db")
    _create_rich_old_db(db_path)

    _run_init_with_engine(db_path)

    eng = _open_db(db_path)
    try:
        with eng.begin() as c:
            # 实绩版本：历史实绩均为第 1 版
            assert c.execute(text(
                "SELECT DISTINCT version FROM loading_actuals")).fetchall() == [(1,)]
            # 实绩事件原值：orig_minute=minute、orig_crane_no=crane_no、未更正
            assert c.execute(text(
                "SELECT minute, orig_minute, crane_no, orig_crane_no, corrected "
                "FROM loading_actual_events ORDER BY id")).fetchall() == [
                (10, 10, 2, 2, 0), (20, 20, 3, 3, 0),
            ]
            # 卸箱区间版本：历史执行均为 0
            assert c.execute(text(
                "SELECT DISTINCT window_version FROM unloading_executions")
            ).fetchall() == [(0,)]
            # 生效区间：仅已开始的执行 1 回填冻结区间，执行 2 不回填
            assert c.execute(text(
                "SELECT execution_id, discharge_order, min_remaining_moment, "
                "max_remaining_moment FROM unloading_effective_ports ORDER BY id")
            ).fetchall() == [(1, 1, 100.0, 200.0), (1, 2, 0.0, 50.0)]
            # 封签版本：清单/凭证均为 0
            assert c.execute(text(
                "SELECT (SELECT seal_version FROM seal_manifests), "
                "(SELECT seal_version FROM seal_releases)")).fetchone() == (0, 0)
            # 计数器：各类别回填到历史最大修订号
            assert dict(c.execute(text(
                "SELECT category, last_allocated FROM revision_counters")
            ).fetchall()) == {"slots": 7, "manifests": 6}
            # 唯一索引已建
            assert c.execute(text(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' "
                "AND name='uq_slot_revision_no'")).scalar_one() == 1
        # 台账七步齐全
        assert len(_migration_ledger(eng)) == 7
    finally:
        eng.dispose()


def test_repeated_startup_does_not_rebackfill_or_burn_numbers(tmp_path):
    """迁移后重复启动：不重复回填、计数器不回退不跳号。"""
    db_path = str(tmp_path / "old.db")
    _create_rich_old_db(db_path)

    _run_init_with_engine(db_path)
    # 模拟业务已在迁移后分配过新号（slots 8、9）
    eng = _open_db(db_path)
    with eng.begin() as c:
        c.execute(text("UPDATE revision_counters SET last_allocated = 9 "
                       "WHERE category = 'slots'"))
    eng.dispose()

    # 再启动两次：均应识别已完成状态、空操作
    _run_init_with_engine(db_path)
    _run_init_with_engine(db_path)

    eng = _open_db(db_path)
    try:
        with eng.begin() as c:
            assert dict(c.execute(text(
                "SELECT category, last_allocated FROM revision_counters")
            ).fetchall()) == {"slots": 9, "manifests": 6}
            # 生效区间仍只有冻结的两条（不重复回填）
            assert c.execute(text(
                "SELECT COUNT(*) FROM unloading_effective_ports")).scalar_one() == 2
            # 事件原值未被二次回填改写
            assert c.execute(text(
                "SELECT minute, orig_minute FROM loading_actual_events ORDER BY id")
            ).fetchall() == [(10, 10), (20, 20)]
        assert len(_migration_ledger(eng)) == 7
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# 多进程：同一份旧库两个进程同时启动
# ---------------------------------------------------------------------------


_CHILD_SCRIPT = r"""
import sys
from app import database
from sqlalchemy import create_engine

db_path = sys.argv[1]
eng = create_engine("sqlite:///" + db_path, connect_args={"check_same_thread": False})
database.engine = eng
try:
    database.init_db()
finally:
    eng.dispose()
print("INIT_OK")
"""


def _child_init(db_path: str, ready: queue_mod.Queue) -> None:
    # spawn/fork 均可：用独立 Python 子进程，环境变量传递路径
    import subprocess

    proc = subprocess.run(
        [sys.executable, "-c", _CHILD_SCRIPT, db_path],
        capture_output=True, text=True, env={
            **os.environ, "PYTHONPATH": os.path.dirname(os.path.dirname(__file__)),
        },
    )
    ready.put({"returncode": proc.returncode,
               "stdout": proc.stdout, "stderr": proc.stderr})


def test_two_processes_start_concurrently_only_one_upgrade(tmp_path):
    """两个进程同时启动旧库：均成功；库只被有效升级一次，后到者等待后识别完成。"""
    db_path = str(tmp_path / "old.db")
    _create_rich_old_db(db_path)

    q: queue_mod.Queue = queue_mod.Queue()
    # 用屏障尽量让两者同时进入 init_db（写锁竞争真实发生）
    barrier = threading.Barrier(2)

    def run_child():
        barrier.wait()
        _child_init(db_path, q)

    threads = [threading.Thread(target=run_child) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    results = [q.get(timeout=30) for _ in range(2)]

    assert all(r["returncode"] == 0 for r in results), results
    assert all("INIT_OK" in r["stdout"] for r in results)

    # 升级后结构/值正确且只回填一次
    eng = _open_db(db_path)
    try:
        with eng.begin() as c:
            assert c.execute(text(
                "SELECT COUNT(*) FROM unloading_effective_ports")).scalar_one() == 2
            assert dict(c.execute(text(
                "SELECT category, last_allocated FROM revision_counters")
            ).fetchall()) == {"slots": 7, "manifests": 6}
            assert c.execute(text(
                "SELECT COUNT(*) FROM loading_actual_events "
                "WHERE orig_minute = minute")).scalar_one() == 2
        ledger = _migration_ledger(eng)
        assert len(ledger) == 7
        # 台账每步仅一行（唯一主键兜底 + 只追加一次）
        with eng.begin() as c:
            assert c.execute(text(
                "SELECT COUNT(*) FROM schema_migrations")).scalar_one() == 7
    finally:
        eng.dispose()


def test_late_comer_waits_then_serves_while_upgrade_in_progress(tmp_path, monkeypatch):
    """先到进程升级期间，后到进程在写锁上等待；先到提交后后到识别完成并成功。"""
    db_path = str(tmp_path / "old.db")
    _create_rich_old_db(db_path)
    eng = create_engine(f"sqlite:///{db_path}",
                        connect_args={"check_same_thread": False})
    monkeypatch.setattr(database, "engine", eng)

    # 先到进程：独立线程内用该线程自己的 sqlite3 连接持写锁约 1 秒
    import sqlite3

    locked = threading.Event()
    released = threading.Event()

    def hold_lock_then_release():
        holder = sqlite3.connect(db_path)
        holder.execute("PRAGMA busy_timeout=30000")
        holder.execute("BEGIN IMMEDIATE")
        locked.set()
        time.sleep(1.0)
        holder.commit()
        holder.close()
        released.set()

    holder_thread = threading.Thread(target=hold_lock_then_release)
    holder_thread.start()
    assert locked.wait(5)
    try:
        # 后到进程：应等待 ~1s 后拿到锁并完成升级，而不是立即 SQLITE_BUSY
        t0 = time.monotonic()
        database.init_db()
        waited = time.monotonic() - t0
        assert waited >= 0.9
    finally:
        holder_thread.join()
        eng.dispose()

    check = _open_db(db_path)
    try:
        assert released.is_set()
        assert len(_migration_ledger(check)) == 7
    finally:
        check.dispose()


def test_lock_timeout_aborts_with_locatable_reason(tmp_path):
    """写锁长期被占：启动按可定位原因中止（StartupError），库未被部分升级。"""
    db_path = str(tmp_path / "old.db")
    _create_rich_old_db(db_path)

    import sqlite3

    holder = sqlite3.connect(db_path)
    holder.execute("PRAGMA busy_timeout=100")
    holder.execute("BEGIN IMMEDIATE")
    holder.execute("CREATE TABLE IF NOT EXISTS probe_lock (x INTEGER)")
    try:
        with pytest.raises(database.StartupError, match="写锁"):
            _run_init_with_engine(db_path, lock_timeout_ms=300)
    finally:
        holder.execute("DROP TABLE probe_lock")
        holder.commit()
        holder.close()

    # 锁占用方释放后，库仍可正常完成升级（结构回到一致状态）
    _run_init_with_engine(db_path)
    eng = _open_db(db_path)
    try:
        assert len(_migration_ledger(eng)) == 7
    finally:
        eng.dispose()


def test_connect_phase_lock_timeout_is_locatable(tmp_path, monkeypatch):
    """连接/取锁阶段抢不到写锁：报可定位启动错误，不遗留半成品结构。"""
    db_path = str(tmp_path / "fresh.db")
    # 全新库文件：先到连接在 WAL 切换或 BEGIN IMMEDIATE 阶段即需要写锁
    import sqlite3

    holder = sqlite3.connect(db_path)
    holder.execute("PRAGMA busy_timeout=100")
    holder.execute("BEGIN IMMEDIATE")
    holder.execute("CREATE TABLE IF NOT EXISTS probe_lock (x INTEGER)")
    try:
        # 辅助函数的引擎带与生产一致的连接 PRAGMA（含 WAL 切换）
        with pytest.raises(database.StartupError, match="写锁"):
            _run_init_with_engine(db_path, lock_timeout_ms=300)
    finally:
        holder.execute("DROP TABLE probe_lock")
        holder.commit()
        holder.close()

    # 超时中止后库未被部分升级（无迁移台账）；锁释放后正常完成
    eng = _open_db(db_path)
    try:
        with eng.begin() as c:
            tables = {r[0] for r in c.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table'")).all()}
            assert "schema_migrations" not in tables
    finally:
        eng.dispose()
    _run_init_with_engine(db_path)
    eng = _open_db(db_path)
    try:
        assert len(_migration_ledger(eng)) == 7
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# 中断恢复
# ---------------------------------------------------------------------------


def test_failure_mid_upgrade_rolls_back_and_restart_converges(tmp_path, monkeypatch):
    """升级事务中途抛错：整体回滚（无半列/半台账）；重启后干净收敛。"""
    db_path = str(tmp_path / "old.db")
    _create_rich_old_db(db_path)

    # 在第 1 步补列之后、回填/台账之前令事务失败
    orig_apply = database._apply_lightweight_migrations

    def fail_after_first_alter(conn):
        orig_apply(conn)  # 先正常跑完内部步骤……
        raise RuntimeError("模拟升级提交前进程被杀")

    monkeypatch.setattr(database, "_apply_lightweight_migrations",
                        fail_after_first_alter)

    eng = create_engine(f"sqlite:///{db_path}",
                        connect_args={"check_same_thread": False})
    monkeypatch.setattr(database, "engine", eng)
    try:
        with pytest.raises(RuntimeError, match="模拟升级提交前进程被杀"):
            database.init_db()
    finally:
        eng.dispose()

    # 整个升级事务回滚：新增列、台账均不存在，库仍是升级前一致结构
    eng = _open_db(db_path)
    try:
        with eng.begin() as c:
            assert "version" not in _table_columns(eng, "loading_actuals")
            tables = {r[0] for r in c.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table'")).all()}
            assert "schema_migrations" not in tables
            # 历史资料原样保留，未被挑选或改写
            assert c.execute(text(
                "SELECT COUNT(*) FROM slot_revisions")).scalar_one() == 3
    finally:
        eng.dispose()

    # 再次启动：从升级前结构一次性收敛，回填值正确
    monkeypatch.undo()
    _run_init_with_engine(db_path)
    eng = _open_db(db_path)
    try:
        with eng.begin() as c:
            assert "version" in _table_columns(eng, "loading_actuals")
            assert c.execute(text(
                "SELECT COUNT(*) FROM unloading_effective_ports")).scalar_one() == 2
            assert dict(c.execute(text(
                "SELECT category, last_allocated FROM revision_counters")
            ).fetchall()) == {"slots": 7, "manifests": 6}
        assert len(_migration_ledger(eng)) == 7
    finally:
        eng.dispose()


def test_half_backfilled_old_style_db_resumes_without_duplicates(tmp_path):
    """旧版本代码“跨独立事务”中断（生效区间表已建但未回填）：重启只补缺口。"""
    db_path = str(tmp_path / "old.db")
    _create_rich_old_db(db_path)

    # 模拟旧版本代码已完成的前置部分：create_all 建了新表（含空生效区间），
    # 且各 ALTER 已独立提交，只漏了冻结区间 -> 生效区间的回填。
    eng = create_engine(f"sqlite:///{db_path}")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE unloading_effective_ports ("
                       "id INTEGER PRIMARY KEY, execution_id INTEGER, "
                       "discharge_order INTEGER, min_remaining_moment FLOAT, "
                       "max_remaining_moment FLOAT)"))
        c.execute(text("ALTER TABLE loading_actuals ADD COLUMN version "
                       "INTEGER NOT NULL DEFAULT 1"))
        c.execute(text("ALTER TABLE loading_actual_events ADD COLUMN orig_minute "
                       "INTEGER NOT NULL DEFAULT 0"))
        c.execute(text("UPDATE loading_actual_events SET orig_minute = minute"))
        c.execute(text("ALTER TABLE loading_actual_events ADD COLUMN orig_crane_no "
                       "INTEGER NOT NULL DEFAULT 1"))
        c.execute(text("UPDATE loading_actual_events SET orig_crane_no = crane_no"))
        c.execute(text("ALTER TABLE loading_actual_events ADD COLUMN corrected "
                       "BOOLEAN NOT NULL DEFAULT 0"))
    eng.dispose()

    _run_init_with_engine(db_path)
    _run_init_with_engine(db_path)  # 第二次启动不得重复回填

    eng = _open_db(db_path)
    try:
        rows = eng.connect().execute(text(
            "SELECT execution_id, discharge_order FROM unloading_effective_ports "
            "ORDER BY execution_id, discharge_order")).fetchall()
        assert rows == [(1, 1), (1, 2)]
        assert len(_migration_ledger(eng)) == 7
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# 冲突库：列出修订号并中止，不挑选/不改写，重复启动行为一致
# ---------------------------------------------------------------------------


def test_conflicting_db_aborts_and_leaves_data_untouched(tmp_path):
    """历史同号多份：列出冲突修订号中止；不建唯一索引、不回填台账、不删资料。"""
    db_path = str(tmp_path / "conflict.db")
    eng0 = create_engine(f"sqlite:///{db_path}")
    with eng0.begin() as c:
        c.execute(text("CREATE TABLE slot_revisions (id INTEGER PRIMARY KEY, "
                       "revision INTEGER NOT NULL, min_moment FLOAT, "
                       "max_moment FLOAT, created_at DATETIME)"))
        c.execute(text("CREATE TABLE manifest_revisions (id INTEGER PRIMARY KEY, "
                       "revision INTEGER NOT NULL, created_at DATETIME)"))
        for rev in (2, 2, 3):
            c.execute(text("INSERT INTO slot_revisions (revision) VALUES (:r)"),
                      {"r": rev})
        c.execute(text("INSERT INTO manifest_revisions (revision) VALUES (1),(1)"))
    eng0.dispose()

    with pytest.raises(RuntimeError) as exc_info:
        _run_init_with_engine(db_path)
    msg = str(exc_info.value)
    assert "槽位资料" in msg and "2" in msg
    assert "箱清单" in msg
    assert "不任取其一" in msg

    eng = _open_db(db_path)
    try:
        with eng.begin() as c:
            # 冲突资料原样保留（未自动挑选/删除/改写）
            assert c.execute(text(
                "SELECT COUNT(*) FROM slot_revisions")).scalar_one() == 3
            tables = {r[0] for r in c.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table'")).all()}
            # 台账未写入（升级事务回滚）
            assert "schema_migrations" not in tables
            # 唯一索引未建
            assert c.execute(text(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' "
                "AND name='uq_slot_revision_no'")).scalar_one() == 0

        # 再次启动仍是同样的冲突中止（不会“第一次失败第二次放行”）
        with pytest.raises(RuntimeError, match="同号多份"):
            _run_init_with_engine(db_path)

        # 人工清理同号资料（每个修订号仅保留一份）
        with eng.begin() as c:
            c.execute(text(
                "DELETE FROM slot_revisions WHERE id NOT IN "
                "(SELECT MIN(id) FROM slot_revisions GROUP BY revision)"))
            c.execute(text(
                "DELETE FROM manifest_revisions WHERE id NOT IN "
                "(SELECT MIN(id) FROM manifest_revisions GROUP BY revision)"))
    finally:
        eng.dispose()

    # 清理后启动成功：计数器按保留的最大修订号回填
    _run_init_with_engine(db_path)
    eng2 = _open_db(db_path)
    try:
        assert len(_migration_ledger(eng2)) == 7
        with eng2.begin() as c:
            assert dict(c.execute(text(
                "SELECT category, last_allocated FROM revision_counters")
            ).fetchall()) == {"slots": 3, "manifests": 1}
    finally:
        eng2.dispose()
