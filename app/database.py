"""数据库引擎、会话与启动升级（建表、冲突检查、旧库补列回填）。

同一份持久化 SQLite 库可能被两个服务进程同时启动。启动升级遵循：

* **一次有效升级**：升级在单条 ``BEGIN IMMEDIATE`` 写事务内完成（建表、
  资料修订冲突检查、旧库补列与回填、升级标记落库同一事务）。并发启动时
  第二个进程在 SQLite 写锁外等待，先到者提交后进入，读到升级标记即直接
  服务，不重复升级、不重复回填、不烧号；
* **中断一致**：SQLite 的 DDL 同样事务化。升级任一步失败/被杀死，整笔
  事务回滚，库恢复到升级前结构；再次启动重新升级。标记与全部结构变更在
  同一事务提交，库上要么看不到标记（未完成），要么看到完整新结构
  （已完成），不存在“部分升级”的中间状态对外提供服务；
* **冲突中止**：历史库同号多份资料仍在升级事务内列出冲突修订号并中止，
  不自动挑选或改写；
* **定位清晰**：升级各阶段失败时异常信息标明所处阶段与底层原因，
  长期拿不到写锁时提示并发占用/陈旧 -wal/-shm。
"""

from __future__ import annotations

import logging

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings

logger = logging.getLogger("stowage.database")

settings = get_settings()

# 升级等待并发写锁的时长（毫秒）：旧库补列回填一般秒级完成；超时仍失败时
# 明确报错而不是让服务带着未升级结构启动。
_MIGRATION_BUSY_TIMEOUT_MS = 60000
# 长期被占（如另一进程恰在升级中且接近超时）时有限重试
_MIGRATION_LOCK_RETRIES = 3

_connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(
    settings.database_url,
    connect_args=_connect_args,
    pool_pre_ping=True,
)


def _is_sqlite(eng: Engine) -> bool:
    return eng.dialect.name == "sqlite"


# SQLite: 默认 DEFERRED 事务，确认方案时需要 IMMEDIATE 立即拿写锁，
# 以保证并发确认不会产生冲突的已确认槽位占用。
if _is_sqlite(engine):

    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA busy_timeout=5000")
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA foreign_keys=ON")
        # 并发确认时第二个写连接等待写锁（毫秒），避免立刻 SQLITE_BUSY
        cur.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 启动升级
# ---------------------------------------------------------------------------

# 当前库结构版本：新库直接建出最新结构并写入该版本号；历史库升级完成后
# 写入该版本号。今后再有结构演进，递增本号并在 _upgrade_schema 内补步骤。
SCHEMA_VERSION = 1
_SCHEMA_VERSION_TABLE = "schema_versions"


def _sqlite_migration_engine(eng: Engine | None = None) -> Engine:
    """升级专用引擎：每条事务都以 BEGIN IMMEDIATE 开始。

    与业务引擎隔离：业务引擎保留 DEFERRED（读不阻塞），仅启动升级需要
    立即拿写锁。驱动 isolation_level 置 None 关闭其隐式事务管理，由
    ``begin`` 事件显式发出 BEGIN IMMEDIATE（SQLAlchemy 文档给出的 SQLite
    事务锁用法）。URL 取自传入/当前业务引擎，保证临时引擎（测试）也指向
    同一个库文件。
    """
    eng = eng if eng is not None else engine
    mig_engine = create_engine(
        eng.url,
        connect_args={"check_same_thread": False, "timeout": _MIGRATION_BUSY_TIMEOUT_MS / 1000},
        pool_pre_ping=True,
    )

    @event.listens_for(mig_engine, "connect", insert=True)
    def _manual_commit_mode(dbapi_conn, _record):
        dbapi_conn.isolation_level = None

    @event.listens_for(mig_engine, "connect")
    def _migration_pragmas(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        try:
            cur.execute(f"PRAGMA busy_timeout={_MIGRATION_BUSY_TIMEOUT_MS}")
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA foreign_keys=ON")
        finally:
            cur.close()

    @event.listens_for(mig_engine, "begin")
    def _begin_immediate(conn):
        conn.exec_driver_sql("BEGIN IMMEDIATE")

    return mig_engine


def init_db() -> None:
    """启动升级入口（容器入口、本地初始化与 API lifespan 均调用，幂等）。

    SQLite：单进程内/多进程并发调用均安全——升级由 IMMEDIATE 写事务
    串行，后到者读到版本标记即跳过；任一步失败整体回滚，可安全重启。
    非 SQLite：沿用各语句独立事务的轻量补齐（结构补列均为幂等操作）。
    """
    from app import models  # noqa: F401  确保模型已注册

    if _is_sqlite(engine):
        _init_sqlite()
    else:
        Base.metadata.create_all(bind=engine)
        check_revision_conflicts()
        with engine.begin() as conn:
            _apply_lightweight_migrations(conn)


def _init_sqlite() -> None:
    """SQLite 启动升级：IMMEDIATE 单事务串行化，失败可恢复。"""
    from contextlib import ExitStack

    last_locked_err: Exception | None = None
    for attempt in range(1, _MIGRATION_LOCK_RETRIES + 1):
        mig_engine = _sqlite_migration_engine(engine)
        try:
            with ExitStack() as stack:
                try:
                    conn = stack.enter_context(mig_engine.begin())
                except Exception as exc:  # noqa: BLE001 - BEGIN IMMEDIATE 等待写锁超时
                    if _is_lock_error(exc):
                        last_locked_err = exc
                        logger.warning(
                            "启动升级等待写锁超时（第 %s/%s 次），稍后重试：%r",
                            attempt, _MIGRATION_LOCK_RETRIES, exc,
                        )
                        continue
                    raise RuntimeError(
                        "启动中止：数据库升级无法开始（取得写锁前失败），"
                        "库未被改动，修复后可重新启动。底层原因：" + repr(exc)
                    ) from exc
                # 此后任一步失败（含被杀死），退出 with 时整笔 IMMEDIATE
                # 事务回滚：DDL/DML/版本标记一并撤销，库恢复升级前结构。
                applied = _schema_versions_applied(conn)
                if SCHEMA_VERSION in applied:
                    logger.info("数据库结构已是版本 %s，跳过启动升级", SCHEMA_VERSION)
                    return
                _upgrade_schema(conn)
            logger.info("数据库启动升级完成，结构版本 %s", SCHEMA_VERSION)
            return
        except _SchemaLockedError as exc:
            last_locked_err = exc.__cause__
            logger.warning(
                "启动升级等待写锁超时（第 %s/%s 次），稍后重试：%r",
                attempt, _MIGRATION_LOCK_RETRIES, exc.__cause__,
            )
            continue
        finally:
            mig_engine.dispose()
    raise RuntimeError(
        "启动中止：升级数据库时长期无法取得写锁（SQLITE_BUSY/SQLITE_LOCKED）。"
        "可能原因：另一服务进程/手工连接正长时间占用该库未提交，或异常退出遗留"
        "了需恢复的 WAL；请确认没有其他进程占用，必要时检查并清理同目录下的 "
        "-wal/-shm 文件后重试。底层原因：" + repr(last_locked_err)
    )


class _SchemaLockedError(Exception):
    """升级期间等待 SQLite 写锁超时（可安全重试整次升级）。"""


def _schema_versions_applied(conn) -> set[int]:
    """已落库的结构版本集合（schema_versions 尚不存在时为空集）。"""
    if _SCHEMA_VERSION_TABLE not in inspect(conn).get_table_names():
        return set()
    rows = conn.execute(
        text(f"SELECT version FROM {_SCHEMA_VERSION_TABLE}")
    ).all()
    return {int(r[0]) for r in rows}


def _upgrade_schema(conn) -> None:
    """在已持有的 IMMEDIATE 写事务内执行全部升级步骤（整体原子）。

    任一步抛错则整个事务回滚，库恢复升级前结构，不留下部分升级。
    """
    # 1. 版本表与全部最新结构建表（checkfirst：旧库已存在的表不动）。
    #    建表与后续步骤同一事务：中途失败连同本步骤一并回滚。
    _run_step(conn, "创建版本表", lambda: conn.execute(text(
        f"CREATE TABLE IF NOT EXISTS {_SCHEMA_VERSION_TABLE} ("
        "version INTEGER PRIMARY KEY, "
        "applied_at DATETIME NOT NULL"
        ")"
    )))
    _run_step(conn, "创建缺失表", lambda: Base.metadata.create_all(bind=conn, checkfirst=True))

    # 2. 资料修订冲突检查（历史同号多份资料：列出冲突修订号并中止，
    #    不任取其一、不自动改写；事务回滚，库维持原状）。
    conflicts = find_revision_conflicts(conn)
    if conflicts:
        parts = [
            f"{label}修订号 {'、'.join(str(r) for r in revs)} 各存在多份资料"
            for label, revs in conflicts.items()
        ]
        raise RuntimeError(
            "启动中止：历史库存在同号多份资料（" + "；".join(parts) + "）。"
            "同一修订号对应多份不同资料，无法确定唯一资料，服务不任取其一；"
            "请人工核对并清理重复修订（每个修订号仅保留一份）后再启动。"
            "本次升级未提交，库结构与数据保持升级前状态，可清理后重启。"
        )

    # 3. 旧库补列与回填、唯一索引与计数器补齐（幂等）。
    _apply_lightweight_migrations(conn)

    # 4. 标记当前版本（与全部结构变更同一事务提交：见标记即见完整结构）。
    from datetime import datetime, timezone

    _run_step(conn, "写入结构版本", lambda: conn.execute(
        text(
            f"INSERT OR IGNORE INTO {_SCHEMA_VERSION_TABLE} (version, applied_at) "
            "VALUES (:v, :ts)"
        ),
        {"v": SCHEMA_VERSION, "ts": datetime.now(timezone.utc).isoformat(sep=" ")},
    ))


def _run_step(conn, label: str, action) -> None:
    """执行一个升级步骤，把底层异常包装成可定位的启动错误。"""
    try:
        action()
    except _SchemaLockedError:
        raise
    except Exception as exc:  # noqa: BLE001 - 启动错误需保留中文上下文
        if _is_lock_error(exc):
            raise _SchemaLockedError(label) from exc
        raise RuntimeError(
            f"启动中止：数据库升级步骤【{label}】失败，本次升级未提交，"
            f"库保持升级前状态，修复后可重新启动。底层原因：{exc!r}"
        ) from exc


def _is_lock_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return "database is locked" in msg or "database table is locked" in msg


# ---------------------------------------------------------------------------
# 资料修订冲突检查
# ---------------------------------------------------------------------------


def find_revision_conflicts(conn) -> dict[str, list[int]]:
    """查询各类别中同号多份的资料修订号（无冲突返回空 dict）。

    表尚不存在（全新库尚未建表阶段）时视为无冲突。
    """
    inspector = inspect(conn)
    existing = set(inspector.get_table_names())
    conflicts: dict[str, list[int]] = {}
    for table, label in (
        ("slot_revisions", "槽位资料"),
        ("manifest_revisions", "箱清单"),
    ):
        if table not in existing:
            continue
        rows = conn.execute(
            text(
                f"SELECT revision FROM {table} "
                "GROUP BY revision HAVING COUNT(*) > 1 ORDER BY revision"
            )
        ).all()
        if rows:
            conflicts[label] = [int(r[0]) for r in rows]
    return conflicts


def check_revision_conflicts(eng=None) -> None:
    """独立冲突检查（供手工初始化与测试直接调用）。

    正常启动路径中冲突检查已并入单事务升级（见 ``_upgrade_schema``）。
    同一修订号对应多份不同资料时无法确定唯一资料，服务不任取其一；
    须人工核对并清理重复修订（每个修订号仅保留一份）后再启动。
    """
    eng = eng if eng is not None else engine
    with eng.begin() as conn:
        conflicts = find_revision_conflicts(conn)
    if not conflicts:
        return
    parts = [
        f"{label}修订号 {'、'.join(str(r) for r in revs)} 各存在多份资料"
        for label, revs in conflicts.items()
    ]
    raise RuntimeError(
        "启动中止：历史库存在同号多份资料（" + "；".join(parts) + "）。"
        "同一修订号对应多份不同资料，无法确定唯一资料，服务不任取其一；"
        "请人工核对并清理重复修订（每个修订号仅保留一份）后再启动。"
    )


# ---------------------------------------------------------------------------
# 旧库轻量迁移（补列/回填/唯一索引/计数器）
# ---------------------------------------------------------------------------


def _apply_lightweight_migrations(conn) -> None:
    """对已存在的库幂等补齐后续版本新增列（不做破坏性变更）。

    在调用方已持有的事务/连接内执行（SQLite 启动路径为单条 IMMEDIATE
    写事务；非 SQLite 路径由 init_db 包在 ``engine.begin()`` 中）：

    * 新库由 ``create_all`` 直接建出最新结构，下列检查均跳过；
    * 历史库补列并回填原值；所有回填均按“尚未回填”的条件执行，重复
      调用不覆盖既有值、不重复插入，故中断重启安全；
    * 每次更正的版本链与原始事件留档列在此补齐。
    """
    inspector = inspect(conn)

    def columns(table: str) -> set[str]:
        if table not in inspector.get_table_names():
            return set()
        return {c["name"] for c in inspector.get_columns(table)}

    def table_names() -> set[str]:
        return set(inspector.get_table_names())

    # -- 装船实绩：版本列与原始事件留档列 --------------------------------
    added_actuals = columns("loading_actuals")
    if added_actuals and "version" not in added_actuals:
        _run_step(conn, "补列 loading_actuals.version", lambda: conn.execute(
            text("ALTER TABLE loading_actuals ADD COLUMN version INTEGER "
                 "NOT NULL DEFAULT 1")
        ))
        # 历史库：已有实绩均视为第 1 版（尚未发生过更正）。补列 DEFAULT 1
        # 已逐行写入原值，这里无需也不会再次改写。

    added_events = columns("loading_actual_events")
    if added_events:
        # 三个留档列各自补列（可能是旧版本多事务迁移中断后只补了一部分）。
        if "orig_minute" not in added_events:
            _run_step(conn, "补列 loading_actual_events.orig_minute", lambda: conn.execute(
                text("ALTER TABLE loading_actual_events ADD COLUMN orig_minute "
                     "INTEGER NOT NULL DEFAULT 0")
            ))
        if "orig_crane_no" not in added_events:
            _run_step(conn, "补列 loading_actual_events.orig_crane_no", lambda: conn.execute(
                text("ALTER TABLE loading_actual_events ADD COLUMN orig_crane_no "
                     "INTEGER NOT NULL DEFAULT 1")
            ))
        if "corrected" not in added_events:
            _run_step(conn, "补列 loading_actual_events.corrected", lambda: conn.execute(
                text("ALTER TABLE loading_actual_events ADD COLUMN corrected "
                     "BOOLEAN NOT NULL DEFAULT 0")
            ))
        # 原值回填（含旧版本补列后、回填前中断的半升级库恢复）。不变量：
        # 未更正事件的原始值恒等于现行值（只有更正会改现行值，且更正同时
        # 置 corrected=1 并保留原值）。按该不变量对账：
        #   * 全新/一致库：无行被更新（幂等，重复启动不改任何值）；
        #   * 未回填的历史行（orig 列为补列默认值）：回填为现行值；
        #   * 已更正行（corrected=1）：永不触碰，归档原值保留。
        _run_step(conn, "回填 orig_minute", lambda: conn.execute(
            text("UPDATE loading_actual_events SET orig_minute = minute "
                 "WHERE corrected = 0 AND orig_minute <> minute")
        ))
        _run_step(conn, "回填 orig_crane_no", lambda: conn.execute(
            text("UPDATE loading_actual_events SET orig_crane_no = crane_no "
                 "WHERE corrected = 0 AND orig_crane_no <> crane_no")
        ))

    # -- 卸箱执行：区间版本列与生效区间表 --------------------------------
    # 新表由 create_all 直接建出；历史库只需补列并把冻结原定区间回填为
    # 区间版本 0 的生效区间（尚未开始的执行无冻结区间，无需回填）。
    added_exec = columns("unloading_executions")
    if added_exec and "window_version" not in added_exec:
        _run_step(conn, "补列 unloading_executions.window_version", lambda: conn.execute(
            text("ALTER TABLE unloading_executions ADD COLUMN window_version "
                 "INTEGER NOT NULL DEFAULT 0")
        ))
    if added_exec and "unloading_effective_ports" in table_names():
        # 历史库的生效区间表刚由 create_all 建出且为空：把每个已开始执行的
        # 冻结原定区间回填为区间版本 0 的生效区间。以“执行尚无生效区间
        # 行”为条件逐执行回填，重复调用/中断重启不重复插入。
        _run_step(conn, "回填 unloading_effective_ports", lambda: conn.execute(
            text(
                "INSERT INTO unloading_effective_ports "
                "(execution_id, discharge_order, min_remaining_moment, "
                "max_remaining_moment) "
                "SELECT f.execution_id, f.discharge_order, "
                "f.min_remaining_moment, f.max_remaining_moment "
                "FROM unloading_frozen_ports f "
                "WHERE NOT EXISTS ("
                "SELECT 1 FROM unloading_effective_ports e "
                "WHERE e.execution_id = f.execution_id)"
            )
        ))

    # -- 封签换签：方案级换签版本列 --------------------------------------
    # 新库由 create_all 直接建出；历史库补列，既有清单/凭证换签版本均为 0
    # （尚未发生过换签）。seal_replacements 新表同样由 create_all 建出。
    added_manifest = columns("seal_manifests")
    if added_manifest and "seal_version" not in added_manifest:
        _run_step(conn, "补列 seal_manifests.seal_version", lambda: conn.execute(
            text("ALTER TABLE seal_manifests ADD COLUMN seal_version "
                 "INTEGER NOT NULL DEFAULT 0")
        ))
    added_releases = columns("seal_releases")
    if added_releases and "seal_version" not in added_releases:
        _run_step(conn, "补列 seal_releases.seal_version", lambda: conn.execute(
            text("ALTER TABLE seal_releases ADD COLUMN seal_version "
                 "INTEGER NOT NULL DEFAULT 0")
        ))

    _migrate_revision_uniqueness(conn, inspector)


def _migrate_revision_uniqueness(conn, inspector) -> None:
    """资料修订号并发唯一分配的幂等补齐（历史库）。

    1. 为 slot_revisions.revision / manifest_revisions.revision 补唯一索引
       （新库由 create_all 的唯一约束直接建出，此处跳过）；历史库若存在
       同号多份资料，升级事务内的冲突检查已先中止并回滚，不会走到这里。
    2. revision_counters 新表由 create_all 建出；历史库按各类别当前最大
       修订号回填计数器，保证下一份资料取得比全部历史修订更大的唯一号。
       已有计数器行（已回填或已被业务递增）绝不覆盖，重复启动不回退、
       不烧号。
    """
    from app import models

    def has_unique_on(table: str, column: str) -> bool:
        if table not in inspector.get_table_names():
            return True  # 表不存在时由 create_all 按最新结构建出
        for uc in inspector.get_unique_constraints(table):
            if uc.get("column_names") == [column]:
                return True
        for ix in inspector.get_indexes(table):
            if ix.get("unique") and ix.get("column_names") == [column]:
                return True
        return False

    if not has_unique_on("slot_revisions", "revision"):
        _run_step(conn, "建唯一索引 uq_slot_revision_no", lambda: conn.execute(
            text("CREATE UNIQUE INDEX IF NOT EXISTS uq_slot_revision_no "
                 "ON slot_revisions (revision)")
        ))
    if not has_unique_on("manifest_revisions", "revision"):
        _run_step(conn, "建唯一索引 uq_manifest_revision_no", lambda: conn.execute(
            text("CREATE UNIQUE INDEX IF NOT EXISTS uq_manifest_revision_no "
                 "ON manifest_revisions (revision)")
        ))

    if "revision_counters" not in inspector.get_table_names():
        return
    for category, table in (
        (models.SLOT_REVISION_CATEGORY, "slot_revisions"),
        (models.MANIFEST_REVISION_CATEGORY, "manifest_revisions"),
    ):
        def _backfill(category=category, table=table) -> None:
            has_row = conn.execute(
                text("SELECT 1 FROM revision_counters WHERE category = :c"),
                {"c": category},
            ).first()
            if has_row is not None:
                # 已回填或计数器已被业务递增：保留现值，不回退、不烧号
                return
            max_rev = conn.execute(
                text(f"SELECT COALESCE(MAX(revision), 0) FROM {table}")
            ).scalar_one()
            conn.execute(
                text("INSERT INTO revision_counters "
                     "(category, last_allocated) VALUES (:c, :n)"),
                {"c": category, "n": max_rev},
            )

        _run_step(conn, f"回填计数器 {category}", _backfill)
