"""数据库引擎、会话与启动升级。

启动升级（``init_db``）在 SQLite 上按以下规则组织，保证**同一份持久化库被
两个服务进程同时启动时只发生一次有效升级**：

1. 升级前先取得库级写事务（``BEGIN IMMEDIATE``）。先到进程拿到写锁执行
   升级；后到进程在 ``busy_timeout`` 内等待，待先到进程提交后再读库结构，
   识别“已完成”状态后直接对外服务（升级本身对后到进程为幂等空操作）。
2. 建表（``create_all``）、历史资料修订冲突检查、旧库补列回填与迁移台账
   写入全部在**同一个写事务**内完成。任一步骤失败（含进程被杀、掉电）整
   事务回滚——SQLite 的 ALTER/CREATE/INSERT 均可随事务回滚——重启后库仍
   停留在升级前的一致结构，再升级成功前不对外提供服务。
3. 每个补列/回填步骤在台账（``schema_migrations``）中留一行迁移 ID。
   台账只追加、不删除；结合各步骤自身的“缺列才补、缺行才回填”判断，重复
   启动不会重复回填或烧号。

历史库若存在同号多份资料，冲突检查在写锁内报出具体冲突修订号并中止升级
（事务回滚、不自动挑选或改写任何资料），须人工清理后再启动。
"""

from __future__ import annotations

import logging
import time

import sqlalchemy.exc
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings

logger = logging.getLogger("stowage.database")

settings = get_settings()


class StartupError(RuntimeError):
    """启动升级无法完成（库结构停留在一致状态，服务不得对外）。

    消息面向运维，包含可定位原因：升级等待写锁超时（可能存在残留进程）、
    SQL 执行失败的语句与阶段等。
    """


_connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(
    settings.database_url,
    connect_args=_connect_args,
    pool_pre_ping=True,
)

# SQLite: 默认 DEFERRED 事务，确认方案时需要 IMMEDIATE 立即拿写锁，
# 以保证并发确认不会产生冲突的已确认槽位占用。
if settings.database_url.startswith("sqlite"):

    # 启动阶段标志：为 True 时新建连接按启动等待时长设置 busy_timeout，
    # 使两个进程同时启动时后到者（含 WAL 切换）等待先到者完成升级；
    # 升级结束（成功或失败）后置回 False，业务连接恢复短超时。
    _startup_phase = True

    @event.listens_for(engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        try:
            # busy_timeout 必须先于 journal_mode 设置：WAL 切换本身也可能
            # 因先到进程持写锁而需要等待。
            timeout = (
                settings.startup_lock_timeout_ms if _startup_phase else 5000
            )
            cur.execute(f"PRAGMA busy_timeout={int(timeout)}")
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA foreign_keys=ON")
        except Exception as exc:  # pragma: no cover - 包装为可定位的启动错误
            if "locked" in str(exc).lower():
                raise StartupError(
                    "启动中止：建立数据库连接时等待写锁超时（数据库被另一进程"
                    "以写事务占用）。请确认另一服务进程的启动升级已完成，或无"
                    "残留进程占用该库后重试。原始错误：" + str(exc)
                ) from exc
            raise
        finally:
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


def init_db() -> None:
    """建表并完成旧库升级，然后才允许对外服务。

    SQLite：整段升级在一个 IMMEDIATE 写事务内串行执行（并发启动只升级一
    次，后到进程等待后识别已完成状态）；任一步失败整体回滚，库保持升级
    前的一致结构，本函数以异常中止启动。

    非 SQLite：``create_all`` + 只读冲突检查（轻量补列迁移仅针对 SQLite
    历史库，其他后端由新库直接建出最新结构）。
    """
    __import__("app.models", fromlist=["models"])  # 确保模型已注册到 Base.metadata

    if not settings.database_url.startswith("sqlite"):
        Base.metadata.create_all(bind=engine)
        check_revision_conflicts()
        return
    _init_sqlite()


def _init_sqlite() -> None:
    """SQLite 启动升级：库级写锁串行化 + 单事务原子升级。"""
    global _startup_phase
    timeout_ms = settings.startup_lock_timeout_ms
    wait_s = timeout_ms / 1000.0

    conn = None
    dialect = engine.dialect
    orig_do_begin = dialect.do_begin
    trans = None
    committed = False
    try:
        # 建立连接（触发 WAL/外键/锁等待 PRAGMA；默认 busy_timeout 已是启动
        # 等待时长，使后到进程等先到进程提交，而不是立刻 SQLITE_BUSY）。
        conn = engine.connect()

        # 让本事务以 BEGIN IMMEDIATE 开始（SQLAlchemy pysqlite 方言默认
        # 发出裸 BEGIN，首个写语句才拿写锁；启动必须先拿锁再检查/升级）。
        def _begin_immediate(dbapi_conn):  # pragma: no cover - 薄封装
            cur = dbapi_conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.close()

        dialect.do_begin = _begin_immediate
        wait_start = time.monotonic()
        try:
            trans = conn.begin()  # patched do_begin：此处即获取库级写锁
        finally:
            dialect.do_begin = orig_do_begin
        waited = time.monotonic() - wait_start
        if waited >= 0.05:
            logger.warning(
                "启动升级：等待另一进程释放写锁 %.1f 秒后进入；"
                "该进程已完成升级，本次仅核对状态。", waited
            )

        # 1) 建表（新库直接建出最新结构；旧库 IF NOT EXISTS 语义不改动
        #    已存在的表，补列由后续迁移步骤完成）。
        Base.metadata.create_all(bind=conn)

        # 2) 历史资料同号多份冲突检查：在写锁内、补唯一索引之前执行；
        #    有冲突直接抛错回滚整个事务，不任取其一也不写任何结构。
        conflicts = find_revision_conflicts(conn)
        if conflicts:
            raise RuntimeError(_conflict_message(conflicts))

        # 3) 旧库补列回填（按迁移台账跳过已完成步骤，全部随本事务提交）。
        applied = _apply_lightweight_migrations(conn)

        trans.commit()
        committed = True
        if applied:
            logger.info("启动升级完成：%s（同一写事务原子提交）", "、".join(applied))
        else:
            logger.info("数据库结构已是最新，无需升级")
    except StartupError:
        # 连接建立阶段（PRAGMA 抢写锁）已包装为可定位启动错误，直接上抛。
        raise
    except sqlalchemy.exc.OperationalError as exc:
        if "locked" in str(exc).lower():
            raise StartupError(
                f"启动中止：等待数据库写锁超过 {wait_s:.0f} 秒仍被占用。"
                "可能另一服务进程仍在执行启动升级（可等待其完成后重启本进程），"
                "或上次进程异常残留占用（确认无其他进程使用该库后清理 -wal/-shm "
                "外的残留进程再启动）。原始错误：" + str(exc.orig)
            ) from exc
        raise StartupError(
            "启动中止：建表/升级阶段执行 SQL 失败，升级事务已回滚，"
            "数据库保持升级前结构，服务未对外提供。可定位原因：" + str(exc.orig)
        ) from exc
    except RuntimeError:
        # 资料冲突等明确中止原因：保留原消息（含具体冲突修订号）。
        raise
    except Exception as exc:  # pragma: no cover - 防御性包装
        raise StartupError(
            "启动中止：建表/升级阶段发生异常，升级事务已回滚，"
            "数据库保持升级前结构，服务未对外提供。可定位原因：" + repr(exc)
        ) from exc
    finally:
        # 启动阶段结束（无论成败）：此后新建业务连接恢复 5s 短超时；
        # 丢弃池内按启动长超时建立的连接，避免其被业务复用。
        _startup_phase = False
        if trans is not None and not committed:
            # 升级未提交（含冲突中止、SQL 失败）：整体回滚，不留部分升级。
            try:
                trans.rollback()
            except Exception:  # pragma: no cover
                logger.exception("升级事务回滚失败")
        dialect.do_begin = orig_do_begin
        if conn is not None:
            conn.close()
        try:
            engine.dispose()
        except Exception:  # pragma: no cover
            logger.exception("启动后连接池清理失败")


def _conflict_message(conflicts: dict[str, list[int]]) -> str:
    parts = [
        f"{label}修订号 {'、'.join(str(r) for r in revs)} 各存在多份资料"
        for label, revs in conflicts.items()
    ]
    return (
        "启动中止：历史库存在同号多份资料（" + "；".join(parts) + "）。"
        "同一修订号对应多份不同资料，无法确定唯一资料，服务不任取其一；"
        "请人工核对并清理重复修订（每个修订号仅保留一份）后再启动。"
    )


# ---------------------------------------------------------------------------
# 资料修订冲突检查（历史同号多份资料）
# ---------------------------------------------------------------------------


def find_revision_conflicts(conn: Connection) -> dict[str, list[int]]:
    """查询各类别中同号多份的资料修订号（无冲突返回空 dict）。"""
    conflicts: dict[str, list[int]] = {}
    for table, label in (
        ("slot_revisions", "槽位资料"),
        ("manifest_revisions", "箱清单"),
    ):
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
    """启动前置检查：历史库存在同号多份资料时明确报出冲突修订号并中止。

    同一修订号对应多份不同资料时无法确定唯一资料，服务不任取其一；
    须人工核对并清理重复修订（每个修订号仅保留一份）后再启动。

    正常启动流程中本检查已包含在升级写事务内（``init_db``）；此独立入口
    保留给脚本/测试对任意引擎做只读检查，使用单独的短事务，不改动库结构。
    """
    eng = eng if eng is not None else engine
    with eng.begin() as conn:
        conflicts = find_revision_conflicts(conn)
    if conflicts:
        raise RuntimeError(_conflict_message(conflicts))


# ---------------------------------------------------------------------------
# 旧库轻量补列/回填迁移（SQLite）
# ---------------------------------------------------------------------------


# 迁移步骤 ID（即台账版本号），按顺序登记到 schema_migrations。
# 已应用的步骤在重复启动时直接跳过；台账与全部 DDL/回填在同一写事务提交。
_MIGRATION_ACTUALS_VERSION = "0001_loading_actuals_version"
_MIGRATION_EVENTS_ORIG = "0002_actual_events_orig_values"
_MIGRATION_EXEC_WINDOW = "0003_unloading_window_version"
_MIGRATION_EFFECTIVE_PORTS = "0004_unloading_effective_ports_v0"
_MIGRATION_SEAL_MANIFEST_VERSION = "0005_seal_manifest_version"
_MIGRATION_SEAL_RELEASE_VERSION = "0006_seal_release_version"
_MIGRATION_REVISION_UNIQUENESS = "0007_revision_uniqueness_and_counters"


def _read_schema(conn: Connection) -> tuple[set[str], dict[str, set[str]]]:
    """读取当前表名与各表列名（在升级事务内调用，反映本事务最新结构）。"""
    inspector = inspect(conn)
    tables = set(inspector.get_table_names())
    columns: dict[str, set[str]] = {}
    for table in tables:
        columns[table] = {c["name"] for c in inspector.get_columns(table)}
    return tables, columns


def _applied_migrations(conn: Connection) -> set[str]:
    return {
        r[0]
        for r in conn.execute(text("SELECT revision FROM schema_migrations")).all()
    }


def _record_migration(conn: Connection, migration_id: str) -> None:
    conn.execute(
        text(
            "INSERT INTO schema_migrations (revision, applied_at) "
            "VALUES (:id, CURRENT_TIMESTAMP) "
            "ON CONFLICT(revision) DO NOTHING"
        ),
        {"id": migration_id},
    )


def _apply_lightweight_migrations(conn: Connection) -> list[str]:
    """对历史 SQLite 库幂等补齐新增列/索引并回填数据（不做破坏性变更）。

    在调用方持有的 IMMEDIATE 写事务内执行，整批随事务提交或回滚。

    - 新库：``create_all`` 已直接建出最新结构，步骤体均为空操作，仅登记
      台账，保证重复启动不重复回填。
    - 历史库：缺列才 ALTER 补列，缺数据才回填（回填语句本身按“目标行不
      存在”防护），完成后登记台账。
    - 旧版本代码升级到一半（各 ALTER 原本各自独立提交）遗留的半新库：
      按现存列/表逐项判定并补齐剩余部分，仍然收敛到一致结构。
    """
    from app import models

    tables, columns = _read_schema(conn)
    applied_before = _applied_migrations(conn)
    newly_applied: list[str] = []

    def skip(migration_id: str) -> bool:
        return migration_id in applied_before

    # 1) 装船实绩版本列：历史实绩均视为第 1 版（尚未发生过更正）。
    if not skip(_MIGRATION_ACTUALS_VERSION):
        actual_cols = columns.get("loading_actuals")
        if actual_cols is not None and "version" not in actual_cols:
            conn.execute(
                text("ALTER TABLE loading_actuals ADD COLUMN version INTEGER "
                     "NOT NULL DEFAULT 1")
            )
        _record_migration(conn, _MIGRATION_ACTUALS_VERSION)
        newly_applied.append(_MIGRATION_ACTUALS_VERSION)

    # 2) 实绩事件原值留档列：已登记事件的原始分钟/岸桥即其现行分钟/岸桥。
    if not skip(_MIGRATION_EVENTS_ORIG):
        event_cols = columns.get("loading_actual_events")
        if event_cols is not None:
            if "orig_minute" not in event_cols:
                conn.execute(
                    text("ALTER TABLE loading_actual_events ADD COLUMN orig_minute "
                         "INTEGER NOT NULL DEFAULT 0")
                )
                conn.execute(
                    text("UPDATE loading_actual_events SET orig_minute = minute")
                )
            if "orig_crane_no" not in event_cols:
                conn.execute(
                    text("ALTER TABLE loading_actual_events ADD COLUMN orig_crane_no "
                         "INTEGER NOT NULL DEFAULT 1")
                )
                conn.execute(
                    text("UPDATE loading_actual_events SET orig_crane_no = crane_no")
                )
            if "corrected" not in event_cols:
                conn.execute(
                    text("ALTER TABLE loading_actual_events ADD COLUMN corrected "
                         "BOOLEAN NOT NULL DEFAULT 0")
                )
        _record_migration(conn, _MIGRATION_EVENTS_ORIG)
        newly_applied.append(_MIGRATION_EVENTS_ORIG)

    # 3) 卸箱执行区间版本列：历史执行均为区间版本 0（未发生过收紧调整）。
    if not skip(_MIGRATION_EXEC_WINDOW):
        exec_cols = columns.get("unloading_executions")
        if exec_cols is not None and "window_version" not in exec_cols:
            conn.execute(
                text("ALTER TABLE unloading_executions ADD COLUMN window_version "
                     "INTEGER NOT NULL DEFAULT 0")
            )
        _record_migration(conn, _MIGRATION_EXEC_WINDOW)
        newly_applied.append(_MIGRATION_EXEC_WINDOW)

    # 4) 卸箱生效区间回填：把每个已开始执行的冻结原定区间复制为区间版本 0
    #    的生效区间（尚未开始的执行无冻结区间，无需回填）。
    #    按执行+卸港逐条“目标不存在才插入”防护：即使历史库由旧版本代码
    #    补列后中断（生效区间表存在但未回填/只回填一半），重启也只补齐缺
    #    口，绝不重复回填。
    if not skip(_MIGRATION_EFFECTIVE_PORTS):
        if "unloading_effective_ports" in tables and "unloading_frozen_ports" in tables:
            conn.execute(
                text(
                    "INSERT INTO unloading_effective_ports "
                    "(execution_id, discharge_order, min_remaining_moment, "
                    "max_remaining_moment) "
                    "SELECT f.execution_id, f.discharge_order, "
                    "f.min_remaining_moment, f.max_remaining_moment "
                    "FROM unloading_frozen_ports f "
                    "WHERE NOT EXISTS ("
                    "SELECT 1 FROM unloading_effective_ports e "
                    "WHERE e.execution_id = f.execution_id "
                    "AND e.discharge_order = f.discharge_order)"
                )
            )
        _record_migration(conn, _MIGRATION_EFFECTIVE_PORTS)
        newly_applied.append(_MIGRATION_EFFECTIVE_PORTS)

    # 5) 封签清单方案级换签版本列：既有清单换签版本均为 0（未发生过换签）。
    if not skip(_MIGRATION_SEAL_MANIFEST_VERSION):
        manifest_cols = columns.get("seal_manifests")
        if manifest_cols is not None and "seal_version" not in manifest_cols:
            conn.execute(
                text("ALTER TABLE seal_manifests ADD COLUMN seal_version "
                     "INTEGER NOT NULL DEFAULT 0")
            )
        _record_migration(conn, _MIGRATION_SEAL_MANIFEST_VERSION)
        newly_applied.append(_MIGRATION_SEAL_MANIFEST_VERSION)

    # 6) 放行凭证方案级换签版本列：同上，历史凭证为 0。
    if not skip(_MIGRATION_SEAL_RELEASE_VERSION):
        release_cols = columns.get("seal_releases")
        if release_cols is not None and "seal_version" not in release_cols:
            conn.execute(
                text("ALTER TABLE seal_releases ADD COLUMN seal_version "
                     "INTEGER NOT NULL DEFAULT 0")
            )
        _record_migration(conn, _MIGRATION_SEAL_RELEASE_VERSION)
        newly_applied.append(_MIGRATION_SEAL_RELEASE_VERSION)

    # 7) 资料修订号唯一索引 + 计数器回填（历史库）。
    #    冲突检查已在本事务更早处中止，同号多份不会走到这里；计数器按各类
    #    别当前最大修订号初始化，保证下一份资料取得比全部历史修订更大的唯
    #    一号。台账步骤保证重复启动不重复初始化、计数器不回退（见下）。
    if not skip(_MIGRATION_REVISION_UNIQUENESS):
        if "slot_revisions" in tables and not _revision_column_unique(
            conn, "slot_revisions"
        ):
            conn.execute(
                text("CREATE UNIQUE INDEX IF NOT EXISTS uq_slot_revision_no "
                     "ON slot_revisions (revision)")
            )
        if "manifest_revisions" in tables and not _revision_column_unique(
            conn, "manifest_revisions"
        ):
            conn.execute(
                text("CREATE UNIQUE INDEX IF NOT EXISTS uq_manifest_revision_no "
                     "ON manifest_revisions (revision)")
            )
        if "revision_counters" in tables:
            for category, table in (
                (models.SLOT_REVISION_CATEGORY, "slot_revisions"),
                (models.MANIFEST_REVISION_CATEGORY, "manifest_revisions"),
            ):
                # 仅当计数器无本类别行时按历史最大修订号初始化；
                # 已存在（含已被业务推进）绝不回退、不重复烧号。
                if table not in tables:
                    max_rev = 0
                else:
                    max_rev = conn.execute(
                        text(f"SELECT COALESCE(MAX(revision), 0) FROM {table}")
                    ).scalar_one()
                conn.execute(
                    text(
                        "INSERT INTO revision_counters (category, last_allocated) "
                        "VALUES (:c, :n) ON CONFLICT(category) DO NOTHING"
                    ),
                    {"c": category, "n": max_rev},
                )
        _record_migration(conn, _MIGRATION_REVISION_UNIQUENESS)
        newly_applied.append(_MIGRATION_REVISION_UNIQUENESS)

    return newly_applied


def _revision_column_unique(conn: Connection, table: str) -> bool:
    """revision 列上是否已存在唯一索引/唯一约束（表名为内部常量，安全拼接）。

    覆盖三种来源：新库 ``create_all`` 的 UNIQUE 约束（sqlite 自动索引）、
    旧版本代码补建的唯一索引、本步骤此前补建的索引。
    """
    for row in conn.execute(text(f"PRAGMA index_list({table})")).all():
        # 列：seq, name, unique, origin, partial
        is_unique = row[2]
        index_name = row[1]
        if not is_unique:
            continue
        info = conn.execute(
            text(f"PRAGMA index_info({index_name})")
        ).all()  # seqno, cid, name
        if [r[2] for r in info] == ["revision"]:
            return True
    return False
