
import hashlib
import secrets
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 入账互斥闸：签收 / 回拨工单 / 放行写库一律串行，争用显式失败，禁止静默成功
ACK_LOCK = threading.Lock()

PLAN_A = "甲"
PLAN_B = "乙"
PLAN_SEMANTICS = {
    PLAN_A: "甲方案：签收成功才把该消费方水位往前推；签收失败则放行态回退未放行并记拒签。签收之前不存在任何下游已消费读数。",
    PLAN_B: "乙方案：放行成功立即推水位，签收只追加只读回执；拒签触发回拨工单，不得直接改正式放行态。工单未闭环前禁止该消费方再推水位。",
}


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


# ---------- 偏差簿（口径偏离拦截留痕） ----------

def log_deviation(kind: str, detail: str, operator: str = "-", conn=None):
    """签收口径偏了：拦下的同时必须留痕。
    有活动连接时同事务写入（由调用方在抛错前 commit 留痕）；无连接时独立提交。"""
    sql = ("INSERT INTO ack_deviations(kind,detail,operator,created_at)"
           " VALUES (?,?,?,?)")
    args = (kind, detail, operator, now())
    if conn is not None:
        conn.execute(sql, args)
    else:
        with connect() as dc:
            dc.execute(sql, args)


def reject(conn, status: int, message: str, kind: str, detail: str, operator: str):
    """拦截入账：偏差随业务痕迹同一事务提交留痕，事务外再抛错，避免 SQLite 写锁自死锁。"""
    log_deviation(kind, detail, operator, conn=conn)
    conn.commit()
    raise HTTPException(status, message)


# ---------- 库表 ----------

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS slices(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT UNIQUE NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          completeness TEXT NOT NULL,
          release_status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL,
          ack_status TEXT NOT NULL DEFAULT '待签收',
          updated_at TEXT,
          release_state_snapshot TEXT NOT NULL DEFAULT '已放行'
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          watermark TEXT NOT NULL,
          source_slice TEXT NOT NULL,
          push_reason TEXT NOT NULL DEFAULT '签收推水位',
          receipt_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS ack_strategy(
          id INTEGER PRIMARY KEY CHECK(id=1),
          plan TEXT NOT NULL CHECK(plan IN ('甲','乙')),
          locked INTEGER NOT NULL CHECK(locked=1),
          locked_by TEXT NOT NULL,
          locked_at TEXT NOT NULL,
          semantics TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ack_receipts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          release_id INTEGER NOT NULL UNIQUE,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          result TEXT NOT NULL CHECK(result IN ('签收成功','拒签')),
          operator TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rollback_orders(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          release_id INTEGER NOT NULL,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('待处理','已处理')),
          created_by TEXT NOT NULL,
          created_at TEXT NOT NULL,
          resolved_at TEXT,
          resolve_note TEXT
        );
        CREATE TABLE IF NOT EXISTS ack_deviations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          detail TEXT NOT NULL,
          operator TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        # 策略簿物理不可变：UPDATE/DELETE 直接被 SQLite 驳回
        conn.executescript("""
        CREATE TRIGGER IF NOT EXISTS trg_ack_strategy_no_upd BEFORE UPDATE ON ack_strategy
        BEGIN SELECT RAISE(ABORT,'签收策略簿锁定后禁止改写'); END;
        CREATE TRIGGER IF NOT EXISTS trg_ack_strategy_no_del BEFORE DELETE ON ack_strategy
        BEGIN SELECT RAISE(ABORT,'签收策略簿锁定后禁止删除'); END;
        """)
        _migrate_legacy(conn)
        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            _seed_demo(conn)


def _column_names(conn, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _migrate_legacy(conn):
    """老库补列 + 历史数据回填，保证策略锁定后对账可对读。"""
    if "ack_status" not in _column_names(conn, "releases"):
        conn.execute("ALTER TABLE releases ADD COLUMN ack_status TEXT NOT NULL DEFAULT '待签收'")
    if "updated_at" not in _column_names(conn, "releases"):
        conn.execute("ALTER TABLE releases ADD COLUMN updated_at TEXT")
    if "release_state_snapshot" not in _column_names(conn, "releases"):
        conn.execute("ALTER TABLE releases ADD COLUMN release_state_snapshot TEXT NOT NULL DEFAULT '已放行'")
    wm_cols = _column_names(conn, "watermarks")
    if "push_reason" not in wm_cols:
        conn.execute("ALTER TABLE watermarks ADD COLUMN push_reason TEXT NOT NULL DEFAULT '历史补录'")
    if "receipt_id" not in wm_cols:
        conn.execute("ALTER TABLE watermarks ADD COLUMN receipt_id INTEGER")
    # 历史放行流水补签收态：成功的待签收，拒绝的不适用
    conn.execute("UPDATE releases SET ack_status='不适用' WHERE status='拒绝' AND (ack_status IS NULL OR ack_status='')")
    conn.execute("UPDATE releases SET ack_status='待签收' WHERE status='成功' AND (ack_status IS NULL OR ack_status='')")
    conn.execute("UPDATE watermarks SET push_reason='历史补录' WHERE push_reason IS NULL OR push_reason=''")


def _seed_demo(conn):
    rows = [
        ("SL-2026-11-01-east", "2026-11-01", "east", 980, 1000, "部分完整", "未放行"),
        ("SL-2026-11-01-west", "2026-11-01", "west", 1000, 1000, "完整", "已放行"),
        ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
    ]
    for r in rows:
        conn.execute(
            "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status)"
            " VALUES (?,?,?,?,?,?,?)", r)
    # 演示口径：放行成功但下游尚未签收，水位页不得出现该消费方已消费读数
    conn.execute(
        "INSERT INTO releases(slice_id,consumer,status,note,created_at,ack_status)"
        " VALUES (2,'ads_order','成功','完整切片放行（待签收）','2026-11-01T18:00:00','待签收')")


# ---------- 模型 ----------

class LoginIn(BaseModel):
    username: str
    password: str


class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int
    expected_count: int


class ReleaseIn(BaseModel):
    consumer: str


class StrategyIn(BaseModel):
    plan: str


class AckIn(BaseModel):
    result: str
    note: str = ""


class ResolveIn(BaseModel):
    note: str = ""


# ---------- 鉴权（沿用现有站点登录） ----------

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


def get_strategy(conn) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM ack_strategy WHERE id=1").fetchone()


def require_strategy(conn, operator: str) -> sqlite3.Row:
    """入账边界：策略簿未定，任何放行/签收/工单入账一律拦下（由调用方 commit 留痕）。"""
    row = get_strategy(conn)
    if row is None:
        log_deviation("策略未定先入账", "签收策略簿尚未锁定，入账动作被拦截", operator, conn=conn)
        conn.commit()
        raise HTTPException(409, "签收策略簿尚未锁定，禁止任何入账；请先在签收策略页二选一定稿")
    return row


def insert_watermark(conn, *, consumer: str, biz_date: str, source_slice: str,
                     reason: str, receipt_id: int | None):
    """水位唯一入口：调用点即入账边界，外面不许直接 INSERT watermarks。"""
    conn.execute(
        "INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,push_reason,receipt_id)"
        " VALUES (?,?,?,?,?,?)",
        (consumer, biz_date, now(), source_slice, reason, receipt_id),
    )


PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账；签收回执闭环以策略簿定口径"

app = FastAPI(title=PRODUCT)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)


@app.on_event("startup")
def _start():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "slices": n, "db": str(DB_PATH)}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute(
            "SELECT username, role FROM users WHERE username=? AND password_sha=?",
            (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


# ---------- 切片（既有链路，不动账） ----------

@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM slices ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    comp = ("完整" if body.row_count >= body.expected_count and body.expected_count > 0
            else ("部分完整" if body.row_count > 0 else "待检"))
    with connect() as c:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status)"
                " VALUES (?,?,?,?,?,?,?)",
                (body.slice_no, body.biz_date, body.partition_key, body.row_count,
                 body.expected_count, comp, "未放行"))
            return {"id": cur.lastrowid, "completeness": comp}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "切片号已存在")


@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        comp = ("完整" if row["row_count"] >= row["expected_count"] and row["expected_count"] > 0
                else ("部分完整" if row["row_count"] > 0 else "待检"))
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
    return {"id": sid, "completeness": comp}


# ---------- 签收策略簿：二选一，落簿即锁，禁止改口 ----------

@app.get("/api/ack/strategy")
def get_ack_strategy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = get_strategy(c)
        locked = dict(row) if row else None
    return {
        "locked": locked is not None,
        "strategy": locked,
        "catalog": [{"plan": p, "semantics": s} for p, s in PLAN_SEMANTICS.items()],
    }


@app.post("/api/ack/strategy")
def lock_ack_strategy(body: StrategyIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    if body.plan not in (PLAN_A, PLAN_B):
        raise HTTPException(400, "plan 只能是 甲 或 乙")
    if not ACK_LOCK.acquire(blocking=False):
        # 锁外拦截：无活动事务，独立连接留痕
        log_deviation("签收争用", f"策略定稿争用，值班 {operator} 提交失败", operator)
        raise HTTPException(409, "签收争用：已有值班正在定稿签收策略，本次互斥失败，请重试")
    try:
        with connect() as c:
            existing = get_strategy(c)
            if existing is not None:
                reject(c, 409, f"签收策略簿已锁定为{existing['plan']}方案，禁止改口",
                       "策略改口",
                       f"策略簿已锁定为 {existing['plan']} 方案，{operator} 试图改口为 {body.plan} 方案",
                       operator)
            c.execute(
                "INSERT INTO ack_strategy(id,plan,locked,locked_by,locked_at,semantics)"
                " VALUES (1,?,1,?,?,?)",
                (body.plan, operator, now(), PLAN_SEMANTICS[body.plan]))
            _backfill_under_strategy(c, body.plan, operator)
        return {"ok": True, "plan": body.plan, "semantics": PLAN_SEMANTICS[body.plan]}
    finally:
        ACK_LOCK.release()


def _backfill_under_strategy(conn, plan: str, operator: str):
    """策略落簿时给历史放行/水位补齐闭环，让三处读数立刻可对读。"""
    legacy = conn.execute(
        "SELECT r.*, s.slice_no, s.biz_date FROM releases r JOIN slices s ON s.id=r.slice_id"
        " WHERE r.status='成功' AND r.ack_status='待签收'").fetchall()
    for r in legacy:
        wm = conn.execute(
            "SELECT * FROM watermarks WHERE consumer=? AND source_slice=? AND receipt_id IS NULL",
            (r["consumer"], r["slice_no"])).fetchall()
        if plan == PLAN_A:
            # 甲口径：存在历史水位即视同历史签收成功，补只读回执并挂接；无水位则继续待签收
            if wm:
                cur = conn.execute(
                    "INSERT INTO ack_receipts(release_id,slice_id,consumer,result,operator,note,created_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (r["id"], r["slice_id"], r["consumer"], "签收成功", "系统历史补录",
                     "历史放行已存在水位，按甲方案补录签收回执", now()))
                conn.execute(
                    "UPDATE releases SET ack_status='已签收', updated_at=? WHERE id=?",
                    (now(), r["id"]))
                conn.execute("UPDATE watermarks SET receipt_id=?, push_reason='历史补录·签收推水位（甲）'"
                             " WHERE id=?", (cur.lastrowid, wm[0]["id"]))
        else:
            # 乙口径：每条成功放行必须有一条水位，缺则补；签收仍等下游回执
            if not wm:
                insert_watermark(conn, consumer=r["consumer"], biz_date=r["biz_date"],
                                 source_slice=r["slice_no"], reason="历史补录·放行即推水位（乙）",
                                 receipt_id=None)
            else:
                conn.execute("UPDATE watermarks SET push_reason='历史补录·放行即推水位（乙）'"
                             " WHERE id=?", (wm[0]["id"],))


# ---------- 放行（入账边界受策略簿约束） ----------

@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    if not ACK_LOCK.acquire(blocking=False):
        log_deviation("签收争用", f"放行入账争用，值班 {operator} 提交失败", operator)
        raise HTTPException(409, "入账争用：已有签收值班正在提交，本次互斥失败，请重试")
    try:
        with connect() as c:
            strategy = require_strategy(c, operator)
            plan = strategy["plan"]
            row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
            if not row:
                raise HTTPException(404, "切片不存在")
            if row["completeness"] != "完整":
                c.execute(
                    "INSERT INTO releases(slice_id,consumer,status,note,created_at,ack_status)"
                    " VALUES (?,?,?,?,?,?)",
                    (sid, body.consumer, "拒绝", "完整度未达标", now(), "不适用"))
                reject(c, 409, "未完整禁止放行", "未完整放行拦截",
                       f"切片 {row['slice_no']} 完整度 {row['completeness']} 被申请放行", operator)
            active = c.execute(
                "SELECT id FROM releases WHERE slice_id=? AND status='成功' AND ack_status!='拒签'",
                (sid,)).fetchone()
            if active:
                reject(c, 409, "该切片存在有效放行（待签收/已签收），禁止重复放行",
                       "重复放行拦截",
                       f"切片 {row['slice_no']} 已存在有效放行 #{active['id']}，{operator} 再次申请",
                       operator)
            if plan == PLAN_B:
                open_order = c.execute(
                    "SELECT id FROM rollback_orders WHERE consumer=? AND status='待处理'",
                    (body.consumer,)).fetchone()
                if open_order:
                    reject(c, 409, "该消费方存在待处理回拨工单，闭环前禁止放行推水位",
                           "回拨工单未闭环",
                           f"消费方 {body.consumer} 工单 #{open_order['id']} 待处理，禁止再推水位",
                           operator)
            c.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (sid,))
            c.execute(
                "INSERT INTO releases(slice_id,consumer,status,note,created_at,ack_status,updated_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (sid, body.consumer, "成功", f"按{plan}方案放行，等待下游签收回执", now(), "待签收", now()))
            rid = c.execute("SELECT last_insert_rowid() i").fetchone()["i"]
            if plan == PLAN_B:
                insert_watermark(c, consumer=body.consumer, biz_date=row["biz_date"],
                                 source_slice=row["slice_no"], reason="放行即推水位（乙）", receipt_id=None)
            # 甲方案：此处刻意不写水位——签收成功才允许推
            return {"ok": True, "release_id": rid, "plan": plan,
                    "watermark_pushed": plan == PLAN_B}
    finally:
        ACK_LOCK.release()


# ---------- 签收回执（闭环核心） ----------

@app.post("/api/releases/{rid}/ack")
def submit_ack(rid: int, body: AckIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    if body.result not in ("签收成功", "拒签"):
        raise HTTPException(400, "result 只能是 签收成功 或 拒签")
    # 两名值班同时提交：后手写明争用，不得静默成功
    if not ACK_LOCK.acquire(blocking=False):
        log_deviation("签收争用", f"放行 #{rid} 签收争用，后到值班 {operator} 被互斥拦下", operator)
        raise HTTPException(409, "签收争用：已有签收值班正在提交该回执，本次互斥失败，请刷新后重试")
    try:
        with connect() as c:
            strategy = require_strategy(c, operator)
            plan = strategy["plan"]
            rel = c.execute("SELECT r.*, s.slice_no, s.biz_date FROM releases r"
                            " JOIN slices s ON s.id=r.slice_id WHERE r.id=?", (rid,)).fetchone()
            if not rel:
                raise HTTPException(404, "放行流水不存在")
            if rel["status"] != "成功":
                reject(c, 409, "只有成功放行才能登记签收回执",
                       "非成功放行被签收", f"放行 #{rid} 状态 {rel['status']}，签收被拦", operator)
            dup = c.execute("SELECT id FROM ack_receipts WHERE release_id=?", (rid,)).fetchone()
            if dup:
                reject(c, 409, "该放行已有签收回执，禁止重复签收；如为同时提交，你是后到一方",
                       "重复签收拦截",
                       f"放行 #{rid} 已有回执 #{dup['id']}，{operator} 重复提交", operator)
            c.execute(
                "INSERT INTO ack_receipts(release_id,slice_id,consumer,result,operator,note,created_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (rid, rel["slice_id"], rel["consumer"], body.result, operator, body.note, now()))
            receipt_id = c.execute("SELECT last_insert_rowid() i").fetchone()["i"]

            if plan == PLAN_A:
                if body.result == "签收成功":
                    # 甲：签收成功才推水位，这是甲口径下水位的唯一推进点
                    c.execute("UPDATE releases SET ack_status='已签收', updated_at=? WHERE id=?", (now(), rid))
                    insert_watermark(c, consumer=rel["consumer"], biz_date=rel["biz_date"],
                                     source_slice=rel["slice_no"], reason="签收成功推水位（甲）",
                                     receipt_id=receipt_id)
                else:
                    # 甲：拒签 → 正式放行态回退未放行，绝不允许保留已放行再推水位
                    # 旧流水快照冻结在「未放行」，后续同切片重放行不会翻旧账
                    c.execute("UPDATE releases SET ack_status='拒签', updated_at=?,"
                              " release_state_snapshot='未放行' WHERE id=?", (now(), rid))
                    c.execute("UPDATE slices SET release_status='未放行' WHERE id=?", (rel["slice_id"],))
                    log_deviation("拒签回退",
                                  f"放行 #{rid} 被 {operator} 拒签，切片 {rel['slice_no']} 回退未放行",
                                  operator, conn=c)
            else:
                # 乙：回执只读追加，正式放行态与水位一律不动
                c.execute("UPDATE releases SET ack_status=?, updated_at=? WHERE id=?",
                          ("已签收" if body.result == "签收成功" else "拒签", now(), rid))
                if body.result == "拒签":
                    c.execute(
                        "INSERT INTO rollback_orders(release_id,slice_id,consumer,status,created_by,created_at)"
                        " VALUES (?,?,?,'待处理',?,?)",
                        (rid, rel["slice_id"], rel["consumer"], operator, now()))
                    log_deviation("拒签回拨工单",
                                  f"放行 #{rid} 被 {operator} 拒签，已开回拨工单，正式放行态保持不变",
                                  operator, conn=c)
            return {"ok": True, "receipt_id": receipt_id, "plan": plan, "result": body.result}
    finally:
        ACK_LOCK.release()


@app.post("/api/rollback-orders/{oid}/resolve")
def resolve_order(oid: int, body: ResolveIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    if not ACK_LOCK.acquire(blocking=False):
        log_deviation("签收争用", f"回拨工单 #{oid} 处理争用，值班 {operator} 被互斥拦下", operator)
        raise HTTPException(409, "工单争用：已有值班正在处理该回拨工单，本次互斥失败")
    try:
        with connect() as c:
            strategy = require_strategy(c, operator)
            if strategy["plan"] != PLAN_B:
                reject(c, 409, "回拨工单是乙方案专属口径",
                       "口径越权", f"{strategy['plan']}方案下不存在回拨工单闭环动作", operator)
            order = c.execute("SELECT * FROM rollback_orders WHERE id=?", (oid,)).fetchone()
            if not order:
                raise HTTPException(404, "回拨工单不存在")
            if order["status"] == "已处理":
                reject(c, 409, "该回拨工单已闭环，禁止重复处理",
                       "重复工单闭环", f"工单 #{oid} 已处理，{operator} 重复提交", operator)
            c.execute("UPDATE rollback_orders SET status='已处理', resolved_at=?, resolve_note=? WHERE id=?",
                      (now(), body.note, oid))
            return {"ok": True}
    finally:
        ACK_LOCK.release()


# ---------- 只读查询：签收列表 / 放行详情 / 水位 / 偏差，全部同源可对读 ----------

@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""
            SELECT r.*, s.slice_no, s.biz_date, s.release_status AS slice_release_status,
                   a.id AS receipt_id, a.result AS receipt_result, a.operator AS receipt_operator,
                   a.created_at AS receipt_at, a.note AS receipt_note,
                   (SELECT GROUP_CONCAT(id || ':' || status) FROM rollback_orders o
                      WHERE o.release_id=r.id) AS rollback_orders
            FROM releases r JOIN slices s ON s.id=r.slice_id
            LEFT JOIN ack_receipts a ON a.release_id=r.id
            ORDER BY r.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/watermarks")
def list_wm(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM watermarks ORDER BY id DESC").fetchall()
        blocked = [r["consumer"] for r in c.execute(
            "SELECT DISTINCT consumer FROM rollback_orders WHERE status='待处理'").fetchall()]
    return {"items": [dict(r) for r in rows], "blocked_consumers": blocked}


@app.get("/api/ack/receipts")
def list_receipts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""
            SELECT a.*, r.ack_status, s.slice_no
            FROM ack_receipts a
            JOIN releases r ON r.id=a.release_id
            JOIN slices s ON s.id=a.slice_id
            ORDER BY a.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/rollback-orders")
def list_orders(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""
            SELECT o.*, s.slice_no FROM rollback_orders o
            JOIN slices s ON s.id=o.slice_id ORDER BY o.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/ack/deviations")
def list_deviations(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM ack_deviations ORDER BY id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/reconciliation")
def reconciliation(authorization: str | None = Header(default=None)):
    """水位页 / 签收列表 / 放行详情三处读数的统一对读口径。"""
    require_user(authorization)
    with connect() as c:
        strategy = get_strategy(c)
        plan = strategy["plan"] if strategy else None
        releases = c.execute("""
            SELECT r.*, s.slice_no, s.release_status AS slice_release_status
            FROM releases r JOIN slices s ON s.id=r.slice_id
            WHERE r.status='成功' ORDER BY r.id""").fetchall()
        receipts = {r["release_id"]: dict(r)
                    for r in c.execute("SELECT * FROM ack_receipts").fetchall()}
        watermarks = c.execute("SELECT * FROM watermarks").fetchall()
        orders = c.execute("SELECT * FROM rollback_orders").fetchall()
        deviation_count = c.execute("SELECT COUNT(*) i FROM ack_deviations").fetchone()["i"]

    lines: list[dict[str, Any]] = []
    violations: list[str] = []
    for r in releases:
        rcpt = receipts.get(r["id"])
        if plan == PLAN_A and rcpt is not None and rcpt["result"] == "拒签":
            # 甲方案拒签行只按回执精确归属水位，避免把同切片重放行的新水位误算到旧行
            wms = [dict(w) for w in watermarks if w["receipt_id"] == rcpt["id"]]
        else:
            wms = [dict(w) for w in watermarks
                   if w["consumer"] == r["consumer"] and w["source_slice"] == r["slice_no"]]
        rel_orders = [dict(o) for o in orders if o["release_id"] == r["id"]]
        open_order = any(o["status"] == "待处理" for o in rel_orders)
        line = {
            "release_id": r["id"], "slice_no": r["slice_no"], "consumer": r["consumer"],
            "slice_release_status": r["slice_release_status"],
            "release_state_snapshot": r["release_state_snapshot"],
            "ack_status": r["ack_status"],
            "receipt": rcpt, "watermarks": wms, "rollback_orders": rel_orders,
            "consumed_label": "", "ok": True, "problems": [],
        }
        if plan == PLAN_A:
            if rcpt is None:
                # 无签收 → 绝不能显示下游已消费，也绝不能有水位
                line["consumed_label"] = "待签收·未消费"
                if wms:
                    line["ok"] = False
                    line["problems"].append("甲方案：未签收却已存在水位（无签收显示已消费）")
            elif rcpt["result"] == "签收成功":
                line["consumed_label"] = "下游已消费"
                if len(wms) != 1:
                    line["ok"] = False
                    line["problems"].append(f"甲方案：签收成功应恰有一条水位，实际 {len(wms)} 条")
                elif wms[0]["receipt_id"] != rcpt["id"]:
                    line["ok"] = False
                    line["problems"].append("水位未挂接对应签收回执")
            else:
                # 甲方案拒签：该流水快照必须冻结在未放行，且全链路不得残留任何水位
                line["consumed_label"] = "拒签·已回退未放行"
                if wms:
                    line["ok"] = False
                    line["problems"].append("甲方案：拒签后仍残留水位")
                if r["release_state_snapshot"] != "未放行":
                    line["ok"] = False
                    line["problems"].append("甲方案：拒签后正式放行态未回退")
        elif plan == PLAN_B:
            line["consumed_label"] = "已推水位·" + (
                rcpt["result"] if rcpt else "待签收")
            if len(wms) != 1:
                line["ok"] = False
                line["problems"].append(f"乙方案：成功放行应恰有一条水位，实际 {len(wms)} 条")
            if rcpt and rcpt["result"] == "拒签" and not rel_orders:
                line["ok"] = False
                line["problems"].append("乙方案：拒签后缺少回拨工单")
            if open_order:
                line["consumed_label"] = "拒签·回拨工单待处理（暂停再推水位）"
            if rcpt and rcpt["result"] == "拒签" and r["release_state_snapshot"] != "已放行":
                line["ok"] = False
                line["problems"].append("乙方案：拒签不得直接改正式放行态")
        else:
            line["consumed_label"] = "策略未锁定"
            if wms:
                line["ok"] = False
                line["problems"].append("策略未定却存在水位入账")
        if not line["ok"]:
            for p in line["problems"]:
                violations.append(f"放行 #{r['id']}（{r['slice_no']}/{r['consumer']}）：{p}")
        lines.append(line)

    return {
        "plan": plan,
        "strategy_semantics": PLAN_SEMANTICS.get(plan) if plan else "签收策略簿尚未锁定，入账已冻结",
        "ok": not violations,
        "lines": lines,
        "violations": violations,
        "counts": {
            "releases": len(releases),
            "receipts": len(receipts),
            "watermarks": len(watermarks),
            "open_rollback_orders": sum(1 for o in orders if o["status"] == "待处理"),
            "deviations": deviation_count,
        },
    }


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
