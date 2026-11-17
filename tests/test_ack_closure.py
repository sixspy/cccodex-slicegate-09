
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

import app as appmod
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    monkeypatch.setattr("app.DB_PATH", db)
    init_db()
    yield db


@pytest.fixture
def client():
    return TestClient(app)


def login(c, username="admin", password="123456"):
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}


def lock(h, plan):
    r = TestClient(app).post("/api/ack/strategy", headers=h, json={"plan": plan})
    assert r.status_code == 200, r.text


def complete_slice(h, no="SL-T-1", date="2026-11-04", part="mid"):
    r = TestClient(app).post("/api/slices", headers=h, json={
        "slice_no": no, "biz_date": date, "partition_key": part,
        "row_count": 10, "expected_count": 10})
    assert r.status_code == 200
    return r.json()["id"]


def release_id_for(c, h, slice_no):
    rows = c.get("/api/releases", headers=h).json()["items"]
    return [r for r in rows if r["slice_no"] == slice_no][0]["id"]


# ---------- 策略簿：二选一、落簿即锁、禁止改口 ----------

def test_strategy_must_be_chosen_and_is_immutable(client):
    h = login(client)
    st = client.get("/api/ack/strategy", headers=h).json()
    assert st["locked"] is False and {x["plan"] for x in st["catalog"]} == {"甲", "乙"}
    # 未定策略：放行、签收全部拦下并留痕
    sid = complete_slice(h)
    r = client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "c1"})
    assert r.status_code == 409
    lock(h, "甲")
    # 改口为乙：409 + 偏差簿
    again = client.post("/api/ack/strategy", headers=h, json={"plan": "乙"})
    assert again.status_code == 409 and "禁止改口" in again.json()["detail"]
    # 数据库层物理禁改禁删
    with appmod.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE ack_strategy SET plan='乙' WHERE id=1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM ack_strategy WHERE id=1")
        row = conn.execute("SELECT plan,locked FROM ack_strategy WHERE id=1").fetchone()
        assert row["plan"] == "甲" and row["locked"] == 1
    dev = client.get("/api/ack/deviations", headers=h).json()["items"]
    kinds = {d["kind"] for d in dev}
    assert "策略未定先入账" in kinds and "策略改口" in kinds


def test_invalid_plan_rejected(client):
    h = login(client)
    r = client.post("/api/ack/strategy", headers=h, json={"plan": "丙"})
    assert r.status_code == 400


# ---------- 甲方案：签收成功才推水位，拒签回退未放行 ----------

def test_plan_a_reject_rolls_back_release_and_keeps_no_watermark(client):
    h = login(client)
    lock(h, "甲")
    sid = complete_slice(h, no="SL-REJ-1")
    rel = client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_a"})
    assert rel.status_code == 200 and rel.json()["watermark_pushed"] is False
    rid = release_id_for(client, h, "SL-REJ-1")
    rej = client.post(f"/api/releases/{rid}/ack", headers=h,
                      json={"result": "拒签", "note": "行数对不上"})
    assert rej.status_code == 200
    # 正式放行态回退未放行；无任何水位；拒签留痕
    sl = [s for s in client.get("/api/slices", headers=h).json()["items"]
          if s["slice_no"] == "SL-REJ-1"][0]
    assert sl["release_status"] == "未放行"
    assert client.get("/api/watermarks", headers=h).json()["items"] == []
    rel_row = [r for r in client.get("/api/releases", headers=h).json()["items"]
               if r["id"] == rid][0]
    assert rel_row["ack_status"] == "拒签"
    # 拒签后该切片允许重新放行；旧放行不会再推水位
    rel2 = client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_a"})
    assert rel2.status_code == 200
    rid2 = release_id_for(client, h, "SL-REJ-1")
    assert rid2 != rid
    client.post(f"/api/releases/{rid2}/ack", headers=h, json={"result": "签收成功"})
    wms = [w for w in client.get("/api/watermarks", headers=h).json()["items"]
           if w["source_slice"] == "SL-REJ-1"]
    assert len(wms) == 1
    recon = client.get("/api/reconciliation", headers=h).json()
    assert recon["ok"] is True


def test_plan_a_duplicate_ack_blocked(client):
    h = login(client)
    lock(h, "甲")
    sid = complete_slice(h, no="SL-DUP-1")
    client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "c"})
    rid = release_id_for(client, h, "SL-DUP-1")
    client.post(f"/api/releases/{rid}/ack", headers=h, json={"result": "签收成功"})
    dup = client.post(f"/api/releases/{rid}/ack", headers=h, json={"result": "签收成功"})
    assert dup.status_code == 409 and "重复签收" in dup.json()["detail"]
    wms = [w for w in client.get("/api/watermarks", headers=h).json()["items"]
           if w["source_slice"] == "SL-DUP-1"]
    assert len(wms) == 1


# ---------- 乙方案：放行即推水位，签收只读，拒签开回拨工单 ----------

def test_plan_b_release_pushes_watermark_and_ack_is_readonly(client):
    h = login(client)
    lock(h, "乙")
    sid = complete_slice(h, no="SL-B-1")
    rel = client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_b"})
    assert rel.json()["watermark_pushed"] is True
    wms = [w for w in client.get("/api/watermarks", headers=h).json()["items"]
           if w["source_slice"] == "SL-B-1"]
    assert len(wms) == 1  # 无签收也允许有水位，但签收列表明确显示待签收
    rid = release_id_for(client, h, "SL-B-1")
    ok = client.post(f"/api/releases/{rid}/ack", headers=h, json={"result": "签收成功"})
    assert ok.status_code == 200
    # 签收不重复推水位
    wms2 = [w for w in client.get("/api/watermarks", headers=h).json()["items"]
            if w["source_slice"] == "SL-B-1"]
    assert len(wms2) == 1
    rel_row = [r for r in client.get("/api/releases", headers=h).json()["items"]
               if r["id"] == rid][0]
    assert rel_row["ack_status"] == "已签收"
    assert client.get("/api/reconciliation", headers=h).json()["ok"] is True


def test_plan_b_reject_opens_rollback_order_without_touching_release(client):
    h = login(client)
    lock(h, "乙")
    sid = complete_slice(h, no="SL-B-REJ")
    client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_b2"})
    rid = release_id_for(client, h, "SL-B-REJ")
    rej = client.post(f"/api/releases/{rid}/ack", headers=h, json={"result": "拒签"})
    assert rej.status_code == 200
    sl = [s for s in client.get("/api/slices", headers=h).json()["items"]
          if s["slice_no"] == "SL-B-REJ"][0]
    assert sl["release_status"] == "已放行"  # 正式放行态不得直接改
    wms = [w for w in client.get("/api/watermarks", headers=h).json()["items"]
           if w["source_slice"] == "SL-B-REJ"]
    assert len(wms) == 1  # 水位保留，由回拨工单纠偏
    orders = client.get("/api/rollback-orders", headers=h).json()["items"]
    assert len(orders) == 1 and orders[0]["status"] == "待处理"
    # 工单未闭环：同消费方禁止再放行推水位
    sid2 = complete_slice(h, no="SL-B-REJ-2", part="north")
    blocked = client.post(f"/api/slices/{sid2}/release", headers=h, json={"consumer": "ads_b2"})
    assert blocked.status_code == 409 and "回拨工单" in blocked.json()["detail"]
    # 闭环后可再放行
    done = client.post(f"/api/rollback-orders/{orders[0]['id']}/resolve",
                       headers=h, json={"note": "下游重放完成"})
    assert done.status_code == 200
    again = client.post(f"/api/slices/{sid2}/release", headers=h, json={"consumer": "ads_b2"})
    assert again.status_code == 200
    assert client.get("/api/reconciliation", headers=h).json()["ok"] is True


def test_plan_b_rollback_endpoint_rejected_under_plan_a(client):
    h = login(client)
    lock(h, "甲")
    r = client.post("/api/rollback-orders/1/resolve", headers=h, json={"note": "x"})
    assert r.status_code == 409


# ---------- 并发：两名签收值班互斥排队，后到显式失败 ----------

def test_concurrent_ack_one_wins_one_gets_contention(client):
    h = login(client)
    lock(h, "甲")
    sid = complete_slice(h, no="SL-CONC-1")
    client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "c"})
    rid = release_id_for(client, h, "SL-CONC-1")

    h1 = login(TestClient(app), "admin")
    h2 = login(TestClient(app), "viewer", "view123")
    barrier = threading.Barrier(2)
    results = []

    def worker(headers, who):
        c = TestClient(app)
        barrier.wait()
        r = c.post(f"/api/releases/{rid}/ack", headers=headers,
                   json={"result": "签收成功", "note": who})
        results.append((who, r.status_code))

    t1 = threading.Thread(target=worker, args=(h1, "admin"))
    t2 = threading.Thread(target=worker, args=(h2, "viewer"))
    t1.start(); t2.start(); t1.join(); t2.join()

    statuses = sorted(s for _, s in results)
    assert statuses == [200, 409]  # 一成一争用失败，绝不允许两个都静默成功
    loser = [who for who, s in results if s == 409][0]
    assert loser in ("admin", "viewer")
    receipts = client.get("/api/ack/receipts", headers=h).json()["items"]
    assert len([x for x in receipts if x["release_id"] == rid]) == 1
    wms = [w for w in client.get("/api/watermarks", headers=h).json()["items"]
           if w["source_slice"] == "SL-CONC-1"]
    assert len(wms) == 1
    dev = client.get("/api/ack/deviations", headers=h).json()["items"]
    assert any(d["kind"] == "签收争用" and "后到值班" in d["detail"] for d in dev)


def test_concurrent_strategy_lock_one_wins(client):
    h1 = login(TestClient(app), "admin")
    h2 = login(TestClient(app), "viewer", "view123")
    barrier = threading.Barrier(2)
    out = []

    def worker(headers, plan):
        c = TestClient(app)
        barrier.wait()
        r = c.post("/api/ack/strategy", headers=headers, json={"plan": plan})
        out.append(r.status_code)

    t1 = threading.Thread(target=worker, args=(h1, "甲"))
    t2 = threading.Thread(target=worker, args=(h2, "乙"))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(out) == [200, 409]
    # 只有一路落簿，不存在混签
    with appmod.connect() as conn:
        assert conn.execute("SELECT COUNT(*) i FROM ack_strategy").fetchone()["i"] == 1


# ---------- 重复放行 / 对账读数 ----------

def test_duplicate_active_release_blocked(client):
    h = login(client)
    lock(h, "乙")
    sid = complete_slice(h, no="SL-DUPREL")
    assert client.post(f"/api/slices/{sid}/release", headers=h,
                       json={"consumer": "c"}).status_code == 200
    dup = client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "c"})
    assert dup.status_code == 409 and "重复放行" in dup.json()["detail"]


def test_reconciliation_lines_cross_read_three_sources(client):
    h = login(client)
    lock(h, "甲")
    sid = complete_slice(h, no="SL-READ-1")
    client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_r"})
    rid = release_id_for(client, h, "SL-READ-1")
    before = client.get("/api/reconciliation", headers=h).json()
    line = [l for l in before["lines"] if l["release_id"] == rid][0]
    assert line["consumed_label"] == "待签收·未消费" and line["ok"] is True
    client.post(f"/api/releases/{rid}/ack", headers=h, json={"result": "签收成功"})
    after = client.get("/api/reconciliation", headers=h).json()
    assert after["ok"] is True
    line2 = [l for l in after["lines"] if l["release_id"] == rid][0]
    assert line2["consumed_label"] == "下游已消费"
    # 三处接口同源同数
    rels = client.get("/api/releases", headers=h).json()["items"]
    rcpts = client.get("/api/ack/receipts", headers=h).json()["items"]
    wms = client.get("/api/watermarks", headers=h).json()["items"]
    assert [r for r in rels if r["id"] == rid][0]["receipt_result"] == "签收成功"
    assert any(a["release_id"] == rid for a in rcpts)
    assert any(w["source_slice"] == "SL-READ-1" for w in wms)


# ---------- 老库迁移：沿用本地文件库，历史数据不丢且能对读 ----------

def test_legacy_database_migration_and_backfill(_db):
    from app import sha
    # 先按老口径建一个含已放行+水位的库
    _db.unlink()
    legacy = sqlite3.connect(_db)
    legacy.executescript("""
    CREATE TABLE users(username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL);
    CREATE TABLE slices(id INTEGER PRIMARY KEY AUTOINCREMENT, slice_no TEXT UNIQUE, biz_date TEXT,
      partition_key TEXT, row_count INTEGER, expected_count INTEGER, completeness TEXT, release_status TEXT);
    INSERT INTO slices VALUES (1,'SL-OLD-1','2026-10-30','east',100,100,'完整','已放行');
    CREATE TABLE releases(id INTEGER PRIMARY KEY AUTOINCREMENT, slice_id INTEGER, consumer TEXT,
      status TEXT, note TEXT, created_at TEXT);
    INSERT INTO releases VALUES (1,1,'ads_old','成功','完整切片放行','2026-10-30T18:00:00');
    CREATE TABLE watermarks(id INTEGER PRIMARY KEY AUTOINCREMENT, consumer TEXT, biz_date TEXT,
      watermark TEXT, source_slice TEXT);
    INSERT INTO watermarks VALUES (1,'ads_old','2026-10-30','2026-10-30T18:00:00','SL-OLD-1');
    """)
    legacy.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
    legacy.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))
    legacy.commit(); legacy.close()

    init_db()  # 触发迁移
    c = TestClient(app)
    h = login(c)
    lock(h, "甲")
    # 甲方案历史补录：旧水位挂接补录回执，对账通过
    rels = c.get("/api/releases", headers=h).json()["items"]
    old = [r for r in rels if r["slice_no"] == "SL-OLD-1"][0]
    assert old["ack_status"] == "已签收" and old["receipt_result"] == "签收成功"
    wm = [w for w in c.get("/api/watermarks", headers=h).json()["items"]
          if w["source_slice"] == "SL-OLD-1"][0]
    assert wm["receipt_id"] == old["receipt_id"]
    assert c.get("/api/reconciliation", headers=h).json()["ok"] is True


def test_legacy_plan_b_backfills_missing_watermark(_db):
    from app import sha
    _db.unlink()
    legacy = sqlite3.connect(_db)
    legacy.executescript("""
    CREATE TABLE users(username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL);
    CREATE TABLE slices(id INTEGER PRIMARY KEY AUTOINCREMENT, slice_no TEXT UNIQUE, biz_date TEXT,
      partition_key TEXT, row_count INTEGER, expected_count INTEGER, completeness TEXT, release_status TEXT);
    INSERT INTO slices VALUES (1,'SL-OLD-2','2026-10-31','west',50,50,'完整','已放行');
    CREATE TABLE releases(id INTEGER PRIMARY KEY AUTOINCREMENT, slice_id INTEGER, consumer TEXT,
      status TEXT, note TEXT, created_at TEXT);
    INSERT INTO releases VALUES (1,1,'ads_old2','成功','完整切片放行','2026-10-31T18:00:00');
    CREATE TABLE watermarks(id INTEGER PRIMARY KEY AUTOINCREMENT, consumer TEXT, biz_date TEXT,
      watermark TEXT, source_slice TEXT);
    """)
    legacy.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
    legacy.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))
    legacy.commit(); legacy.close()

    init_db()
    c = TestClient(app)
    h = login(c)
    lock(h, "乙")
    wm = [w for w in c.get("/api/watermarks", headers=h).json()["items"]
          if w["source_slice"] == "SL-OLD-2"]
    assert len(wm) == 1 and "历史补录" in wm[0]["push_reason"]
    old = [r for r in c.get("/api/releases", headers=h).json()["items"]
           if r["slice_no"] == "SL-OLD-2"][0]
    assert old["ack_status"] == "待签收"  # 回执仍等下游，不伪造签收
    assert c.get("/api/reconciliation", headers=h).json()["ok"] is True
