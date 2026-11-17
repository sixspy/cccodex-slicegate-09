
import pytest
from fastapi.testclient import TestClient
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def login(c, username="admin", password="123456"):
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}


def lock_plan(h, plan):
    r = TestClient(app).post("/api/ack/strategy", headers=h, json={"plan": plan})
    assert r.status_code == 200, r.text
    return r


def make_complete_slice(h, no="SL-T-1", date="2026-11-04"):
    r = TestClient(app).post("/api/slices", headers=h, json={
        "slice_no": no, "biz_date": date, "partition_key": "mid",
        "row_count": 10, "expected_count": 10})
    assert r.status_code == 200
    return r.json()["id"]


def test_pipeline_plan_a_ack_drives_watermark():
    c = TestClient(app)
    assert c.get("/api/health").json()["product"] == "日切放行"
    h = login(c)
    # 策略未定：入账冻结
    pre = c.post("/api/slices/2/release", headers=h, json={"consumer": "x"})
    assert pre.status_code == 409 and "策略簿" in pre.json()["detail"]
    lock_plan(h, "甲")
    # 部分完整仍禁止放行
    bad = c.post("/api/slices/1/release", headers=h, json={"consumer": "x"})
    assert bad.status_code == 409
    sid = make_complete_slice(h)
    rel = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_x"})
    assert rel.status_code == 200
    assert rel.json()["watermark_pushed"] is False
    # 甲方案：未签收绝不能出现水位（禁止无签收显示下游已消费）
    wm = c.get("/api/watermarks", headers=h).json()["items"]
    assert not any(w["source_slice"] == "SL-T-1" for w in wm)
    rid = [r for r in c.get("/api/releases", headers=h).json()["items"]
           if r["slice_no"] == "SL-T-1"][0]["id"]
    ack = c.post(f"/api/releases/{rid}/ack", headers=h,
                 json={"result": "签收成功", "note": "ok"})
    assert ack.status_code == 200
    after = c.get("/api/watermarks", headers=h).json()["items"]
    mine = [w for w in after if w["source_slice"] == "SL-T-1"]
    assert len(mine) == 1 and mine[0]["receipt_id"] == ack.json()["receipt_id"]
    recon = c.get("/api/reconciliation", headers=h).json()
    assert recon["ok"] is True and recon["plan"] == "甲"
