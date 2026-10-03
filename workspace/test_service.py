"""端到端测试：启动真实 HTTP 服务，覆盖功能/并发/重启/审计。

运行：python3 test_service.py
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Client:
    def __init__(self, base: str):
        self.base = base

    def call(self, method: str, path: str, body=None, headers=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def get(self, path):
        return self.call("GET", path)

    def post(self, path, body, headers=None):
        return self.call("POST", path, body, headers)


class Server:
    def __init__(self):
        self.port = free_port()
        self.db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db.close()
        os.unlink(self.db.name)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "quota.http_server",
             "--host", "127.0.0.1", "--port", str(self.port),
             "--db", self.db.name],
            cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self.client = Client(f"http://127.0.0.1:{self.port}")
        self._wait_ready()

    def _wait_ready(self):
        deadline = time.time() + 15
        last = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read() if self.proc.stdout else ""
                raise RuntimeError("服务提前退出:\n" + out)
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{self.port}/api/state", timeout=1)
                return
            except Exception as e:
                last = e
                time.sleep(0.1)
        raise RuntimeError(f"服务未就绪: {last}")

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()

    def restart(self):
        self.stop()
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "quota.http_server",
             "--host", "127.0.0.1", "--port", str(self.port),
             "--db", self.db.name],
            cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self._wait_ready()


# ----------------------------------------------------------------------

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = ""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name}  {detail}")


def audit_ok(c: Client):
    code, a = c.get("/api/audit")
    failed = [x for x in a["checks"] if not x["ok"]]
    return code == 200 and a["ok"], failed


def setup_hierarchy(c: Client, cap=1000):
    c.post("/api/bootstrap", {"capacity": cap})
    _, o = c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-1"})
    _, p = c.post("/api/nodes", {"type": "project", "parent_id": "org-1", "node_id": "proj-1"})
    _, cl = c.post("/api/nodes", {"type": "client", "parent_id": "proj-1", "node_id": "cli-1"})
    return o["node_id"], p["node_id"], cl["node_id"]


# ----------------------------------------------------------------------

def test_basic_flow(srv: Server):
    print("\n[1] 基础分配 / 分层租约 / 记账守恒")
    c = srv.client
    o, p, cl = setup_hierarchy(c, 1000)

    code, _ = c.post("/api/leases", {"lessor_id": "platform", "holder_id": o, "amount": 400, "lease_id": "L-org"})
    check("平台 → 组织 400", code == 201, f"code={code}")
    code, _ = c.post("/api/leases", {"lessor_id": o, "holder_id": p, "amount": 300, "lease_id": "L-proj"})
    check("组织 → 项目 300", code == 201, f"code={code}")
    code, _ = c.post("/api/leases", {"lessor_id": p, "holder_id": cl, "amount": 200, "lease_id": "L-cli"})
    check("项目 → 客户端 200", code == 201, f"code={code}")

    st = c.get("/api/state")[1]
    bal = {n["id"]: n["available"] for n in st["nodes"]}
    check("分层余额 platform=600", bal["platform"] == 600, str(bal))
    check("分层余额 org-1=100（400 入 - 300 出）", bal["org-1"] == 100, str(bal))
    check("分层余额 proj-1=100（300 入 - 200 出）", bal["proj-1"] == 100, str(bal))
    check("分层余额 cli-1=200", bal["cli-1"] == 200, str(bal))
    check("Σ可用+消费=容量", st["total_available"] + st["total_consumed"] == 1000)

    # 超父级可用额度 → 拒绝
    code, err = c.post("/api/leases", {"lessor_id": "proj-1", "holder_id": "cli-1", "amount": 150, "lease_id": "L-over"})
    check("超出父级可用 → 409", code == 409, f"code={code} {err}")

    # 非法层级
    code, _ = c.post("/api/leases", {"lessor_id": "platform", "holder_id": "proj-1", "amount": 1})
    check("跨级授约（平台→项目）→ 400", code == 400, f"code={code}")


def test_usage_release_consumed_kept(srv: Server):
    print("\n[2] 幂等扣减；归还只退未消费，已消费不消失")
    c = srv.client
    code, u1 = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 70, "usage_event_id": "evt-1"})
    check("用量 70", code == 201, f"code={code}")
    code, r1b = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 70, "usage_event_id": "evt-1"})
    check("相同请求重试 → 200 幂等重放，不重复扣减", code == 200 and r1b.get("replayed") is True,
          f"code={code}")
    st0 = c.get("/api/state")[1]
    check("重放后总消费仍为 70", st0["total_consumed"] == 70, str(st0["total_consumed"]))
    code, _ = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 70, "usage_event_id": "evt-1",
                                           "command_id": "other-cmd"})
    check("同一回执换 command_id/不同语义 → 409", code == 409, f"code={code}")
    code, _ = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 150, "usage_event_id": "evt-2"})
    check("超租约剩余的用量 → 409", code == 409, f"code={code}")

    st = c.get("/api/state")[1]
    l = next(x for x in st["leases"] if x["id"] == "L-cli")
    check("租约剩余 = 130", l["remaining"] == 130, str(l))
    bal = {n["id"]: n["available"] for n in st["nodes"]}
    check("消费只扣持有者 cli=130", bal["cli-1"] == 130, str(bal))

    # 关闭必须自底向上：下层余量未回收时，先关上层会让上层余额为负
    code, _ = c.post("/api/leases/release", {"lease_id": "L-org"})
    check("下层仍占用时先归还组织租约 → 409", code == 409, f"code={code}")
    code, _ = c.post("/api/leases/release", {"lease_id": "L-proj"})
    check("下层仍占用时先归还项目租约 → 409", code == 409, f"code={code}")

    code, r = c.post("/api/leases/release", {"lease_id": "L-cli"})
    check("归还客户端租约", code == 201, f"code={code}")
    check("释放未消费 130", r["released_amount"] == 130, str(r))
    check("已消费 70 保留", r["consumed_amount"] == 70, str(r))

    code, r = c.post("/api/leases/release", {"lease_id": "L-proj"})
    check("再归还项目租约：释放 230（300-70 消费沿链传播）", code == 201 and r["released_amount"] == 230, str(r))
    code, r = c.post("/api/leases/release", {"lease_id": "L-org"})
    check("归还组织租约：释放 330（400-70）", code == 201 and r["released_amount"] == 330, str(r))

    st = c.get("/api/state")[1]
    check("全部关闭后平台恢复 = 容量-消费 = 930", st["nodes"][0]["available"] == 930, str(st["nodes"][0]))
    check("总消费仍为 70（未因撤销/归还消失）", st["total_consumed"] == 70, str(st["total_consumed"]))
    check("Σ可用(930)+消费(70)=1000", st["total_available"] + st["total_consumed"] == 1000)

    # 已关闭租约拒绝任何操作
    code, _ = c.post("/api/leases/usage", {"lease_id": "L-cli", "amount": 1, "usage_event_id": "evt-9"})
    check("已关闭租约不能再扣减 → 409", code == 409, f"code={code}")


def test_command_replay(srv: Server):
    print("\n[3] 命令幂等重放（同一 command_id 返回同一结果）")
    c = srv.client
    body = {"type": "organization", "parent_id": "platform", "node_id": "org-replay", "command_id": "cmd-rep-1"}
    code1, r1 = c.post("/api/nodes", body)
    code2, r2 = c.post("/api/nodes", body)
    check("首次 201、重放 200", code1 == 201 and code2 == 200, f"{code1}/{code2}")
    check("重放返回同一节点 ID", r1["node_id"] == r2["node_id"] == "org-replay")
    check("重放标记 replayed=true", r2.get("replayed") is True)

    code, _ = c.post("/api/nodes", {**body, "node_id": "org-other"})
    check("相同 command_id 不同请求体 → 409", code == 409, f"code={code}")

    # Idempotency-Key 头
    _, r3 = c.post("/api/nodes",
                   {"type": "project", "parent_id": "org-replay", "node_id": "p-hdr"},
                   headers={"Idempotency-Key": "hdr-key-1"})
    code4, r4 = c.post("/api/nodes",
                       {"type": "project", "parent_id": "org-replay", "node_id": "p-hdr"},
                       headers={"Idempotency-Key": "hdr-key-1"})
    check("Header 幂等键同样生效", code4 == 200 and r4["node_id"] == r3["node_id"])


def test_capacity_shrink(srv: Server):
    print("\n[4] 父级缩容保护（平台 & 中间层 revoke）")
    c = srv.client
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-s"})
    c.post("/api/nodes", {"type": "project", "parent_id": "org-s", "node_id": "proj-s"})
    c.post("/api/nodes", {"type": "client", "parent_id": "proj-s", "node_id": "cli-s"})
    c.post("/api/leases", {"lessor_id": "platform", "holder_id": "org-s", "amount": 200, "lease_id": "Ls-org"})
    c.post("/api/leases", {"lessor_id": "org-s", "holder_id": "proj-s", "amount": 120, "lease_id": "Ls-proj"})
    c.post("/api/leases", {"lessor_id": "proj-s", "holder_id": "cli-s", "amount": 80, "lease_id": "Ls-cli"})

    # 此时平台已承担：历史消费 70（前序）+ 新授出 200 = 270 不可缩
    code, _ = c.post("/api/capacity", {"capacity": 269})
    check("平台缩容到下限以下 → 409", code == 409, f"code={code}")

    # 消费 30 不改变平台余额（关闭时才退），下限仍 270；缩到 270 恰好安全
    c.post("/api/leases/usage", {"lease_id": "Ls-cli", "amount": 30, "usage_event_id": "evt-s1"})
    code, r = c.post("/api/capacity", {"capacity": 270})
    check("缩容到 270（=已承诺/消费下限）成功", code == 201, str(r))
    code, _ = c.post("/api/capacity", {"capacity": 269})
    check("继续低于下限 → 409", code == 409, f"code={code}")

    # 撤销必须自底向上；已消费部分不会退回
    code, _ = c.post("/api/leases/revoke", {"lease_id": "Ls-org"})
    check("子级仍有占用时撤销上层 → 409", code == 409, f"code={code}")
    code, r = c.post("/api/leases/revoke", {"lease_id": "Ls-cli"})  # 退 50 给 proj
    check("撤销客户端租约释放 50（80-30）", code == 201 and r["released_amount"] == 50, str(r))
    code, r = c.post("/api/leases/revoke", {"lease_id": "Ls-proj"})  # proj 余 90，退 90
    check("撤销项目租约释放 90（120-30）", code == 201 and r["released_amount"] == 90, str(r))
    code, r = c.post("/api/leases/revoke", {"lease_id": "Ls-org"})
    check("撤销组织租约释放 170（200-30）", code == 201 and r["released_amount"] == 170, str(r))
    st = c.get("/api/state")[1]
    check("消费累计 = 70(前序) + 30 = 100，且永久保留", st["total_consumed"] == 100,
          str(st["total_consumed"]))


def test_expiry_and_offline(srv: Server):
    print("\n[5] 租约到期（含级联）与客户端离线")
    c = srv.client
    # 前序测试把全局容量缩到 180，先扩回 1000
    code, _ = c.post("/api/capacity", {"capacity": 1000})
    assert code == 201, f"恢复容量失败 {code}"
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-e"})
    c.post("/api/nodes", {"type": "project", "parent_id": "org-e", "node_id": "proj-e"})
    c.post("/api/nodes", {"type": "client", "parent_id": "proj-e", "node_id": "cli-e"})

    # 项目租约 1 秒到期（早于客户端 3600 秒）：
    # 到期先挂起(pending_expired)，不抽走仍支撑下游的余量；
    # 待客户端租约释放后，项目租约才结算退款。
    c.post("/api/leases", {"lessor_id": "platform", "holder_id": "org-e", "amount": 100, "lease_id": "Le-org", "ttl_seconds": 3600})
    c.post("/api/leases", {"lessor_id": "org-e", "holder_id": "proj-e", "amount": 80, "lease_id": "Le-proj", "ttl_seconds": 1})
    c.post("/api/leases", {"lessor_id": "proj-e", "holder_id": "cli-e", "amount": 50, "lease_id": "Le-cli", "ttl_seconds": 3600})
    c.post("/api/leases/usage", {"lease_id": "Le-cli", "amount": 20, "usage_event_id": "evt-e1"})

    def lease_map():
        st = c.get("/api/state")[1]
        return {x["id"]: x for x in st["leases"] if x["id"].startswith("Le-")}

    # 等待项目租约到期挂起（客户端租约仍 active，项目不会被强退）
    deadline = time.time() + 5
    while time.time() < deadline:
        lm = lease_map()
        if lm.get("Le-proj", {}).get("status") == "pending_expired":
            break
        time.sleep(0.15)
    lm = lease_map()
    check("项目租约到期 → pending_expired（不抽走支撑下游的余量）",
          lm["Le-proj"]["status"] == "pending_expired", lm["Le-proj"]["status"])
    check("下游客户端租约仍然有效（未被级联强退）",
          lm["Le-cli"]["status"] == "active", lm["Le-cli"]["status"])
    check("挂起期间不能再扣减项目租约 → 409",
          c.post("/api/leases/usage", {"lease_id": "Le-proj", "amount": 1, "usage_event_id": "evt-pending"})[0] == 409)
    ok, failed = audit_ok(c)
    check("挂起期间审计通过（无负余额、不超卖）", ok, str(failed))

    # 客户端租约归还后，项目挂起租约自动结算，只退未消费 60
    c.post("/api/leases/release", {"lease_id": "Le-cli"})
    deadline = time.time() + 5
    while time.time() < deadline:
        lm = lease_map()
        if lm["Le-proj"]["status"] == "closed":
            break
        time.sleep(0.15)
    lm = lease_map()
    check("下游释放后挂起租约结算关闭，reason=expired",
          lm["Le-proj"]["status"] == "closed" and lm["Le-proj"]["reason"] == "expired",
          str(lm["Le-proj"]))
    check("结算只退未消费（proj 退 60）", lm["Le-proj"]["remaining"] == 60, str(lm["Le-proj"]))
    ok, failed = audit_ok(c)
    check("延迟结算后审计通过", ok, str(failed))

    # 离线场景：新租约 + 离线即关闭（客户端是叶子，恒安全）
    c.post("/api/leases", {"lessor_id": "org-e", "holder_id": "proj-e", "amount": 30, "lease_id": "Le-proj2"})
    c.post("/api/leases", {"lessor_id": "proj-e", "holder_id": "cli-e", "amount": 20, "lease_id": "Le-cli2"})
    code, r = c.post("/api/clients/presence", {"client_id": "cli-e", "online": False})
    check("客户端下线", code == 201, str(r))
    st = c.get("/api/state")[1]
    l2 = next(x for x in st["leases"] if x["id"] == "Le-cli2")
    check("离线关闭其活动租约，reason=offline", l2["status"] == "closed" and l2["reason"] == "offline")
    code, _ = c.post("/api/leases", {"lessor_id": "proj-e", "holder_id": "cli-e", "amount": 1, "lease_id": "Le-blocked"})
    check("离线客户端不能获得租约 → 409", code == 409, f"code={code}")
    code, _ = c.post("/api/leases/usage", {"lease_id": "Le-cli2", "amount": 1, "usage_event_id": "evt-x"})
    check("离线客户端不能扣减 → 409", code == 409, f"code={code}")
    c.post("/api/clients/presence", {"client_id": "cli-e", "online": True})
    code, _ = c.post("/api/leases", {"lessor_id": "proj-e", "holder_id": "cli-e", "amount": 5, "lease_id": "Le-online"})
    check("重新上线后可再租约", code == 201, f"code={code}")


def test_concurrent_grants(srv: Server):
    print("\n[6] 跨层并发申请：高并发下绝不超卖")
    c = srv.client
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-c"})
    c.post("/api/nodes", {"type": "project", "parent_id": "org-c", "node_id": "proj-c"})
    for i in range(30):
        c.post("/api/nodes", {"type": "client", "parent_id": "proj-c", "node_id": f"cli-c{i}"})
    c.post("/api/leases", {"lessor_id": "platform", "holder_id": "org-c", "amount": 100, "lease_id": "Lc-org"})
    c.post("/api/leases", {"lessor_id": "org-c", "holder_id": "proj-c", "amount": 100, "lease_id": "Lc-proj"})

    # 30 个客户端各抢 10（共需 300，只有 100）
    results = []
    lock = threading.Lock()

    def worker(i):
        code, body = srv.client.post(
            "/api/leases",
            {"lessor_id": "proj-c", "holder_id": f"cli-c{i}", "amount": 10,
             "lease_id": f"Lc-c{i}", "command_id": f"cmd-grant-{i}"},
        )
        with lock:
            results.append((code, body))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(30)]
    for t in threads: t.start()
    for t in threads: t.join()
    granted = sum(1 for code, _ in results if code == 201)
    rejected = sum(1 for code, _ in results if code == 409)
    check(f"恰好 10 个成功 / 20 个被拒（实际 {granted}/{rejected}）", granted == 10 and rejected == 20,
          str([b.get("message") for cd, b in results if cd not in (201, 409)][:3]))

    st = c.get("/api/state")[1]
    bal = {n["id"]: n["available"] for n in st["nodes"]}
    check("并发后项目可用为 0（未超卖）", bal["proj-c"] == 0, str(bal))
    check("所有余额非负", all(n["available"] >= 0 for n in st["nodes"]))

    # 释放成功的 10 条租约，余量 100 回到项目，供下一步并发扣减
    for code, body in results:
        if code == 201:
            rc, _ = c.post("/api/leases/release", {"lease_id": body["lease_id"]})
            assert rc == 201, f"释放失败 {rc}"

    # 并发用量：同一租约 20 线程各扣 10，余量 100
    c.post("/api/leases", {"lessor_id": "proj-c", "holder_id": "cli-c0",
                           "amount": 100, "lease_id": "Lc-use"})
    usage_res = []

    def uworker(i):
        code, _ = srv.client.post("/api/leases/usage",
                                  {"lease_id": "Lc-use", "amount": 10, "usage_event_id": f"uce-{i}"})
        with lock:
            usage_res.append(code)

    threads = [threading.Thread(target=uworker, args=(i,)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()
    check("并发扣减恰好 10 成功 / 10 拒绝",
          usage_res.count(201) == 10 and usage_res.count(409) == 10, str(usage_res))
    st = c.get("/api/state")[1]
    l = next(x for x in st["leases"] if x["id"] == "Lc-use")
    check("租约最终消费 100、剩余 0", l["consumed"] == 100 and l["remaining"] == 0)


def test_concurrent_replay_safe(srv: Server):
    print("\n[7] 同一命令的并发重复提交只生效一次")
    c = srv.client
    c.post("/api/nodes", {"type": "organization", "parent_id": "platform", "node_id": "org-rp"})
    codes = []
    lock = threading.Lock()

    def worker():
        code, body = srv.client.post("/api/leases", {
            "lessor_id": "platform", "holder_id": "org-rp", "amount": 50,
            "lease_id": "L-rp-once", "command_id": "cmd-once-50",
        })
        with lock:
            codes.append(code)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    check("10 次相同并发提交：恰好 1 次 201，其余重放/去重",
          codes.count(201) == 1 and all(x in (200, 201) for x in codes), str(codes))
    st = c.get("/api/state")[1]
    check("组织余额只扣一次 = 50", next(n for n in st["nodes"] if n["id"] == "org-rp")["available"] == 50)


def test_restart_rebuild(srv: Server):
    print("\n[8] 服务重启：从事件账本重建当前余额")
    c = srv.client
    before = c.get("/api/state")[1]
    events_before = c.get("/api/events")[1]["events"]

    srv.restart()

    after = c.get("/api/state")[1]
    events_after = c.get("/api/events")[1]["events"]
    check("事件数重启前后一致", len(events_before) == len(events_after))
    check("全局容量重建一致", after["global_capacity"] == before["global_capacity"])
    check("节点集合重建一致",
          [n["id"] for n in after["nodes"]] == [n["id"] for n in before["nodes"]])
    check("各节点余额逐字段重建一致",
          [(n["id"], n["available"], n["consumed"]) for n in after["nodes"]]
          == [(n["id"], n["available"], n["consumed"]) for n in before["nodes"]])
    check("租约状态（含已关闭/消费/原因）重建一致",
          [(l["id"], l["status"], l["amount"], l["consumed"], l["remaining"], l["reason"])
           for l in after["leases"]]
          == [(l["id"], l["status"], l["amount"], l["consumed"], l["remaining"], l["reason"])
              for l in before["leases"]])
    check("总消费重建一致", after["total_consumed"] == before["total_consumed"])
    ok, failed = audit_ok(c)
    check("重启后审计通过（含 replay_equivalence）", ok, str(failed))

    # 重启后旧幂等键仍然有效
    code, _ = c.post("/api/leases/usage",
                     {"lease_id": "L-rp-once", "amount": 1, "usage_event_id": "uce-0"})
    # uce-0 已在并发用量测试中用于 Lc-use（不同租约）；usage id 全局去重
    check("重启后已处理过的 usage_event_id 仍被拒绝", code == 409, f"code={code}")


def test_tamper_detection(srv: Server):
    print("\n[9] 账本篡改检测（哈希链 + 重放不等价）")
    c = srv.client
    ok, _ = audit_ok(c)
    check("正常状态审计通过", ok)
    # 直接改 SQLite 里的事件载荷
    import sqlite3
    conn = sqlite3.connect(srv.db.name)
    row = conn.execute("SELECT seq, payload FROM events ORDER BY seq LIMIT 1").fetchone()
    payload = json.loads(row[1])
    if "capacity" in payload:
        payload["capacity"] += 999
    conn.execute("UPDATE events SET payload=? WHERE seq=?", (json.dumps(payload, sort_keys=True), row[0]))
    conn.commit()
    conn.close()
    code, a = c.get("/api/audit")
    chain_check = next(x for x in a["checks"] if x["name"] == "event_hash_chain")
    check("篡改事件载荷后哈希链审计失败", code == 200 and not a["ok"] and not chain_check["ok"],
          str(chain_check))


def main():
    srv = Server()
    try:
        test_basic_flow(srv)
        test_usage_release_consumed_kept(srv)
        test_command_replay(srv)
        test_capacity_shrink(srv)
        test_expiry_and_offline(srv)
        test_concurrent_grants(srv)
        test_concurrent_replay_safe(srv)
        test_restart_rebuild(srv)
        test_tamper_detection(srv)

        print("\n[终态审计]")
        code, a = srv.client.get("/api/audit")
        for chk in a["checks"]:
            print(f"  {'✓' if chk['ok'] else '✗'} {chk['name']}: {chk['detail']}")
    finally:
        srv.stop()

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
