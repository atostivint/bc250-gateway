#!/usr/bin/env python3
"""End-to-end test of bc250-gateway against a fake, power-cyclable backend."""
import json
import os
import pathlib
import sys
import time

import httpx

BASE = pathlib.Path(__file__).resolve().parent / "run"
GW = os.environ.get("GW_URL", "http://127.0.0.1:8199")
LOG = BASE / "events.log"

ok_count = 0
fail_count = 0


def check(name, cond, detail=""):
    global ok_count, fail_count
    if cond:
        ok_count += 1
        print(f"  PASS  {name}")
    else:
        fail_count += 1
        print(f"  FAIL  {name}  {detail}")


def events():
    try:
        return [l.split()[0] for l in LOG.read_text().splitlines() if l.strip()]
    except FileNotFoundError:
        return []


def status(c):
    return c.get(f"{GW}/admin/status").json()


def main():
    c = httpx.Client(timeout=httpx.Timeout(120.0, connect=5.0))

    print("1. cold start: /v1/models with an empty cache wakes the node once")
    before = len([e for e in events() if e == "plug_on"])
    r = c.get(f"{GW}/v1/models")
    check("200", r.status_code == 200, str(r.status_code))
    ids = [m["id"] for m in r.json()["data"]]
    check("model list", ids == ["gpt-oss-20b-MXFP4", "Qwen3-8B-Q4_K_M"], str(ids))
    check("one wake sent", len([e for e in events() if e == "plug_on"]) == before + 1, str(events()))
    check("state ready", status(c)["state"] == "ready")

    print("2. /admin/sleep powers down then cuts the plug, in that order")
    r = c.post(f"{GW}/admin/sleep")
    check("200", r.status_code == 200, str(r.status_code))
    ev = events()
    check("poweroff before plug_off",
          "poweroff" in ev and "plug_off" in ev and ev.index("poweroff") < ev.index("plug_off"),
          str(ev))
    check("state asleep", status(c)["state"] == "asleep", str(status(c)))

    print("3. /v1/models while asleep is served from cache and sends no wake")
    before = len([e for e in events() if e == "plug_on"])
    r = c.get(f"{GW}/v1/models")
    check("200 from cache", r.status_code == 200 and len(r.json()["data"]) == 2)
    time.sleep(1.0)
    check("no wake sent", len([e for e in events() if e == "plug_on"]) == before, str(events()))

    print("4. streaming request on a sleeping node: SSE keep-alives, then real tokens")
    t0 = time.time()
    lines = []
    with c.stream("POST", f"{GW}/v1/chat/completions",
                  json={"model": "gpt-oss-20b-MXFP4", "stream": True,
                        "messages": [{"role": "user", "content": "hi"}]}) as r:
        check("200", r.status_code == 200, str(r.status_code))
        for line in r.iter_lines():
            lines.append(line)
    elapsed = time.time() - t0
    body = "\n".join(lines)
    check("keep-alive comment first", lines and lines[0].startswith(": bc250-waking"), lines[:2])
    check("ready comment present", ": bc250-ready" in body, body[:200])
    check("4 tokens streamed", body.count("tok") == 4, str(body.count("tok")))
    check(f"waited for the boot ({elapsed:.1f}s)", 2.5 < elapsed < 40, f"{elapsed:.1f}s")

    print("4b. l'horloge d'inactivité est remise à zéro par une requête, pas par son début")
    # 4 s de génération : si l'horloge part du début de la requête, idle_for_s
    # vaut ~4 s juste après ; si elle repart de la fin, il vaut ~0.
    r = c.post(f"{GW}/v1/chat/completions", headers={"x-test-delay": "4"},
               json={"model": "gpt-oss-20b-MXFP4", "messages": [{"role": "user", "content": "ok"}]})
    check("200 sur la requête témoin", r.status_code == 200, str(r.status_code))
    time.sleep(0.5)
    st = c.get(f"{GW}/admin/status").json()
    # Avant correctif : idle_for_s valait la durée de la requête. Après : ~0.
    check("idle_for_s retombe sous 3 s", (st.get("idle_for_s") or 99) < 3.0, str(st.get("idle_for_s")))
    check("aucune requête en vol", st.get("active_requests") == 0, str(st.get("active_requests")))

    print("4c. une génération plus longue que idle_timeout n'est pas coupée")
    # idle_timeout vaut 6 s dans ce banc ; on streame 9 tokens à 1 s d'intervalle.
    t0 = time.time()
    toks = 0
    peak_active = 0
    with c.stream("POST", f"{GW}/v1/chat/completions",
                  headers={"x-test-tokens": "9", "x-test-delay": "1.0"},
                  json={"model": "gpt-oss-20b-MXFP4", "stream": True,
                        "messages": [{"role": "user", "content": "long"}]}) as r:
        check("200", r.status_code == 200, str(r.status_code))
        for line in r.iter_lines():
            if line.startswith("data: ") and "tok" in line:
                toks += 1
                if toks == 2:
                    peak_active = c.get(f"{GW}/admin/status").json().get("active_requests") or 0
    elapsed = time.time() - t0
    check("9 tokens reçus jusqu'au bout", toks == 9, str(toks))
    check(f"le flux a dépassé idle_timeout ({elapsed:.1f}s > 6s)", elapsed > 6.5, f"{elapsed:.1f}s")
    check("requête comptée en vol pendant la génération", peak_active == 1, str(peak_active))
    check("le noeud est toujours réveillé à la fin", c.get(f"{GW}/admin/status").json()["state"] == "ready")

    print("5. non-streaming request passes through")
    r = c.post(f"{GW}/v1/chat/completions",
               json={"model": "gpt-oss-20b-MXFP4", "messages": [{"role": "user", "content": "ping"}]})
    check("pong", r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "pong",
          r.text[:200])

    print("6. keep-awake blocks the idle shutdown")
    c.post(f"{GW}/admin/keep-awake", json={"enabled": True})
    time.sleep(9.0)
    check("still ready after idle_timeout", status(c)["state"] == "ready", str(status(c)))
    check("keep_awake reported", status(c)["keep_awake"] is True)

    print("7. releasing keep-awake lets the idle timer fire")
    c.post(f"{GW}/admin/keep-awake", json={"enabled": False})
    deadline = time.time() + 30
    while time.time() < deadline and status(c)["state"] != "asleep":
        time.sleep(1.0)
    st = status(c)
    check("went to sleep on idle", st["state"] == "asleep", str(st))
    ev = events()
    check("a second poweroff happened", ev.count("poweroff") >= 2, str(ev))
    check("sleep counter", st["sleeps"] >= 2, str(st))

    print("8. un reveil rate ne cycle pas, mais le suivant cycle tout de suite")
    (BASE / "never_boot").touch()
    before = events()
    r = c.post(f"{GW}/admin/wake", timeout=90)
    ev1 = events()[len(before):]
    st1 = status(c)
    check("echec annonce en 503", r.status_code == 503, str(r.status_code))
    check("aucun cycle au premier echec", "plug_off" not in ev1, str(ev1))
    check("compteur d'echec a 1", st1.get("wake_failures") == 1, str(st1.get("wake_failures")))

    before = events()
    c.post(f"{GW}/admin/wake", timeout=90)
    ev2 = events()[len(before):]
    # Ancien comportement : plug_on (inutile, deja allume) puis plug_off puis plug_on.
    check("cycle immediat: off puis on", ev2[:2] == ["plug_off", "plug_on"], str(ev2))
    (BASE / "never_boot").unlink(missing_ok=True)

    print(f"\n{ok_count} passed, {fail_count} failed")
    return 1 if fail_count else 0


if __name__ == "__main__":
    sys.exit(main())
