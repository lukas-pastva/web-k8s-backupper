web-k8s-backupper 🧰
====================

Click button. Get backups. Pretend it was hard.

What is this?
-------------
- Tiny web UI + API for peeking into Kubernetes PVCs and yoinking their bits.
- Can ZIP whole PVCs or folders on the fly (no temp space, pure stream vibes).
- If your PVC belongs to Postgres/MySQL/MariaDB, it can slurp a DB dump too.

Highlights
----------
- PVC browser: explore, preview, download, and ZIP any path.
- DB auto-detect: finds pods mounting the PVC and spots DB engines from images.
- One-click dumps: streams `.sql.gz` by default; works via pod exec. No secrets logged.

60‑second Tour
--------------
1) Build it:
```
docker build -f src/Dockerfile -t web-k8s-backupper:dev src
```
2) Run it (cluster or local kubeconfig):
```
docker run --rm -p 8080:8080 -v $HOME/.kube:/root/.kube:ro web-k8s-backupper:dev
```
3) Open `http://localhost:8080` and browse PVCs like a file explorer. If we detect
   a DB, a shiny “DB Dump (engine)” button appears. Click → stream → profit.

Useful Endpoints
----------------
- Detect DB: `GET /api/namespaces/{ns}/pvcs/{pvc}/db/detect`
- Dump DB: `GET /api/namespaces/{ns}/pvcs/{pvc}/db/dump?compress=true`
- ZIP path: `GET /api/namespaces/{ns}/pvcs/{pvc}/zip?path=.`

How dumps work (aka: “trust fall”)
----------------------------------
- We exec inside the real DB container and run standard tools:
  - Postgres: `pg_dumpall`
  - MySQL/MariaDB: `mysqldump --all-databases --single-transaction`
- Credentials are sourced from common env vars or `*_FILE` secrets if present.
- If the image has no client binaries, the dump will (comedically) fail. Bring tools.

Safety-ish Notes
----------------
- Read-only by default for PVC helper pods; DB exec does not mutate data.
- We avoid logging secrets, tokens, or bucket URLs. Your jokes, however, are public.

Warranty
--------
- None. But if it works, you must say “wow, neat” at least once.

