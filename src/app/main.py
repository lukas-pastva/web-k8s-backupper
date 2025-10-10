import os
import time
from typing import Dict, List, Iterator
import logging
import io
import queue
import threading
import tarfile
import zipfile
import stat

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

from kubernetes import client, config
from kubernetes.stream import stream as k8s_stream
import yaml


def load_kube():
    try:
        config.load_incluster_config()
    except Exception:
        # Fallback for local development
        config.load_kube_config()


load_kube()
core = client.CoreV1Api()
apps = client.AppsV1Api()
batch = client.BatchV1Api()
networking = client.NetworkingV1Api()
rbac = client.RbacAuthorizationV1Api()


app = FastAPI(title="web-k8s-backupper")
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Logging
logger = logging.getLogger("web_k8s_backupper")
_level = os.environ.get("LOG_LEVEL", "INFO").upper()
try:
    logger.setLevel(getattr(logging, _level))
except Exception:
    logger.setLevel(logging.INFO)
if not logger.handlers:
    # Basic console handler if not already configured by the ASGI server
    h = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    h.setFormatter(fmt)
    logger.addHandler(h)
ARCHIVE_DEBUG = os.environ.get("ARCHIVE_DEBUG", "0") in ("1", "true", "True")
# Zip strategy: 'tar' (stream tar -> zip) or 'walk' (enumerate via ls + cat)
ARCHIVE_ZIP_MODE = os.environ.get("ARCHIVE_ZIP_MODE", "tar").strip().lower()
ARCHIVE_TAR_VERBOSE = os.environ.get("ARCHIVE_TAR_VERBOSE", "0") in ("1", "true", "True")



@app.get("/", response_class=HTMLResponse)
def index():
    with open(os.path.join(static_dir, "index.html"), "r", encoding="utf-8") as f:
        return f.read()


@app.get("/namespace", response_class=HTMLResponse)
def namespace_page():
    with open(os.path.join(static_dir, "namespace.html"), "r", encoding="utf-8") as f:
        return f.read()


@app.get("/pvc", response_class=HTMLResponse)
def pvc_page():
    with open(os.path.join(static_dir, "pvc.html"), "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/namespaces")
def list_namespaces() -> List[str]:
    nss = core.list_namespace().items
    return [n.metadata.name for n in nss]


@app.get("/api/namespaces/{ns}/pvcs")
def list_pvcs(ns: str) -> List[Dict]:
    pvcs = core.list_namespaced_persistent_volume_claim(namespace=ns).items
    return [
        {
            "name": pvc.metadata.name,
            "storageClass": pvc.spec.storage_class_name,
            "volumeName": pvc.spec.volume_name,
            "accessModes": pvc.spec.access_modes or [],
            "capacity": (pvc.status.capacity or {}).get("storage"),
        }
        for pvc in pvcs
    ]


@app.get("/api/namespaces/{ns}/objects")
def list_objects(ns: str) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}

    def collect(kind: str, items):
        result[kind] = [i.metadata.name for i in items]

    collect("pods", core.list_namespaced_pod(ns).items)
    collect("services", core.list_namespaced_service(ns).items)
    collect("configmaps", core.list_namespaced_config_map(ns).items)
    collect("secrets", core.list_namespaced_secret(ns).items)
    collect("persistentvolumeclaims", core.list_namespaced_persistent_volume_claim(ns).items)

    collect("deployments", apps.list_namespaced_deployment(ns).items)
    collect("statefulsets", apps.list_namespaced_stateful_set(ns).items)
    collect("daemonsets", apps.list_namespaced_daemon_set(ns).items)

    collect("jobs", batch.list_namespaced_job(ns).items)
    collect("cronjobs", batch.list_namespaced_cron_job(ns).items)

    collect("ingresses", networking.list_namespaced_ingress(ns).items)

    collect("roles", rbac.list_namespaced_role(ns).items)
    collect("rolebindings", rbac.list_namespaced_role_binding(ns).items)

    return result


def to_yaml_docs(objs: List[dict]) -> str:
    parts = []
    for obj in objs:
        parts.append(yaml.safe_dump(obj, sort_keys=False))
    return "---\n".join(parts)


@app.get("/api/namespaces/{ns}/manifests")
def download_manifests(ns: str, includeSecrets: bool = True, download: bool = True):
    docs: List[dict] = []

    # Core
    docs += [o.to_dict() for o in core.list_namespaced_pod(ns).items]
    docs += [o.to_dict() for o in core.list_namespaced_service(ns).items]
    docs += [o.to_dict() for o in core.list_namespaced_config_map(ns).items]
    if includeSecrets:
        docs += [o.to_dict() for o in core.list_namespaced_secret(ns).items]
    docs += [o.to_dict() for o in core.list_namespaced_persistent_volume_claim(ns).items]

    # Apps
    docs += [o.to_dict() for o in apps.list_namespaced_deployment(ns).items]
    docs += [o.to_dict() for o in apps.list_namespaced_stateful_set(ns).items]
    docs += [o.to_dict() for o in apps.list_namespaced_daemon_set(ns).items]
    docs += [o.to_dict() for o in apps.list_namespaced_replica_set(ns).items]

    # Batch
    docs += [o.to_dict() for o in batch.list_namespaced_job(ns).items]
    docs += [o.to_dict() for o in batch.list_namespaced_cron_job(ns).items]

    # Networking
    docs += [o.to_dict() for o in networking.list_namespaced_ingress(ns).items]

    # RBAC (namespaced)
    docs += [o.to_dict() for o in rbac.list_namespaced_role(ns).items]
    docs += [o.to_dict() for o in rbac.list_namespaced_role_binding(ns).items]

    content = to_yaml_docs(docs)
    filename = f"{ns}-manifests.yaml"
    headers = {}
    if download:
        headers["Content-Disposition"] = f"attachment; filename={filename}"
    media_type = "application/x-yaml" if download else "text/plain; charset=utf-8"
    return Response(content=content, media_type=media_type, headers=headers)


@app.get("/api/namespaces/{ns}/objects/{kind}/{name}/manifest")
def get_object_manifest(ns: str, kind: str, name: str, download: bool = False):
    kind = kind.lower()

    # Map plural kinds to read functions
    readers = {
        "pods": core.read_namespaced_pod,
        "services": core.read_namespaced_service,
        "configmaps": core.read_namespaced_config_map,
        "secrets": core.read_namespaced_secret,
        "persistentvolumeclaims": core.read_namespaced_persistent_volume_claim,
        "deployments": apps.read_namespaced_deployment,
        "statefulsets": apps.read_namespaced_stateful_set,
        "daemonsets": apps.read_namespaced_daemon_set,
        "jobs": batch.read_namespaced_job,
        "cronjobs": batch.read_namespaced_cron_job,
        "ingresses": networking.read_namespaced_ingress,
        "roles": rbac.read_namespaced_role,
        "rolebindings": rbac.read_namespaced_role_binding,
    }
    reader = readers.get(kind)
    if not reader:
        raise HTTPException(status_code=400, detail=f"Unsupported kind: {kind}")

    try:
        obj = reader(name=name, namespace=ns)
    except client.exceptions.ApiException as e:
        status = e.status or 500
        raise HTTPException(status_code=status, detail=e.reason)

    content = to_yaml_docs([obj.to_dict()])
    headers = {}
    if download:
        filename = f"{ns}-{kind}-{name}.yaml"
        headers["Content-Disposition"] = f"attachment; filename={filename}"
    media_type = "application/x-yaml" if download else "text/plain; charset=utf-8"
    return Response(content=content, media_type=media_type, headers=headers)


def ensure_helper_pod(ns: str, pvc: str) -> str:
    name = f"wkb-pvc-reader-{pvc[:40]}-{int(time.time())}"
    # Security context: by default run as root to ensure readable access to all files
    # You can override via env READER_UID/READER_GID to run non-root
    uid = int(os.environ.get("READER_UID", "0"))
    gid = int(os.environ.get("READER_GID", "0"))
    run_as_non_root = uid != 0
    read_only_mount = os.environ.get("READER_READONLY", "1") in ("1", "true", "True")
    pod = client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=client.V1ObjectMeta(name=name, labels={"app": "web-k8s-backupper", "role": "pvc-reader"}),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="reader",
                    image=os.environ.get("READER_IMAGE", "alpine:3.19"),
                    command=["/bin/sh", "-c", "sleep 3600"],
                    volume_mounts=[client.V1VolumeMount(name="data", mount_path="/data", read_only=read_only_mount)],
                    # Default to root to maximize readability; can be overridden via env
                    security_context=client.V1SecurityContext(
                        run_as_non_root=run_as_non_root,
                        run_as_user=uid,
                        run_as_group=gid,
                        allow_privilege_escalation=False,
                    ),
                )
            ],
            volumes=[client.V1Volume(name="data", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=pvc))],
        ),
    )
    try:
        logger.info("Creating helper pod for PVC export: ns=%s pvc=%s name=%s", ns, pvc, name)
        core.create_namespaced_pod(namespace=ns, body=pod)
    except client.exceptions.ApiException as e:
        if e.status != 409:
            raise

    # Wait until Running
    for _ in range(120):
        p = core.read_namespaced_pod(name=name, namespace=ns)
        if p.status.phase == "Running":
            logger.info("Helper pod is Running: %s/%s", ns, name)
            return name
        if p.status.phase in ("Failed", "Succeeded"):
            break
        time.sleep(1)
    raise HTTPException(status_code=500, detail="Helper pod did not become Running")


def _exec_tar_stream(ns: str, pod_name: str, gzip: bool = False):
    """
    Start a tar stream from the helper pod. If gzip is True, compress; otherwise plain tar.

    Returns the Kubernetes stream response object which exposes read methods and lifecycle.
    """
    cmd = ["tar"]
    flags = "c" + ("z" if gzip else "") + ("v" if ARCHIVE_TAR_VERBOSE else "") + "f"
    cmd += [flags, "-"]
    cmd += ["-C", "/data", "."]
    if ARCHIVE_DEBUG:
        logger.info("Exec tar stream cmd: %s", " ".join(cmd))
    resp = k8s_stream(
        core.connect_get_namespaced_pod_exec,
        pod_name,
        ns,
        command=cmd,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )
    return resp


def _exec_tar_stream_path(ns: str, pod_name: str, rel_path: str, gzip: bool = False):
    """Start a tar stream for a specific relative path under /data."""
    rel_path = _sanitize_rel_path(rel_path)
    cmd = ["tar"]
    flags = "c" + ("z" if gzip else "") + ("v" if ARCHIVE_TAR_VERBOSE else "") + "f"
    cmd += [flags, "-"]
    cmd += ["-C", "/data", rel_path]
    if ARCHIVE_DEBUG:
        logger.info("Exec tar stream path cmd: %s", " ".join(cmd))
    resp = k8s_stream(
        core.connect_get_namespaced_pod_exec,
        pod_name,
        ns,
        command=cmd,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )
    return resp


def stream_tar_from_pod(ns: str, pod_name: str) -> Iterator[bytes]:
    """Backwards-compatible: yield a gzipped tar stream of the PVC contents."""
    resp = _exec_tar_stream(ns, pod_name, gzip=True)
    try:
        while resp.is_open():
            resp.update(timeout=5)
            if resp.peek_stdout():
                chunk = resp.read_stdout()
                if isinstance(chunk, str):
                    chunk = chunk.encode()
                yield chunk
            if resp.peek_stderr():
                _ = resp.read_stderr()
    finally:
        resp.close()


def _exec_in_pod(ns: str, pod_name: str, command: list[str]) -> tuple[str, str]:
    resp = k8s_stream(
        core.connect_get_namespaced_pod_exec,
        pod_name,
        ns,
        command=command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )
    out_chunks: list[str] = []
    err_chunks: list[str] = []
    try:
        while resp.is_open():
            resp.update(timeout=5)
            if resp.peek_stdout():
                chunk = resp.read_stdout()
                if isinstance(chunk, bytes):
                    chunk = chunk.decode(errors="replace")
                out_chunks.append(chunk)
            if resp.peek_stderr():
                chunk = resp.read_stderr()
                if isinstance(chunk, bytes):
                    chunk = chunk.decode(errors="replace")
                err_chunks.append(chunk)
    finally:
        resp.close()
    return ("".join(out_chunks), "".join(err_chunks))


class _IteratorReader:
    """Wrap an iterator of bytes into a file-like object with .read()."""

    def __init__(self, iterator: Iterator[bytes]):
        self._it = iterator
        self._buf = bytearray()
        self._done = False

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            # Read all
            chunks = [bytes(self._buf)]
            self._buf.clear()
            for ch in self._it:
                chunks.append(ch)
            self._done = True
            return b"".join(chunks)

        while len(self._buf) < n and not self._done:
            try:
                ch = next(self._it)
                if ch:
                    self._buf.extend(ch)
                else:
                    # Empty chunk – continue
                    continue
            except StopIteration:
                self._done = True
                break
        if n < 0:
            n = len(self._buf)
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out


class _QueueWriter:
    """File-like that pushes written bytes into a queue for streaming."""

    def __init__(self):
        self.q: "queue.Queue[bytes | None]" = queue.Queue(maxsize=16)
        self.closed = False
        self._pos = 0

    def write(self, b: bytes):
        if not b:
            return 0
        # Ensure bytes-like
        if not isinstance(b, (bytes, bytearray, memoryview)):
            b = bytes(b)
        blen = len(b)
        self.q.put(bytes(b))
        self._pos += blen
        return blen

    def flush(self):
        return

    def close(self):
        if not self.closed:
            self.closed = True
            self.q.put(None)

    # ZipFile checks these to decide behavior on non-seekable streams
    def seekable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def reader(self):
        while True:
            item = self.q.get()
            if item is None:
                break
            yield item


def _tar_to_zip_stream(tar_stream: Iterator[bytes], prefix: str = "") -> Iterator[bytes]:
    """
    Convert a (plain) tar byte-stream into a zip byte-stream on the fly.
    Uses a background thread to write into a queue-backed writer while the
    generator yields from that queue.
    """

    writer = _QueueWriter()

    def worker():
        try:
            tar_fileobj = _IteratorReader(tar_stream)
            # Streaming read of tar
            with tarfile.open(fileobj=tar_fileobj, mode="r|") as tf:
                with zipfile.ZipFile(writer, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
                    count_files = 0
                    count_dirs = 0
                    count_links = 0
                    count_other = 0
                    total_in_bytes = 0
                    total_out_files = 0
                    for ti in tf:
                        # Preserve leading dots in filenames; only strip a single
                        # leading "./" or "/" that tar may include as a prefix.
                        name = ti.name
                        if name.startswith("./"):
                            name = name[2:]
                        elif name.startswith("/"):
                            name = name[1:]
                        if not name:
                            continue
                        # Optional top-level prefix (e.g., namespace-pvc/ or basename(path)/)
                        pfx = prefix
                        if pfx and not pfx.endswith("/"):
                            pfx += "/"
                        # Directories
                        if ti.isdir():
                            if not name.endswith("/"):
                                name += "/"
                            zinfo = zipfile.ZipInfo(filename=f"{pfx}{name}")
                            # Preserve basic permissions for dirs
                            perm = ti.mode if ti.mode else 0o755
                            zinfo.external_attr = (stat.S_IFDIR | perm) << 16
                            zf.writestr(zinfo, b"")
                            count_dirs += 1
                            if ARCHIVE_DEBUG:
                                logger.debug("ZIP add dir: %s", name)
                            continue

                        # Regular files
                        if ti.isreg():
                            src = tf.extractfile(ti)
                            if src is None:
                                continue
                            zinfo = zipfile.ZipInfo(filename=f"{pfx}{name}")
                            perm = ti.mode if ti.mode else 0o644
                            zinfo.external_attr = (stat.S_IFREG | perm) << 16
                            with zf.open(zinfo, mode="w") as dest:
                                while True:
                                    chunk = src.read(1024 * 1024)
                                    if not chunk:
                                        break
                                    dest.write(chunk)
                                    total_in_bytes += len(chunk)
                            count_files += 1
                            total_out_files += 1
                            if ARCHIVE_DEBUG:
                                logger.debug("ZIP add file: %s (size≈%s bytes)", name, ti.size)
                            continue

                        # Symlinks and other types: store a small text note
                        if ti.issym() or ti.islnk():
                            target = ti.linkname or ""
                            info = f"SYMLINK -> {target}\n".encode()
                            zinfo = zipfile.ZipInfo(filename=f"{pfx}{name}")
                            zinfo.external_attr = (stat.S_IFLNK | 0o777) << 16
                            zf.writestr(zinfo, info)
                            count_links += 1
                            if ARCHIVE_DEBUG:
                                logger.debug("ZIP add link: %s -> %s", name, target)
                            continue

                        # Fallback: skip unknown types
                        count_other += 1
                    logger.info(
                        "ZIP stream complete: files=%d dirs=%d links=%d other=%d bytes_in≈%d",
                        count_files, count_dirs, count_links, count_other, total_in_bytes,
                    )
        except Exception:
            logger.exception("Error while converting tar->zip stream")
        finally:
            # Ensure the writer signals completion
            try:
                writer.close()
            except Exception:
                pass

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    # Yield data as produced
    for chunk in writer.reader():
        yield chunk


def _sanitize_rel_path(p: str) -> str:
    p = p or "."
    p = p.strip()
    if p.startswith("/"):
        p = p[1:]
    # Normalize and prevent escape upwards
    p = os.path.normpath(p)
    if p in ("", "."):
        return "."
    if p.startswith(".."):
        raise HTTPException(status_code=400, detail="Invalid path")
    return p


def _build_tree_from_tar(ns: str, pod_name: str, rel_path: str = ".", max_nodes: int = 5000):
    rel_path = _sanitize_rel_path(rel_path)
    logger.info("Building PVC tree: ns=%s pod=%s rel_path=%s max_nodes=%d", ns, pod_name, rel_path, max_nodes)
    if ARCHIVE_DEBUG:
        # Quick peek at top-level to diagnose mount/permissions
        try:
            out, err = _exec_in_pod(ns, pod_name, ["/bin/sh", "-lc", "ls -la /data | sed -n '1,50p'"])
            logger.debug("PVC /data ls -la (first 50 lines)\n%s", out)
            if err:
                logger.debug("PVC /data ls stderr: %s", err.strip())
        except Exception:
            logger.exception("Failed to ls -la /data for debug")
    # Start tar limited to rel_path
    cmd_path = rel_path
    resp = k8s_stream(
        core.connect_get_namespaced_pod_exec,
        pod_name,
        ns,
        command=["tar", "cf", "-", "-C", "/data", cmd_path],
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )

    bytes_read = 0
    truncated = False
    total_entries = 0

    def tar_iter() -> Iterator[bytes]:
        nonlocal bytes_read
        stderr_lines = 0
        try:
            while resp.is_open():
                resp.update(timeout=5)
                if resp.peek_stdout():
                    chunk = resp.read_stdout()
                    if isinstance(chunk, str):
                        chunk = chunk.encode()
                    if chunk:
                        bytes_read += len(chunk)
                        yield chunk
                if resp.peek_stderr():
                    err = resp.read_stderr()
                    if err:
                        estr = err if isinstance(err, str) else err.decode(errors="replace")
                        if ARCHIVE_DEBUG and stderr_lines < 20:
                            logger.debug("PVC tar(tree) stderr: %s", estr.strip())
                            stderr_lines += 1
        finally:
            resp.close()

    reader = _IteratorReader(tar_iter())
    root = {"name": "/", "type": "dir", "children": {}}

    def ensure_dir(parent: dict, name: str) -> dict:
        ch = parent["children"]
        if name not in ch:
            ch[name] = {"name": name, "type": "dir", "children": {}}
        elif ch[name]["type"] != "dir":
            # Promote to dir if previously marked as file due to tar ordering
            ch[name]["type"] = "dir"
            ch[name]["children"] = ch.get("children", {})
        return ch[name]

    def add_file(parent: dict, name: str, size: int | None, mode: int | None):
        ch = parent["children"]
        if name not in ch:
            ch[name] = {"name": name, "type": "file", "size": int(size or 0), "mode": int(mode or 0)}
        else:
            # If already present as dir, keep dir; otherwise set file meta
            if ch[name]["type"] != "dir":
                ch[name].update({"type": "file", "size": int(size or 0), "mode": int(mode or 0)})

    try:
        with tarfile.open(fileobj=reader, mode="r|") as tf:
            for ti in tf:
                total_entries += 1
                # Preserve leading dots in filenames; only strip a single
                # leading "./" or "/" that tar may include as a prefix.
                rel = ti.name
                if rel.startswith("./"):
                    rel = rel[2:]
                elif rel.startswith("/"):
                    rel = rel[1:]
                if not rel:
                    continue
                parts = rel.split("/")
                node = root
                # If there are nested parts, mark directories accordingly
                for i, part in enumerate(parts):
                    is_last = (i == len(parts) - 1)
                    if is_last:
                        if ti.isdir():
                            ensure_dir(node, part)
                        elif ti.isreg():
                            add_file(node, part, ti.size, ti.mode)
                        elif ti.issym() or ti.islnk():
                            ch = node["children"]
                            ch[part] = {"name": part, "type": "link", "target": ti.linkname or ""}
                        else:
                            ch = node["children"]
                            ch[part] = {"name": part, "type": "other"}
                    else:
                        node = ensure_dir(node, part)

                # Stop if too many nodes accumulated (approximate: count children entries)
                def count_nodes(n: dict) -> int:
                    c = 0
                    for v in n["children"].values():
                        c += 1
                        if v.get("type") == "dir":
                            c += count_nodes(v)
                    return c

                if total_entries % 500 == 0:
                    # Periodic check to avoid O(n) on every item
                    if count_nodes(root) > max_nodes:
                        truncated = True
                        break
    except Exception:
        logger.exception("Failed to build PVC tree")
        raise
    finally:
        try:
            core.delete_namespaced_pod(name=pod_name, namespace=ns, grace_period_seconds=0)
        except Exception:
            pass
        logger.info("PVC tree built: entries=%d bytes=%d truncated=%s", total_entries, bytes_read, truncated)

    # Convert children dicts to sorted arrays for JSON friendliness
    def to_list(n: dict):
        items = list(n["children"].values())
        # Sort: dirs first, then files, then links/others, by name
        order = {"dir": 0, "file": 1, "link": 2}
        items.sort(key=lambda x: (order.get(x.get("type"), 9), x.get("name", "")))
        out = []
        for it in items:
            if it.get("type") == "dir":
                out.append({"name": it["name"], "type": "dir", "children": to_list(it)})
            else:
                cleaned = {k: v for k, v in it.items() if k != "children"}
                out.append(cleaned)
        return out

    tree = {"name": "/", "type": "dir", "children": to_list(root)}
    return {"root": tree, "truncated": truncated}


def _log_helper_pod_env(ns: str, pod_name: str):
    """Best-effort logging of environment in the helper pod for diagnostics."""
    try:
        script = (
            "set -e; "
            "echo '--- helper env ---'; "
            "id; uname -a; echo SHELL=$SHELL; echo PATH=$PATH; "
            "(tar --version 2>&1 || true) | sed -n '1,2p'; "
            "(busybox 2>&1 || true) | sed -n '1,1p'; "
            "echo '--- /data top (first 50) ---'; cd /data && ls -la | sed -n '1,50p'"
        )
        out, err = _exec_in_pod(ns, pod_name, ["/bin/sh", "-lc", script])
        msg = out.strip()
        if msg:
            logger.info("Helper pod env:\n%s", msg)
        if err and ARCHIVE_DEBUG:
            logger.debug("Helper pod env stderr: %s", err.strip())
    except Exception:
        logger.exception("Failed to log helper pod env")


def _list_dir_entries(ns: str, pod_name: str, rel: str) -> List[Dict]:
    """List entries in a directory using the same logic as pvc_ls, returns list of dicts."""
    rel = _sanitize_rel_path(rel)
    script = f"""
        set -e
        cd /data
        p={sh_quote(rel)}
        if [ ! -e "$p" ]; then echo "__ERR__NOT_FOUND"; exit 2; fi
        if [ -d "$p" ]; then
          for f in "$p"/* "$p"/.[!.]* "$p"/..?*; do
            [ -e "$f" ] || continue
            name="${{f#"$p"/}}"
            if [ -d "$f" ]; then type=dir; elif [ -L "$f" ]; then type=link; else type=file; fi
            size=$(stat -c %s -- "$f" 2>/dev/null || wc -c < "$f" 2>/dev/null || echo 0)
            mtime=$(stat -c %Y -- "$f" 2>/dev/null || date +%s)
            printf '%s|%s|%s|%s\n' "$type" "$size" "$mtime" "$name"
          done
        else
          f="$p"
          name=$(basename -- "$f")
          if [ -d "$f" ]; then type=dir; elif [ -L "$f" ]; then type=link; else type=file; fi
          size=$(stat -c %s -- "$f" 2>/dev/null || wc -c < "$f" 2>/dev/null || echo 0)
          mtime=$(stat -c %Y -- "$f" 2>/dev/null || date +%s)
          printf '%s|%s|%s|%s\n' "$type" "$size" "$mtime" "$name"
        fi
    """
    out, err = _exec_in_pod(ns, pod_name, ["/bin/sh", "-lc", script])
    if "__ERR__NOT_FOUND" in out:
        raise HTTPException(status_code=404, detail="Path not found")
    entries: List[Dict] = []
    for line in out.splitlines():
        if not line:
            continue
        parts = line.split("|", 3)
        if len(parts) != 4:
            continue
        typ, size, mtime, name = parts
        try:
            size_i = int(size)
        except Exception:
            size_i = 0
        try:
            mtime_i = int(mtime)
        except Exception:
            mtime_i = 0
        entries.append({"type": typ, "size": size_i, "mtime": mtime_i, "name": name})
    # Sort similar to UI: dirs first then files then links
    order = {"dir": 0, "file": 1, "link": 2}
    entries.sort(key=lambda x: (order.get(x.get("type"), 9), x.get("name", "")))
    return entries


def _zip_stream_via_walk(ns: str, pod_name: str, rel: str = ".", prefix: str = "") -> Iterator[bytes]:
    """Create a zip by walking the directory with ls + cat, mirroring Browse.

    When prefix is provided, all entries are placed under that top-level folder
    inside the zip (e.g., "namespace-pvc/" or "basename(path)/").
    """
    rel = _sanitize_rel_path(rel)
    writer = _QueueWriter()

    def worker():
        try:
            with zipfile.ZipFile(writer, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
                count_files = 0
                count_dirs = 0
                count_links = 0
                had_error = False

                stack: List[str] = [rel]
                visited = set()
                pfx = prefix
                if pfx and not pfx.endswith("/"):
                    pfx += "/"
                while stack:
                    cur = stack.pop()
                    if cur in visited:
                        continue
                    visited.add(cur)
                    try:
                        entries = _list_dir_entries(ns, pod_name, cur)
                    except Exception:
                        logger.exception("Walk: failed to list %s", cur)
                        had_error = True
                        continue
                    # Add directory entry itself (except root '.')
                    if cur not in ("", "."):
                        dname = cur if cur.endswith("/") else cur + "/"
                        zinfo = zipfile.ZipInfo(filename=f"{pfx}{dname}")
                        zinfo.external_attr = (stat.S_IFDIR | 0o755) << 16
                        try:
                            zf.writestr(zinfo, b"")
                            count_dirs += 1
                        except Exception:
                            logger.exception("Walk: failed to add dir %s", dname)
                    for e in entries:
                        name = e.get("name", "")
                        typ = e.get("type", "")
                        full = name if cur in ("", ".") else f"{cur}/{name}"
                        if typ == "dir":
                            stack.append(full)
                        elif typ == "file":
                            try:
                                zinfo = zipfile.ZipInfo(filename=f"{pfx}{full}")
                                zinfo.external_attr = (stat.S_IFREG | 0o644) << 16
                                with zf.open(zinfo, mode="w") as dest:
                                    for chunk in stream_file_from_pod(ns, pod_name, full):
                                        dest.write(chunk)
                                count_files += 1
                            except Exception:
                                logger.exception("Walk: failed to add file %s", full)
                                had_error = True
                        elif typ == "link":
                            target_info = f"SYMLINK -> (target not resolved)\n".encode()
                            try:
                                zinfo = zipfile.ZipInfo(filename=f"{pfx}{full}")
                                zinfo.external_attr = (stat.S_IFLNK | 0o777) << 16
                                zf.writestr(zinfo, target_info)
                                count_links += 1
                            except Exception:
                                logger.exception("Walk: failed to add link %s", full)
                                had_error = True
                logger.info(
                    "ZIP walk complete: files=%d dirs=%d links=%d errors=%s",
                    count_files, count_dirs, count_links, had_error,
                )
        finally:
            try:
                writer.close()
            except Exception:
                pass

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    for chunk in writer.reader():
        yield chunk


@app.get("/api/namespaces/{ns}/pvcs/{pvc}/tree")
def pvc_tree(ns: str, pvc: str, path: str = ".", maxNodes: int = 5000):
    pod_name = ensure_helper_pod(ns, pvc)
    try:
        rel = _sanitize_rel_path(path)
        result = _build_tree_from_tar(ns, pod_name, rel_path=rel, max_nodes=maxNodes)
        return result
    except Exception as e:
        raise


@app.get("/api/namespaces/{ns}/pvcs/{pvc}/ls")
def pvc_ls(ns: str, pvc: str, path: str = "."):
    pod_name = ensure_helper_pod(ns, pvc)
    rel = _sanitize_rel_path(path)
    try:
        # Single shell to enumerate entries including hidden ones, with size and mtime
        script = f"""
            set -e
            cd /data
            p={sh_quote(rel)}
            if [ ! -e "$p" ]; then echo "__ERR__NOT_FOUND"; exit 2; fi
            if [ -d "$p" ]; then
              for f in "$p"/* "$p"/.[!.]* "$p"/..?*; do
                [ -e "$f" ] || continue
                name="${{f#"$p"/}}"
                if [ -d "$f" ]; then type=dir; elif [ -L "$f" ]; then type=link; else type=file; fi
                size=$(stat -c %s -- "$f" 2>/dev/null || wc -c < "$f" 2>/dev/null || echo 0)
                mtime=$(stat -c %Y -- "$f" 2>/dev/null || date +%s)
                printf '%s|%s|%s|%s\n' "$type" "$size" "$mtime" "$name"
              done
            else
              f="$p"
              name=$(basename -- "$f")
              if [ -d "$f" ]; then type=dir; elif [ -L "$f" ]; then type=link; else type=file; fi
              size=$(stat -c %s -- "$f" 2>/dev/null || wc -c < "$f" 2>/dev/null || echo 0)
              mtime=$(stat -c %Y -- "$f" 2>/dev/null || date +%s)
              printf '%s|%s|%s|%s\n' "$type" "$size" "$mtime" "$name"
            fi
        """
        out, err = _exec_in_pod(ns, pod_name, ["/bin/sh", "-lc", script])
        if err and ARCHIVE_DEBUG:
            logger.debug("PVC ls stderr: %s", err.strip())
        if "__ERR__NOT_FOUND" in out:
            raise HTTPException(status_code=404, detail="Path not found")
        entries = []
        for line in out.splitlines():
            if not line:
                continue
            parts = line.split("|", 3)
            if len(parts) != 4:
                continue
            typ, size, mtime, name = parts
            try:
                size_i = int(size)
            except Exception:
                size_i = 0
            try:
                mtime_i = int(mtime)
            except Exception:
                mtime_i = 0
            entries.append({"name": name, "type": typ, "size": size_i, "mtime": mtime_i})
        return {"path": rel, "entries": entries}
    finally:
        try:
            core.delete_namespaced_pod(name=pod_name, namespace=ns, grace_period_seconds=0)
        except Exception:
            pass


def sh_quote(s: str) -> str:
    # Minimal shell quoting
    return "'" + s.replace("'", "'\\''") + "'"


def stream_file_from_pod(ns: str, pod_name: str, rel_path: str) -> Iterator[bytes]:
    cmd = ["/bin/sh", "-lc", f"cat -- {sh_quote(os.path.join('/data', rel_path))}"]
    resp = k8s_stream(
        core.connect_get_namespaced_pod_exec,
        pod_name,
        ns,
        command=cmd,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )
    try:
        while resp.is_open():
            resp.update(timeout=5)
            if resp.peek_stdout():
                chunk = resp.read_stdout()
                if isinstance(chunk, str):
                    chunk = chunk.encode()
                if chunk:
                    yield chunk
            if resp.peek_stderr():
                err = resp.read_stderr()
                if err:
                    estr = err if isinstance(err, str) else err.decode(errors="replace")
                    if ARCHIVE_DEBUG:
                        logger.debug("PVC file stderr: %s", estr.strip())
    finally:
        resp.close()


@app.get("/api/namespaces/{ns}/pvcs/{pvc}/file")
def pvc_file_download(ns: str, pvc: str, path: str):
    rel = _sanitize_rel_path(path)
    pod_name = ensure_helper_pod(ns, pvc)

    filename = os.path.basename(rel) or "file"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}

    def cleanup():
        try:
            core.delete_namespaced_pod(name=pod_name, namespace=ns, grace_period_seconds=0)
        except Exception:
            pass

    logger.info("Starting PVC file download: ns=%s pvc=%s path=%s pod=%s", ns, pvc, rel, pod_name)
    return StreamingResponse(
        stream_file_from_pod(ns, pod_name, rel),
        media_type="application/octet-stream",
        headers=headers,
        background=BackgroundTask(cleanup),
    )


@app.get("/api/namespaces/{ns}/pvcs/{pvc}/zip")
def pvc_zip_path(ns: str, pvc: str, path: str = "."):
    rel = _sanitize_rel_path(path)
    pod_name = ensure_helper_pod(ns, pvc)

    base = os.path.basename(rel)
    if base in ("", "."):
        base = f"{ns}-{pvc}"
    filename = f"{base}.zip"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}

    logger.info("Starting PVC zip for path: ns=%s pvc=%s path=%s pod=%s mode=%s", ns, pvc, rel, pod_name, ARCHIVE_ZIP_MODE)
    if ARCHIVE_DEBUG:
        _log_helper_pod_env(ns, pod_name)
    if ARCHIVE_ZIP_MODE == "walk":
        def cleanup_walk():
            try:
                core.delete_namespaced_pod(name=pod_name, namespace=ns, grace_period_seconds=0)
            except Exception:
                pass
        return StreamingResponse(
            _zip_stream_via_walk(ns, pod_name, rel, prefix=base),
            media_type="application/zip",
            headers=headers,
            background=BackgroundTask(cleanup_walk),
        )

    resp = _exec_tar_stream_path(ns, pod_name, rel, gzip=False)

    def tar_iter() -> Iterator[bytes]:
        bytes_read = 0
        stderr_lines = 0
        try:
            while resp.is_open():
                resp.update(timeout=5)
                if resp.peek_stdout():
                    chunk = resp.read_stdout()
                    if isinstance(chunk, str):
                        chunk = chunk.encode()
                    if chunk:
                        bytes_read += len(chunk)
                        yield chunk
                if resp.peek_stderr():
                    err = resp.read_stderr()
                    if err:
                        estr = err if isinstance(err, str) else err.decode(errors="replace")
                        # Log a few lines at INFO for diagnostics
                        if stderr_lines < 10:
                            logger.info("PVC tar(zip path) stderr: %s", estr.strip())
                            stderr_lines += 1
        finally:
            resp.close()
            logger.info("PVC tar(zip path) stream closed: ns=%s pvc=%s path=%s bytes=%d", ns, pvc, rel, bytes_read)

    def cleanup():
        try:
            core.delete_namespaced_pod(name=pod_name, namespace=ns, grace_period_seconds=0)
        except Exception:
            pass

    return StreamingResponse(
        _tar_to_zip_stream(tar_iter(), prefix=base),
        media_type="application/zip",
        headers=headers,
        background=BackgroundTask(cleanup),
    )


@app.get("/api/namespaces/{ns}/pvcs/{pvc}/preview")
def pvc_preview(ns: str, pvc: str, path: str, maxBytes: int = 131072):
    rel = _sanitize_rel_path(path)
    pod_name = ensure_helper_pod(ns, pvc)
    # Use head -c to limit size in the pod
    script = f"cd /data && if [ -d {sh_quote(rel)} ]; then echo '__ERR__IS_DIR'; else head -c {maxBytes} -- {sh_quote(rel)}; fi"
    out, err = _exec_in_pod(ns, pod_name, ["/bin/sh", "-lc", script])
    try:
        if "__ERR__IS_DIR" in out:
            raise HTTPException(status_code=400, detail="Path is a directory")
        # Best-effort; content already decoded as text with replacement if needed
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        return Response(content=out, media_type="text/plain", headers=headers)
    finally:
        try:
            core.delete_namespaced_pod(name=pod_name, namespace=ns, grace_period_seconds=0)
        except Exception:
            pass


@app.get("/api/namespaces/{ns}/pvcs/{pvc}/download")
def download_pvc(ns: str, pvc: str):
    pod_name = ensure_helper_pod(ns, pvc)
    # Produce a .zip archive instead of .tar.gz
    filename = f"{ns}-{pvc}.zip"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}

    def cleanup():
        try:
            core.delete_namespaced_pod(name=pod_name, namespace=ns, grace_period_seconds=0)
        except Exception:
            pass

    # Log effective reader context
    logger.info(
        "Starting PVC download: ns=%s pvc=%s pod=%s format=zip READER_UID=%s READER_GID=%s READONLY=%s mode=%s",
        ns,
        pvc,
        pod_name,
        os.environ.get("READER_UID", "0"),
        os.environ.get("READER_GID", "0"),
        os.environ.get("READER_READONLY", "1"),
        ARCHIVE_ZIP_MODE,
    )
    if ARCHIVE_DEBUG:
        _log_helper_pod_env(ns, pod_name)
    # Depending on strategy, either walk or tar-stream
    if ARCHIVE_ZIP_MODE == "walk":
        return StreamingResponse(
            _zip_stream_via_walk(ns, pod_name, ".", prefix=f"{ns}-{pvc}"),
            media_type="application/zip",
            headers=headers,
            background=BackgroundTask(cleanup),
        )

    # Create a plain tar stream from the pod and transcode to zip on the fly
    resp = _exec_tar_stream(ns, pod_name, gzip=False)

    def tar_iter() -> Iterator[bytes]:
        bytes_read = 0
        last_log = 0
        stderr_lines = 0
        try:
            while resp.is_open():
                resp.update(timeout=5)
                if resp.peek_stdout():
                    chunk = resp.read_stdout()
                    if isinstance(chunk, str):
                        chunk = chunk.encode()
                    if chunk:
                        yield chunk
                        bytes_read += len(chunk)
                        if ARCHIVE_DEBUG and bytes_read - last_log >= 10 * 1024 * 1024:
                            logger.debug("PVC tar bytes streamed: %d", bytes_read)
                            last_log = bytes_read
                if resp.peek_stderr():
                    err = resp.read_stderr()
                    if err:
                        # Surface stderr lines for diagnostics, limited to first 10
                        estr = err if isinstance(err, str) else err.decode(errors="replace")
                        if stderr_lines < 10:
                            logger.info("PVC tar(stderr): %s", estr.strip())
                            stderr_lines += 1
        finally:
            resp.close()
            logger.info("PVC tar stream closed: ns=%s pvc=%s bytes=%d", ns, pvc, bytes_read)

    return StreamingResponse(
        _tar_to_zip_stream(tar_iter(), prefix=f"{ns}-{pvc}"),
        media_type="application/zip",
        headers=headers,
        background=BackgroundTask(cleanup),
    )


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host=host, port=port)
