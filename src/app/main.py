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
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
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


@app.get("/", response_class=HTMLResponse)
def index():
    with open(os.path.join(static_dir, "index.html"), "r", encoding="utf-8") as f:
        return f.read()


@app.get("/namespace", response_class=HTMLResponse)
def namespace_page():
    with open(os.path.join(static_dir, "namespace.html"), "r", encoding="utf-8") as f:
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
def download_manifests(ns: str, includeSecrets: bool = False, download: bool = True):
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
    return Response(content=content, media_type="application/x-yaml", headers=headers)


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
    return Response(content=content, media_type="application/x-yaml", headers=headers)


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
    if gzip:
        cmd += ["czf", "-"]
    else:
        cmd += ["cf", "-"]
    cmd += ["-C", "/data", "."]
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


def _tar_to_zip_stream(tar_stream: Iterator[bytes]) -> Iterator[bytes]:
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
                    total_in_bytes = 0
                    total_out_files = 0
                    for ti in tf:
                        name = ti.name.lstrip("./")
                        if not name:
                            continue
                        # Directories
                        if ti.isdir():
                            if not name.endswith("/"):
                                name += "/"
                            zinfo = zipfile.ZipInfo(filename=name)
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
                            zinfo = zipfile.ZipInfo(filename=name)
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
                            zinfo = zipfile.ZipInfo(filename=name)
                            zinfo.external_attr = (stat.S_IFLNK | 0o777) << 16
                            zf.writestr(zinfo, info)
                            count_links += 1
                            if ARCHIVE_DEBUG:
                                logger.debug("ZIP add link: %s -> %s", name, target)
                            continue

                        # Fallback: skip unknown types
                    logger.info(
                        "ZIP stream complete: files=%d dirs=%d links=%d bytes_in≈%d",
                        count_files, count_dirs, count_links, total_in_bytes,
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
        "Starting PVC download: ns=%s pvc=%s pod=%s format=zip READER_UID=%s READER_GID=%s READONLY=%s",
        ns,
        pvc,
        pod_name,
        os.environ.get("READER_UID", "0"),
        os.environ.get("READER_GID", "0"),
        os.environ.get("READER_READONLY", "1"),
    )
    # Create a plain tar stream from the pod and transcode to zip on the fly
    resp = _exec_tar_stream(ns, pod_name, gzip=False)

    def tar_iter() -> Iterator[bytes]:
        bytes_read = 0
        last_log = 0
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
                        # Always surface likely permission or access errors
                        estr = err if isinstance(err, str) else err.decode(errors="replace")
                        if any(k in estr for k in ["Permission denied", "permission denied", "Operation not permitted", "No such file or directory"]):
                            logger.warning("PVC tar stderr: %s", estr.strip())
                        elif ARCHIVE_DEBUG:
                            logger.debug("PVC tar stderr: %s", estr.strip())
        finally:
            resp.close()
            logger.info("PVC tar stream closed: ns=%s pvc=%s bytes=%d", ns, pvc, bytes_read)

    return StreamingResponse(
        _tar_to_zip_stream(tar_iter()),
        media_type="application/zip",
        headers=headers,
        background=BackgroundTask(cleanup),
    )


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host=host, port=port)
