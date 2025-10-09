import os
import time
from typing import Dict, List, Iterator

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
def download_manifests(ns: str, includeSecrets: bool = False):
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
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return Response(content=content, media_type="application/x-yaml", headers=headers)


def ensure_helper_pod(ns: str, pvc: str) -> str:
    name = f"wkb-pvc-reader-{pvc[:40]}-{int(time.time())}"
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
                    volume_mounts=[client.V1VolumeMount(name="data", mount_path="/data")],
                    security_context=client.V1SecurityContext(run_as_non_root=True, allow_privilege_escalation=False),
                )
            ],
            volumes=[client.V1Volume(name="data", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=pvc))],
        ),
    )
    try:
        core.create_namespaced_pod(namespace=ns, body=pod)
    except client.exceptions.ApiException as e:
        if e.status != 409:
            raise

    # Wait until Running
    for _ in range(120):
        p = core.read_namespaced_pod(name=name, namespace=ns)
        if p.status.phase == "Running":
            return name
        if p.status.phase in ("Failed", "Succeeded"):
            break
        time.sleep(1)
    raise HTTPException(status_code=500, detail="Helper pod did not become Running")


def stream_tar_from_pod(ns: str, pod_name: str) -> Iterator[bytes]:
    cmd = ["tar", "czf", "-", "-C", "/data", "."]
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
                yield chunk
            if resp.peek_stderr():
                _ = resp.read_stderr()
    finally:
        resp.close()


@app.get("/api/namespaces/{ns}/pvcs/{pvc}/download")
def download_pvc(ns: str, pvc: str):
    pod_name = ensure_helper_pod(ns, pvc)
    filename = f"{ns}-{pvc}.tar.gz"
    headers = {"Content-Disposition": f"attachment; filename={filename}"}

    def cleanup():
        try:
            core.delete_namespaced_pod(name=pod_name, namespace=ns, grace_period_seconds=0)
        except Exception:
            pass

    return StreamingResponse(
        stream_tar_from_pod(ns, pod_name),
        media_type="application/gzip",
        headers=headers,
        background=BackgroundTask(cleanup),
    )


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host=host, port=port)
