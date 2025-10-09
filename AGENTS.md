# Repository Guidelines

## Project Structure & Module Organization
- `src/`: Container build context (e.g., `Dockerfile`, app scripts/binaries).
- `k8s/`: Kubernetes manifests for deployment and RBAC.
- `scripts/`: Helper scripts for local dev and CI.
- `.github/workflows/`: GitHub Actions (see `build.yaml` for Docker build/push).
- `tests/`: Optional smoke/integration tests for the container.

## Build, Test, and Development Commands
- Build image (local): `docker build -f src/Dockerfile -t web-k8s-backupper:dev src`
- Run smoke test: `docker run --rm -e DRY_RUN=1 web-k8s-backupper:dev --help`
- Push (manual): `docker tag web-k8s-backupper:dev lukaspastva/web-k8s-backupper:dev && docker push lukaspastva/web-k8s-backupper:dev`
- CI builds/pushes on `main` via `build.yaml` (tags: `latest`, `${GITHUB_SHA}`).

## Coding Style & Naming Conventions
- Shell scripts: POSIX-compatible, `#!/usr/bin/env bash` with `set -Eeuo pipefail`; 2-space indent; snake_case filenames (e.g., `backup_runner.sh`).
- Dockerfile: pin base images (e.g., `debian:12.5`), minimize layers, non-root user.
- YAML (K8s): 2-space indent; filenames `component-kind.yaml` (e.g., `backup-cronjob.yaml`).
- Markdown/docs: concise, wrap ~100 chars, use fenced code blocks.

## Testing Guidelines
- Place tests in `tests/`. Prefer container-level smoke tests over unit tests when logic is in scripts.
- Static checks: `shellcheck scripts/*.sh` and `hadolint src/Dockerfile` (if tools available).
- Runtime check: `docker run --rm web-k8s-backupper:dev --version` or `--help` must succeed.
- If adding language-specific code, include framework tests and document how to run them.

## Commit & Pull Request Guidelines
- Use Conventional Commits: `feat: ...`, `fix: ...`, `chore: ...`, `docs: ...`, `ci: ...`.
- Scope small, descriptive commits; reference issues (`Refs #123`).
- PRs must include: summary, rationale, screenshots/logs (if relevant), and notes on deployment impact.
- Update `k8s/` and docs when behavior or interfaces change.

## Security & Configuration Tips
- Never commit secrets. Use GitHub Secrets for `DOCKER_HUB_USERNAME` and `DOCKER_HUB_PASSWORD` (used by CI).
- Prefer read-only K8s permissions; avoid storing kubeconfig in the image.
- Scrub logs of credentials, tokens, and bucket URIs.
