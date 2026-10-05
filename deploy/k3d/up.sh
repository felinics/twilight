#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

cluster="${K3D_CLUSTER:-twilight}"
namespace="${TWILIGHT_NAMESPACE:-twilight}"
password="${TWILIGHT_POSTGRES_PASSWORD:-twilight-dev}"

: "${OPENAI_API_KEY:?set OPENAI_API_KEY before starting the k3d deployment}"

if ! k3d cluster list --no-headers 2>/dev/null | awk '{print $1}' | grep -qx "$cluster"; then
  echo "k3d cluster '$cluster' does not exist" >&2
  echo "Create it with: k3d cluster create $cluster" >&2
  exit 1
fi

platform="${TWILIGHT_PLATFORM:-}"
if [[ -z "$platform" ]]; then
  case "$(docker info --format '{{.Architecture}}')" in
    aarch64|arm64) platform=linux/arm64 ;;
    amd64|x86_64) platform=linux/amd64 ;;
    *) echo "unsupported Docker architecture" >&2; exit 1 ;;
  esac
fi

docker build --platform "$platform" -t twilight:dev .
k3d image import twilight:dev --cluster "$cluster"

kubectl apply -f deploy/k3d/namespace.yaml

if [[ -n "${TWILIGHT_POSTGRES_DSN:-}" ]]; then
  dsn="$TWILIGHT_POSTGRES_DSN"
else
  dsn="postgres://twilight:${password}@postgres:5432/twilight?sslmode=disable"
fi

kubectl -n "$namespace" create secret generic twilight-secrets \
  --from-literal="postgres-password=$password" \
  --from-literal="postgres-dsn=$dsn" \
  --from-literal="OPENAI_API_KEY=$OPENAI_API_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -k deploy/k3d
kubectl -n "$namespace" rollout status statefulset/postgres --timeout=180s
for deployment in owner worker model-backend tool-backend; do
  kubectl -n "$namespace" rollout status "deployment/$deployment" --timeout=180s
done

echo
printf 'Twilight is ready. Access the owner API with:\n'
printf '  kubectl -n %s port-forward svc/owner 8080:8080\n' "$namespace"
