#!/bin/bash
set -e

echo "→ Building Docker image..."
docker build -t foxwatch:latest .

echo "→ Importing into k3s..."
docker save foxwatch:latest | sudo k3s ctr images import -

echo "→ Restarting deployment..."
kubectl rollout restart deployment/foxwatch-deployment -n staging

echo "→ Waiting for rollout..."
kubectl rollout status deployment/foxwatch-deployment -n staging

echo "✓ Done"
