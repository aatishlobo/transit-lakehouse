# Kubernetes deployment

Manifests for the always-on tier plus the ephemeral streaming tier.

**The split is deliberate** (CLAUDE.md section 7). The serving tier is small,
cheap, and runs continuously so the demo is always clickable. Kafka, Spark and
the batch jobs are ephemeral: they are scaled to zero or run as Jobs, because
keeping them warm costs money to produce nothing between polls.

| Component | Kind | Replicas | Why |
|---|---|---|---|
| `poller` | Deployment | **exactly 1** | see below |
| `api` + dashboard | Deployment | 2 | stateless reads of a SQLite artifact |
| Kafka (Strimzi) | Kafka CR | 3 brokers | RF=3, `min.insync.replicas=2` |
| `arrival-resolver` | Deployment + KEDA | 0..3 | scales on consumer lag |
| batch (bronze..marts) | CronJob | - | one graph, one schedule |

## The poller is exactly one replica

It is **not** a Kafka consumer. It has no partition assignment, no consumer
group, and no lag to divide. N replicas means N times the API calls against a
source with a 60 requests/hour quota, so the second replica does not double
throughput -- it halves the effective poll interval budget and gets the token
throttled. The Deployment therefore pins `replicas: 1` with
`strategy: Recreate`, and a PodDisruptionBudget prevents an eviction from
briefly running two.

Consumers scale on lag. The poller never does. This corrects an error in the
original project spec.

## KEDA scales on lag, not CPU

A consumer that is behind is not necessarily busy -- it may be waiting on a
slow write. CPU-based autoscaling therefore scales the wrong signal. `maxReplicaCount`
is capped at the partition count (3): partitions are the unit of parallelism,
and replica four would idle forever holding no assignment.

## Verified deployment

The serving tier has been deployed and exercised on minikube (qemu2, k8s 1.35):

```
$ kubectl -n transit get pods
NAME                   READY   STATUS    RESTARTS   AGE
api-686fdc87c7-kslxh   1/1     Running   0          19s
api-686fdc87c7-xz7lc   1/1     Running   0          19s

$ kubectl -n transit get svc api
api    NodePort   10.96.182.214   <none>   80:30080/TCP

$ curl http://api.transit.svc.cluster.local/health     # from inside the cluster
{"status":"ok","freshness":{"is_stale":false,"age_hours":7.6,...}}
```

Use `k8s/local/api-local.yaml` for a local cluster. It differs from
`k8s/api.yaml` in exactly one respect: the exported marts are baked into the
image rather than mounted from a ReadWriteMany PVC, because minikube's default
storage class is ReadWriteOnce only. Replicas, probes and resource envelope are
identical, so it exercises the real manifest shape.

```bash
minikube start --driver=qemu2
eval $(minikube docker-env)
docker build -t transit-lakehouse/api:latest -f Dockerfile.api .
kubectl apply -f k8s/namespace.yaml -f k8s/local/api-local.yaml
minikube service api -n transit --url
```

The Kafka, poller and batch manifests require Strimzi and KEDA operators plus
images for the Spark stack; they are authored but have not been applied.

## Apply (full stack)

```bash
kubectl apply -k k8s/                # everything
kubectl -n transit get pods
```

Secrets are NOT in these manifests. Create the 511 key out of band:

```bash
kubectl -n transit create secret generic transit-511 \
  --from-literal=API_511_KEY=...
```
