# pg-reflinker

## Overview

This controller watches for PVCs requesting its storage class, contacts
the follower cluster to request a CNPG reflink snapshot on local storage,
and then creates a hostPath/local PV for that snapshot.

## Example PVC

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  annotations:
    cnpg.io/nodeSerial: "1"
    cnpg.io/operatorVersion: 1.28.0
  name: pg-xyz-1
  namespace: apps
  ownerReferences:
    - apiVersion: postgresql.cnpg.io/v1
      controller: true
      kind: Cluster
      name: pg-xyz
      uid: d82a45e1-3236-42a1-933f-2203991d1a66
spec:
  accessModes:
    - ReadWriteOnce
  dataSourceRef:
    apiGroup: ""
    kind: PersistentVolumeClaim
    name: pgdata-empty
  resources:
    requests:
      storage: 6400Gi
  storageClassName: pg-reflinker
  volumeMode: Filesystem
```

## Source Database Access

In the namespace of the CNPG `Cluster` object, there are secrets which
provide access to the `streaming_replica` role, which also has access to
invoke the `pg_backup_start()` and `pg_backup_stop()` functions.

```text
NAME                      TYPE                DATA   AGE
pg-xyz-ca            Opaque              2      21h
pg-xyz-replication   kubernetes.io/tls   2      21h
```

```yaml
apiVersion: v1
data:
  ca.crt: LS0..LQo=
  ca.key: LS0...LQo=
kind: Secret
metadata:
  creationTimestamp: "2026-01-15T17:52:04Z"
  labels:
    app.kubernetes.io/managed-by: cloudnative-pg
  name: pg-xyz-ca
  namespace: pg-xyz
```

## Reconciliation Loop

* Watch for new PVC objects matching our storageClassName
* Look at the volume specified in dataSourceRef
* Find the associated cnpg pod and cluster
* Create a PV with local volume on the same node as the source pod
* Create a Job that runs on that node, mounts the source PVC read-only and the destination local path as hostPath, connects to the database, performs pg_backup_start, reflink copy, pg_backup_stop, and writes the backup_label file
* When the Job succeeds, bind the PV to the PVC by setting claimRef

## The Populator Job

* Created in the same namespace as the source CNPG cluster
* Mount the source database PVC (read-only) and the destination local path (hostPath)
* Use an initContainer running as root to create the destination directory and set ownership to uid/gid 26 (postgres user)
* Use the same PostgreSQL image as specified in the source CNPG cluster spec to ensure psql compatibility
* Connect to the postgresql server on the cnpg pod as the `streaming_replica` role using the secrets under Source Database Access to authenticate the connection.
* Use psql with a here document to execute in a single session: `pg_backup_start(label, true)`, then shell command `cp -a --reflink=always $SOURCEMOUNT $DESTMOUNT/pgdata`, then `pg_backup_stop(false)` with output redirected to `$DESTMOUNT/pgdata/backup_label`
* Exit zero on success, non-zero on any failure

## Environment Variables

HOSTPATH_PREFIX - where the snapshots will be created, used to configure
the the `hostPath.path` or `local` volume.

NAMESPACE_PATH - comma-delimited list of namespaces to search for the source PVC
if the dataSourceRef does not specify a namespace (e.g., due to API server filtering).
The operator first checks the current namespace, then each namespace in this list.

## Snapshot and PersistentVolume Naming (PVC UID-based)

To guarantee uniqueness and prevent race conditions, each snapshot and PersistentVolume (PV) uses the PVC's metadata.uid as the identifier. The PV is named `pvc-<PVC_UID>` to follow Kubernetes naming conventions for storage controllers. The PVC UID is used as the snapshot label, stored in PV annotations, and used for internal tracking. The original PVC name and namespace are stored in the PV's claimRef (to prevent it from being inadvertently claimed by another pvc) and as annotations for traceability.

This approach ensures that each PVC creation results in a unique, deterministic identifier without conflicts.

## PV Cleanup and Snapshot Deletion

When a PersistentVolume (PV) managed by pg-reflinker is deleted, the operator checks the reclaim policy. If the policy is 'Delete', it creates a cleanup Job that runs on the same node to remove the local directory containing the snapshot data. If the policy is 'Retain' (the default), the data remains on the node for manual cleanup or reuse.

## PersistentVolume Reclaim Policy

When creating a PersistentVolume (PV), the operator reads the reclaim policy from the associated StorageClass. If the StorageClass does not specify a reclaim policy, the operator defaults to 'Retain' to prevent surpise deletes.

### Handling Failed PersistentVolumes

If a PersistentVolume managed by pg-reflinker enters the "Failed" phase, the operator will automatically delete the PV. A common cause is the local volume provisioner's inability to delete backing directories outside of `/tmp`. This auto-deletion triggers the same cleanup process as a regular PV deletion, ensuring that any associated reflink snapshots are also removed from the source database.
