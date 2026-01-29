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
* Find the associated cnpg pod
* Connect to the postgresql sevrer on the cnpg pod as the `streaming_replica` role using the postgresql client protocol. Use the secrets under Source Database Access to authenticate the connection.
* Call `reflink_snapshot(label)`
* Create the PV object, referencing the local filesystem directory with `hostPath` or `local`

## Environment Variables

HOSTPATH_PREFIX - where the snapshots will be created, used to configure
the the `hostPath.path` or `local` volume.

NAMESPACE_PATH - comma-delimited list of namespaces to search for the source PVC
if the dataSourceRef does not specify a namespace (e.g., due to API server filtering).
The operator first checks the current namespace, then each namespace in this list.

## Snapshot and PersistentVolume Naming (GUID-based)

To guarantee uniqueness and prevent race conditions, each snapshot and PersistentVolume (PV) is named using a randomly generated GUID (UUID4). The GUID is used as the snapshot label, the PV name, and is stored in PV annotations. The original PVC name and namespace are stored in the PV's claimRef (to prevent it from being inadvertently claimed by another pvc) and as annotations for traceability.

This approach ensures that deleting and recreating a PVC with the same name will not result in naming conflicts or resource collisions.

## PV Cleanup and Snapshot Deletion

When a PersistentVolume (PV) managed by pg-reflinker is deleted, the operator will:

* Read the PV's annotations to determine the source cluster, namespace, backup label, and snapshot path.
* Connect to the source CNPG cluster using the same secret-based authentication as for snapshot creation.
* Call the `delete_snapshot(label)` function in the source database to remove the snapshot associated with the PV.
* Errors during cleanup are logged, but do not block PV deletion.

The goal is to ensure that storage is reclaimed and old snapshots do not accumulate when PVs are removed.

## PersistentVolume Reclaim Policy

When creating a PersistentVolume (PV), the operator reads the reclaim policy from the associated StorageClass. If the StorageClass does not specify a reclaim policy, the operator defaults to 'Retain' to prevent surpise deletes.

### Handling Failed PersistentVolumes

If a PersistentVolume managed by pg-reflinker enters the "Failed" phase, the operator will automatically delete the PV. A common cause is the local volume provisioner's inability to delete backing directories outside of `/tmp`. This auto-deletion triggers the same cleanup process as a regular PV deletion, ensuring that any associated reflink snapshots are also removed from the source database.
