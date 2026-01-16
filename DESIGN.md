# pg-reflinker

## Overview

This controller watches for PVCs requesting a particular storage class,
contacts another controller to request a CNPG reflink snapshot, and then
creates a hostPath/local PV for that snapshot.

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

## Reconciliation Loop

* Watch for new PVC objects matching our storageClassName
* Confirm the referenced pg cluster exists and is on our node, otherwise skip it.
* Connect to the source PG cluster with admin user and postgresql client protocol
* `pg_backup_start('namespace/pvcname', true)`
* Request a reflink snapshot into a local filesystem directory which will become the PV
* Populate the `backup_label` file with the response from `pg_backup_stop()`
* Create the PV object, referencing the local filesystem directory with `hostPath` or `local`
