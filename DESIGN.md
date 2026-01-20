# pg-reflinker

## Snapshot and PersistentVolume Naming (GUID-based)

To guarantee uniqueness and prevent race conditions, each snapshot and PersistentVolume (PV) is named using a randomly generated GUID (UUID4). The GUID is used as the snapshot label, the PV name, and is stored in PV annotations. The original PVC name and namespace are stored in both the PV's claimRef and as annotations for traceability.

This approach ensures that deleting and recreating a PVC with the same name will not result in naming conflicts or resource collisions.

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
  ca.crt: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJsekNDQVQ2Z0F3SUJBZ0lRUkw4eGlLSWV5WTBXU3MwcSsvTHFrVEFLQmdncWhrak9QUVFEQWpBc01SUXcKRWdZRFZRUUxFd3R3WnkxbWIyeHNiM2RsY2pFVU1CSUdBMVVFQXhNTGNHY3RabTlzYkc5M1pYSXdIaGNOTWpZdwpNVEUxTVRjME56QTBXaGNOTWpZd05ERTFNVGMwTnpBMFdqQXNNUlF3RWdZRFZRUUxFd3R3WnkxbWIyeHNiM2RsCmNqRVVNQklHQTFVRUF4TUxjR2N0Wm05c2JHOTNaWEl3V1RBVEJnY3Foa2pPUFFJQkJnZ3Foa2pPUFFNQkJ3TkMKQUFTMUFSc2EzU3hQTmtxTjlSc0NhWWM2Sjh2UmoyUERxQy93Q3BTUStnYnFNamtnQ3JlU2gvNml0SitQTlp3TApuN242MC9ObjdUcms5bThqNHVoWUt2NWtvMEl3UURBT0JnTlZIUThCQWY4RUJBTUNBZ1F3RHdZRFZSMFRBUUgvCkJBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVWI1OThiNlNoVkxicE0ydW1ha3NURm1tdWF6SXdDZ1lJS29aSXpqMEUKQXdJRFJ3QXdSQUlnQUpUS0cxUVZGWmJ5SDBrbm9FbUZ1bm9ldTlHRjB6UFRPcWFDQ0ZuVEJWd0NJRHE0VStwUApFSEU4ekpZd01zbEVvam8yWnNTN3FDQ29CR3hNck1zeVhKYnMKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
  ca.key: LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IY0NBUUVFSU1CQk9LVS9TUVk3MnNFUUJpWFU5dlJocGlkVGFwQi9xU3R3SnBXdXA5elFvQW9HQ0NxR1NNNDkKQXdFSG9VUURRZ0FFdFFFYkd0MHNUelpLamZVYkFtbUhPaWZMMFk5anc2Z3Y4QXFVa1BvRzZqSTVJQXEza29mKwpvclNmanpXY0M1KzUrdFB6WiswNjVQWnZJK0xvV0NyK1pBPT0KLS0tLS1FTkQgRUMgUFJJVkFURSBLRVktLS0tLQo=
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
