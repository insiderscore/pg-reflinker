#!/usr/bin/env python3
"""
pg-reflinker: Kubernetes operator for creating reflink snapshots of CNPG clusters.
"""

import kopf
import kubernetes
from kubernetes import client, config
import psycopg2
import os

# Load Kubernetes config
config.load_incluster_config()  # or config.load_kube_config() for local dev

# Kubernetes API clients
v1 = client.CoreV1Api()
custom_api = client.CustomObjectsApi()

# Environment variables
HOSTPATH_PREFIX = os.getenv('HOSTPATH_PREFIX', '/var/lib/pg-reflinker')

@kopf.on.create('persistentvolumeclaim', labels={'app.kubernetes.io/managed-by': 'pg-reflinker'})
def handle_pvc_create(spec, name, namespace, **kwargs):
    """
    Handle creation of PVCs with storageClassName 'pg-reflinker'.
    """
    # Check if storage class matches
    if spec.get('storageClassName') != 'pg-reflinker':
        return  # Not our concern

    # Extract dataSourceRef
    data_source = spec.get('dataSourceRef')
    if not data_source or data_source.get('kind') != 'PersistentVolumeClaim':
        raise kopf.PermanentError("dataSourceRef must point to a PVC")

    source_pvc_name = data_source['name']

    # Find the CNPG cluster from ownerReferences
    source_pvc = v1.read_namespaced_persistent_volume_claim(source_pvc_name, namespace)
    owner_refs = source_pvc.metadata.owner_references or []
    cluster_name = None
    for ref in owner_refs:
        if ref.kind == 'Cluster' and ref.api_version == 'postgresql.cnpg.io/v1':
            cluster_name = ref.name
            break

    if not cluster_name:
        raise kopf.PermanentError("Source PVC must be owned by a CNPG Cluster")

    # Find the CNPG pod (assuming one pod per cluster for simplicity)
    pods = v1.list_namespaced_pod(namespace, label_selector=f'cnpg.io/cluster={cluster_name}')
    if not pods.items:
        raise kopf.TemporaryError("No pods found for cluster", delay=30)
    pod = pods.items[0]  # Take the first one

    # Get database connection secrets
    secret_name = f'{cluster_name}-replication'  # Adjust based on your setup
    secret = v1.read_namespaced_secret(secret_name, namespace)
    db_host = pod.status.pod_ip
    db_port = 5432  # Default PostgreSQL port
    db_user = 'streaming_replica'
    db_password = secret.data.get('password', '').decode('utf-8')  # Assuming base64 encoded
    db_name = 'postgres'  # Or whatever the maintenance DB is

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        dbname=db_name
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            # Call the reflink_snapshot function
            backup_label = f'{namespace}-{name}'
            cur.execute("SELECT reflink_snapshot(%s)", (backup_label,))
            snapshot_path = cur.fetchone()[0]

        # Create the PV
        pv = client.V1PersistentVolume(
            api_version='v1',
            kind='PersistentVolume',
            metadata=client.V1ObjectMeta(
                name=f'pv-{name}',
                labels={'app.kubernetes.io/managed-by': 'pg-reflinker'}
            ),
            spec=client.V1PersistentVolumeSpec(
                capacity={'storage': spec['resources']['requests']['storage']},
                access_modes=['ReadWriteOnce'],
                host_path=client.V1HostPathVolumeSource(path=snapshot_path),
                storage_class_name='',
                claim_ref=client.V1ObjectReference(
                    kind='PersistentVolumeClaim',
                    name=name,
                    namespace=namespace
                )
            )
        )
        v1.create_persistent_volume(pv)

    finally:
        conn.close()

def main():
    kopf.run()

if __name__ == '__main__':
    main()