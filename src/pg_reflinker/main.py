#!/usr/bin/env python3
"""
pg-reflinker: Kubernetes operator for creating reflink snapshots of CNPG clusters.
"""

import kopf
import kubernetes
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import psycopg2
import os
import tempfile
import base64

# Load Kubernetes config
try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()

# Kubernetes API clients
v1 = client.CoreV1Api()
storage_v1 = client.StorageV1Api()
custom_api = client.CustomObjectsApi()

# Environment variables
HOSTPATH_PREFIX = os.getenv('HOSTPATH_PREFIX', '/var/lib/pg-reflinker')

@kopf.on.create('persistentvolumeclaim')
def handle_pvc_create(spec, name, namespace, **kwargs):
    """
    Handle creation of PVCs with storageClassName 'pg-reflinker'.
    """
    # Check if storage class matches
    storage_class_name = spec.get('storageClassName')
    if storage_class_name != 'pg-reflinker':
        return  # Not our concern

    # Get the storage class to determine reclaim policy
    try:
        storage_class = storage_v1.read_storage_class(storage_class_name)
        reclaim_policy = storage_class.reclaim_policy or 'Retain'
    except ApiException as e:
        raise kopf.PermanentError(f"Failed to read storage class {storage_class_name}: {e}")

    # Extract dataSourceRef
    data_source = spec.get('dataSourceRef')
    if not data_source or data_source.get('kind') != 'PersistentVolumeClaim':
        raise kopf.PermanentError("dataSourceRef must point to a PVC")

    source_pvc_name = data_source['name']

    # Determine source namespace
    if 'namespace' in data_source:
        source_namespace = data_source['namespace']
        try:
            source_pvc = v1.read_namespaced_persistent_volume_claim(source_pvc_name, source_namespace)
        except ApiException as e:
            if e.status == 404:
                raise kopf.PermanentError(f"Source PVC {source_pvc_name} not found in namespace {source_namespace}")
            raise
    else:
        # No namespace specified, try current namespace and NAMESPACE_PATH
        namespace_path = os.getenv('NAMESPACE_PATH', '')
        candidate_namespaces = [namespace]
        if namespace_path:
            candidate_namespaces += [ns.strip() for ns in namespace_path.split(',') if ns.strip()]

        source_pvc = None
        source_namespace = None
        for ns in candidate_namespaces:
            try:
                source_pvc = v1.read_namespaced_persistent_volume_claim(source_pvc_name, ns)
                source_namespace = ns
                break
            except ApiException as e:
                if e.status != 404:
                    raise  # Re-raise non-404 errors
        if not source_pvc:
            raise kopf.PermanentError(f"Source PVC {source_pvc_name} not found in any candidate namespace: {candidate_namespaces}")

    # Find the CNPG cluster from ownerReferences of the source PVC
    owner_refs = source_pvc.metadata.owner_references or []
    cluster_name = None
    for ref in owner_refs:
        if ref.kind == 'Cluster' and ref.api_version == 'postgresql.cnpg.io/v1':
            cluster_name = ref.name
            break

    if not cluster_name:
        raise kopf.PermanentError("Source PVC must be owned by a CNPG Cluster")

    # Find the CNPG pod (assuming one pod per cluster for simplicity)
    pods = v1.list_namespaced_pod(source_namespace, label_selector=f'cnpg.io/cluster={cluster_name}')
    if not pods.items:
        raise kopf.TemporaryError("No pods found for cluster", delay=30)
    pod = pods.items[0]  # Take the first one

    # Get database connection secrets (in the source namespace)
    replication_secret_name = f'{cluster_name}-replication'
    ca_secret_name = f'{cluster_name}-ca'
    
    replication_secret = v1.read_namespaced_secret(replication_secret_name, source_namespace)
    ca_secret = v1.read_namespaced_secret(ca_secret_name, source_namespace)
    
    db_host = pod.status.pod_ip
    db_port = 5432
    db_user = 'streaming_replica'
    db_name = 'postgres'
    
    # Write certs to temp files
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.crt') as client_cert_file:
        client_cert_file.write(base64.b64decode(replication_secret.data['tls.crt']).decode('utf-8'))
        client_cert_path = client_cert_file.name
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.key') as client_key_file:
        client_key_file.write(base64.b64decode(replication_secret.data['tls.key']).decode('utf-8'))
        client_key_path = client_key_file.name
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.crt') as ca_cert_file:
        ca_cert_file.write(base64.b64decode(ca_secret.data['ca.crt']).decode('utf-8'))
        ca_cert_path = ca_cert_file.name
    
    try:
        # Connect to PostgreSQL with TLS cert auth
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            dbname=db_name,
            sslmode='verify-ca',
            sslcert=client_cert_path,
            sslkey=client_key_path,
            sslrootcert=ca_cert_path
        )
        conn.autocommit = True
        
        try:
            with conn.cursor() as cur:
                # Call the reflink_snapshot function
                backup_label = f'{namespace}-{name}'
                cur.execute("SELECT reflink_snapshot(%s)", (backup_label,))
                result = cur.fetchone()
                if result:
                    snapshot_path = result[0]
                else:
                    raise kopf.PermanentError("reflink_snapshot returned no result")
        finally:
            conn.close()
        
        # Create the PV
        pv_path = os.path.join(HOSTPATH_PREFIX, snapshot_path.lstrip('/'))
        pv = client.V1PersistentVolume(
            api_version='v1',
            kind='PersistentVolume',
            metadata=client.V1ObjectMeta(
                name=f'pv-{name}',
                labels={'app.kubernetes.io/managed-by': 'pg-reflinker'},
                annotations={
                    'pg-reflinker/source-cluster': cluster_name,
                    'pg-reflinker/source-namespace': source_namespace,
                    'pg-reflinker/source-pvc': source_pvc_name,
                    'pg-reflinker/backup-label': backup_label,
                }
            ),
            spec=client.V1PersistentVolumeSpec(
                capacity={'storage': spec['resources']['requests']['storage']},
                access_modes=['ReadWriteOnce'],
                host_path=client.V1HostPathVolumeSource(path=pv_path),
                storage_class_name=storage_class_name,
                persistent_volume_reclaim_policy=reclaim_policy,
                claim_ref=client.V1ObjectReference(
                    kind='PersistentVolumeClaim',
                    name=name,
                    namespace=namespace
                )
            )
        )
        v1.create_persistent_volume(pv)
    finally:
        # Clean up temp files
        os.unlink(client_cert_path)
        os.unlink(client_key_path)
        os.unlink(ca_cert_path)

def main():
    kopf.run()

if __name__ == '__main__':
    main()