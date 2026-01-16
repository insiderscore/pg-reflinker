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

def get_cnpg_pod(cluster_name, namespace):
    """Get the CNPG pod for a cluster in a namespace."""
    pods = v1.list_namespaced_pod(namespace, label_selector=f'cnpg.io/cluster={cluster_name}')
    if not pods.items:
        raise kopf.TemporaryError(f"No pods found for cluster {cluster_name} in namespace {namespace}", delay=30)
    return pods.items[0]

def get_db_secrets(cluster_name, namespace):
    """Get the replication and CA secrets for a cluster."""
    replication_secret_name = f'{cluster_name}-replication'
    ca_secret_name = f'{cluster_name}-ca'
    replication_secret = v1.read_namespaced_secret(replication_secret_name, namespace)
    ca_secret = v1.read_namespaced_secret(ca_secret_name, namespace)
    return replication_secret, ca_secret

def create_temp_certs(replication_secret, ca_secret):
    """Create temporary certificate files and return their paths."""
    client_cert_path = None
    client_key_path = None
    ca_cert_path = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.crt') as client_cert_file:
            client_cert_file.write(base64.b64decode(replication_secret.data['tls.crt']).decode('utf-8'))
            client_cert_path = client_cert_file.name
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.key') as client_key_file:
            client_key_file.write(base64.b64decode(replication_secret.data['tls.key']).decode('utf-8'))
            client_key_path = client_key_file.name
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.crt') as ca_cert_file:
            ca_cert_file.write(base64.b64decode(ca_secret.data['ca.crt']).decode('utf-8'))
            ca_cert_path = ca_cert_file.name
        
        return client_cert_path, client_key_path, ca_cert_path
    except Exception:
        # Clean up on failure
        for path in [client_cert_path, client_key_path, ca_cert_path]:
            if path and os.path.exists(path):
                os.unlink(path)
        raise

def connect_to_db(db_host, db_port, db_user, db_name, client_cert_path, client_key_path, ca_cert_path):
    """Connect to PostgreSQL with TLS certificate authentication."""
    return psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        dbname=db_name,
        sslmode='verify-ca',
        sslcert=client_cert_path,
        sslkey=client_key_path,
        sslrootcert=ca_cert_path
    )

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

    # Get CNPG pod and secrets
    pod = get_cnpg_pod(cluster_name, source_namespace)
    replication_secret, ca_secret = get_db_secrets(cluster_name, source_namespace)
    client_cert_path, client_key_path, ca_cert_path = create_temp_certs(replication_secret, ca_secret)
    
    try:
        # Connect to PostgreSQL with TLS cert auth
        conn = connect_to_db(pod.status.pod_ip, 5432, 'streaming_replica', 'postgres', client_cert_path, client_key_path, ca_cert_path)
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
                    'pg-reflinker/source-backup-label': backup_label,
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

@kopf.on.delete('persistentvolume')
def handle_pv_delete(name, **kwargs):
    """
    Handle deletion of PVs managed by pg-reflinker to clean up reflink snapshots.
    """
    # Check if this PV is managed by us
    labels = kwargs.get('labels', {})
    if labels.get('app.kubernetes.io/managed-by') != 'pg-reflinker':
        return  # Not our PV

    # Get source information from annotations
    annotations = kwargs.get('annotations', {})
    source_cluster = annotations.get('pg-reflinker/source-cluster')
    source_namespace = annotations.get('pg-reflinker/source-namespace')
    source_backup_label = annotations.get('pg-reflinker/source-backup-label')

    if not all([source_cluster, source_namespace, source_backup_label]):
        return  # Missing required information

    # Try to connect to the source cluster and delete the snapshot
    try:
        pod = get_cnpg_pod(source_cluster, source_namespace)
        replication_secret, ca_secret = get_db_secrets(source_cluster, source_namespace)
        client_cert_path, client_key_path, ca_cert_path = create_temp_certs(replication_secret, ca_secret)
        
        try:
            conn = connect_to_db(pod.status.pod_ip, 5432, 'streaming_replica', 'postgres', client_cert_path, client_key_path, ca_cert_path)
            conn.autocommit = True
            
            try:
                with conn.cursor() as cur:
                    # Call the delete_snapshot function
                    cur.execute("SELECT delete_snapshot(%s)", (source_backup_label,))
            finally:
                conn.close()
        finally:
            # Clean up temp files
            os.unlink(client_cert_path)
            os.unlink(client_key_path)
            os.unlink(ca_cert_path)
    except Exception as e:
        # Log the error but don't fail the PV deletion
        # In a real implementation, you'd want proper logging
        pass

@kopf.on.update('persistentvolume', labels={'app.kubernetes.io/managed-by': 'pg-reflinker'}, field='status.phase')
def handle_pv_failed(old, new, **kwargs):
    """
    Handle PVs that enter Failed phase by auto-deleting them for cleanup.
    """
    if new == 'Failed':
        # Get the PV object from kwargs
        pv = kwargs['body']
        name = pv['metadata']['name']
        v1.delete_persistent_volume(name)

def main():
    kopf.run()

if __name__ == '__main__':
    main()