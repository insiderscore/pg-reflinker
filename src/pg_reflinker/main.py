import kopf
import kubernetes
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import psycopg2
import os
import tempfile
import base64
import uuid

# Load Kubernetes config
try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()

# Kubernetes API clients
v1 = client.CoreV1Api()
storage_v1 = client.StorageV1Api()
custom_api = client.CustomObjectsApi()
batch_v1 = client.BatchV1Api()

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

def get_db_connection(cluster_name, namespace):
    """Get a database connection for a CNPG cluster."""
    pod = get_cnpg_pod(cluster_name, namespace)
    replication_secret, ca_secret = get_db_secrets(cluster_name, namespace)
    client_cert_path, client_key_path, ca_cert_path = create_temp_certs(replication_secret, ca_secret)
    
    try:
        conn = connect_to_db(pod.status.pod_ip, 5432, 'streaming_replica', 'postgres', client_cert_path, client_key_path, ca_cert_path)
        # Clean up cert files immediately after successful connection
        # psycopg2 loads certificates into memory during connection
        for path in [client_cert_path, client_key_path, ca_cert_path]:
            if path and os.path.exists(path):
                os.unlink(path)
        return conn
    except Exception:
        # Clean up certs on connection failure
        for path in [client_cert_path, client_key_path, ca_cert_path]:
            if path and os.path.exists(path):
                os.unlink(path)
        raise

@kopf.on.create('persistentvolumeclaim')
def handle_pvc_create(spec, meta, name, namespace, **kwargs):
    """
    Handle creation of PVCs with storageClassName 'pg-reflinker'.
    """

    # Get the storage class and check provisioner
    storage_class_name = spec.get('storageClassName')
    if not storage_class_name:
        return  # No storage class specified
    try:
        storage_class = storage_v1.read_storage_class(storage_class_name)
    except ApiException as e:
        raise kopf.PermanentError(f"Failed to read storage class {storage_class_name}: {e}")

    if storage_class.provisioner != 'k8s.insiderscore.com/pg-reflinker':
        return  # Not our concern

    reclaim_policy = storage_class.reclaim_policy or 'Retain'

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

    # Ensure the source PVC is bound before proceeding
    if source_pvc.status.phase != 'Bound':
        raise kopf.TemporaryError(f"Source PVC {source_pvc_name} in namespace {source_namespace} is not bound yet", delay=30)

    # Find the CNPG cluster from ownerReferences of the source PVC
    owner_refs = source_pvc.metadata.owner_references or []
    cluster_name = None
    for ref in owner_refs:
        if ref.kind == 'Cluster' and ref.api_version == 'postgresql.cnpg.io/v1':
            cluster_name = ref.name
            break

    if not cluster_name:
        raise kopf.PermanentError("Source PVC must be owned by a CNPG Cluster")

    # Get the PostgreSQL image from the CNPG cluster spec
    try:
        cluster = custom_api.get_namespaced_custom_object('postgresql.cnpg.io', 'v1', source_namespace, 'clusters', cluster_name)
        postgres_image = cluster.get('spec', {}).get('imageName')
    except ApiException as e:
        raise kopf.PermanentError(f"Failed to get cluster {cluster_name}: {e}")
    
    if not postgres_image:
        postgres_image = 'postgres:16'  # default fallback

    # Get node affinity from the source PVC's bound PV
    node_affinity = None
    if source_pvc.spec.volume_name:
        try:
            source_pv = v1.read_persistent_volume(source_pvc.spec.volume_name)
            node_affinity = source_pv.spec.node_affinity
        except ApiException as e:
            # Log but don't fail - node affinity is optional
            pass

    # Get CNPG pod and secrets
    try:
        pod = get_cnpg_pod(cluster_name, source_namespace)
        replication_secret, ca_secret = get_db_secrets(cluster_name, source_namespace)
    except Exception as e:
        raise kopf.TemporaryError(f"Failed to get pod or secrets: {e}", delay=30)
    
    source_node = pod.spec.node_name
    pod_ip = pod.status.pod_ip

    # Generate GUID for the snapshot - use PVC's UID for uniqueness
    guid = meta.get('uid', str(uuid.uuid4()))
    pv_name = f'pvc-{guid}'
    pv_path = os.path.join(HOSTPATH_PREFIX, guid)

    # Create the PV with local volume
    pv = client.V1PersistentVolume(
        api_version='v1',
        kind='PersistentVolume',
        metadata=client.V1ObjectMeta(
            name=pv_name,
            labels={'app.kubernetes.io/managed-by': 'pg-reflinker'},
            annotations={
                'pg-reflinker/source-cluster': cluster_name,
                'pg-reflinker/source-namespace': source_namespace,
                'pg-reflinker/source-pvc': source_pvc_name,
                'pg-reflinker/source-backup-label': guid,
                'pg-reflinker/claim-namespace': namespace,
                'pg-reflinker/claim-name': name,
                'pg-reflinker/storage-class': storage_class_name,
                'pg-reflinker/node': source_node,
            }
        ),
        spec=client.V1PersistentVolumeSpec(
            capacity={'storage': spec['resources']['requests']['storage']},
            access_modes=['ReadWriteOnce'],
            local=client.V1LocalVolumeSource(path=pv_path),
            # storage_class_name not set initially to prevent premature binding
            persistent_volume_reclaim_policy=reclaim_policy,
            node_affinity=node_affinity,
        )
    )
    v1.create_persistent_volume(pv)

    # Create the Job to perform the reflink backup
    job_name = f'pg-reflinker-{guid}'
    script = f'''
#!/bin/bash
set -e
psql -h $PGHOST -p 5432 -U streaming_replica -d postgres -v ON_ERROR_STOP=1 -v TAG="$BACKUP_LABEL" <<EOF
SELECT pg_backup_start('reflinker-' || :'TAG', true);
\\! cp -a --reflink=always /source /dest/pgdata
\\t
\\a
\\o /dest/pgdata/backup_label
SELECT labelfile from pg_backup_stop(false);
\\o
EOF
'''
    job = client.V1Job(
        api_version='batch/v1',
        kind='Job',
        metadata=client.V1ObjectMeta(
            name=job_name,
            namespace=source_namespace,
            labels={'app.kubernetes.io/managed-by': 'pg-reflinker'},
            annotations={'pg-reflinker/pv-guid': guid}
        ),
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    node_name=source_node,
                    restart_policy='Never',
                    security_context=client.V1PodSecurityContext(fs_group=26),
                    init_containers=[
                        client.V1Container(
                            name='init-permissions',
                            image='busybox:1.36',
                            command=['sh', '-c', 'mkdir -p /dest && chown -R 26:26 /dest'],
                            security_context=client.V1SecurityContext(run_as_user=0, run_as_group=0),
                            volume_mounts=[
                                client.V1VolumeMount(
                                    name='dest-data',
                                    mount_path='/dest'
                                )
                            ]
                        )
                    ],
                    volumes=[
                        client.V1Volume(
                            name='source-data',
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=source_pvc_name,
                                read_only=True
                            )
                        ),
                        client.V1Volume(
                            name='dest-data',
                            host_path=client.V1HostPathVolumeSource(
                                path=pv_path
                            )
                        ),
                        client.V1Volume(
                            name='replication-secret',
                            secret=client.V1SecretVolumeSource(
                                secret_name=f'{cluster_name}-replication',
                                default_mode=0o640,
                                items=[
                                    client.V1KeyToPath(key='tls.crt', path='tls.crt', mode=0o640),
                                    client.V1KeyToPath(key='tls.key', path='tls.key', mode=0o640)
                                ]
                            )
                        ),
                        client.V1Volume(
                            name='ca-secret',
                            secret=client.V1SecretVolumeSource(
                                secret_name=f'{cluster_name}-ca',
                                default_mode=0o640
                            )
                        )
                    ],
                    containers=[
                        client.V1Container(
                            name='reflink-backup',
                            image=postgres_image,
                            security_context=client.V1SecurityContext(run_as_user=26, run_as_group=26),
                            command=['/bin/bash', '-c', script],
                            env=[
                                client.V1EnvVar(name='PGHOST', value=pod_ip),
                                client.V1EnvVar(name='BACKUP_LABEL', value=guid),
                                client.V1EnvVar(name='PGSSLMODE', value='verify-ca'),
                                client.V1EnvVar(name='PGSSLCERT', value='/secrets/replication/tls.crt'),
                                client.V1EnvVar(name='PGSSLKEY', value='/secrets/replication/tls.key'),
                                client.V1EnvVar(name='PGSSLROOTCERT', value='/secrets/ca/ca.crt')
                            ],
                            volume_mounts=[
                                client.V1VolumeMount(
                                    name='source-data',
                                    mount_path='/source'
                                ),
                                client.V1VolumeMount(
                                    name='dest-data',
                                    mount_path='/dest'
                                ),
                                client.V1VolumeMount(
                                    name='replication-secret',
                                    mount_path='/secrets/replication'
                                ),
                                client.V1VolumeMount(
                                    name='ca-secret',
                                    mount_path='/secrets/ca'
                                )
                            ]
                        )
                    ]
                )
            )
        )
    )
    batch_v1.create_namespaced_job(source_namespace, job)

@kopf.on.field('batch', 'v1', 'jobs', field='status.succeeded', labels={'app.kubernetes.io/managed-by': 'pg-reflinker'})
def handle_job_succeeded(old, new, name, namespace, logger, **kwargs):
    """
    Handle successful completion of the populator job by binding the PV to the PVC.
    """
    if not old and new == 1:
        guid = name.replace('pg-reflinker-', '')
        pv_name = f'pvc-{guid}'
        try:
            # Retrieve the PersistentVolume
            pv: client.V1PersistentVolume = v1.read_persistent_volume(pv_name)

            if not pv.spec:
                raise kopf.PermanentError(f"PersistentVolume {pv_name} is missing spec.")

            # Ensure metadata and annotations exist
            if not pv.metadata or not pv.metadata.annotations:
                raise kopf.PermanentError(f"PersistentVolume {pv_name} is missing metadata or annotations.")

            # Update the storage class and claim reference
            storage_class = pv.metadata.annotations.get('pg-reflinker/storage-class')
            claim_name = pv.metadata.annotations.get('pg-reflinker/claim-name')
            claim_namespace = pv.metadata.annotations.get('pg-reflinker/claim-namespace')

            if not storage_class or not claim_name or not claim_namespace:
                raise kopf.PermanentError(
                    f"PersistentVolume {pv_name} is missing required annotations for binding."
                )
            
            # Retrieve the PVC to get its UID for the claimRef
            try:
                pvc: client.V1PersistentVolumeClaim = v1.read_namespaced_persistent_volume_claim(claim_name, claim_namespace)
            except ApiException as e:
                raise kopf.PermanentError(f"Failed to retrieve PVC {claim_namespace}/{claim_name}: {e}")

            pv.spec.storage_class_name = storage_class
            pv.spec.claim_ref = client.V1ObjectReference(
                kind='PersistentVolumeClaim',
                name=claim_name,
                namespace=claim_namespace,
                uid=pvc.metadata.uid
            )

            # Replace the PersistentVolume with updated details
            v1.replace_persistent_volume(pv_name, pv)

            # Log success
            logger.info(
                f"Successfully bound PV {pv_name} to PVC {claim_namespace}/{claim_name}"
            )
        except ApiException as e:
            # Log error details
            error_message = f"API error while binding PV {pv_name}: {e.reason}"
            logger.error(error_message)
            raise kopf.PermanentError(error_message)
        except kopf.PermanentError as e:
            logger.error(str(e))
            raise

@kopf.on.field('batch', 'v1', 'jobs', field='status.failed', labels={'app.kubernetes.io/managed-by': 'pg-reflinker'})
def handle_job_failed(old, new, name, namespace, logger, **kwargs):
    if not old and new == 1:
        logger.warning(f"Job {name} in namespace {namespace} failed. Cleaning up resources.")
        guid = name.replace('pg-reflinker-', '')
        pv_name = f'pvc-{guid}'
        try:
            v1.delete_persistent_volume(pv_name)
            logger.info(f"Cleaned up PersistentVolume {pv_name} due to job failure.")
        except ApiException as e:
            if e.status == 404:
                logger.info(f"PersistentVolume {pv_name} already deleted.")
            else:
                logger.error(f"Failed to delete PersistentVolume {pv_name}: {e}")

@kopf.on.delete('persistentvolume', labels={'app.kubernetes.io/managed-by': 'pg-reflinker'})
def handle_pv_delete(name, logger, **kwargs):
    """
    Handle deletion of PVs managed by pg-reflinker to clean up local directories.
    """
    # Get PV info from kwargs
    pv = kwargs.get('body', {})
    spec = pv.get('spec', {})
    reclaim_policy = spec.get('persistentVolumeReclaimPolicy', 'Retain')
    annotations = pv.get('metadata', {}).get('annotations', {})
    
    guid = annotations.get('pg-reflinker/source-backup-label')
    if not guid:
        logger.warning(f"PV {name} is missing the source-backup-label annotation. Cannot clean up.")
        return
    
    parent_path = HOSTPATH_PREFIX
    
    if reclaim_policy == 'Delete':
        # For 'Delete' policy, we need to remove the directory from the host path
        # This requires running a privileged pod on the correct node.
        # For simplicity in this example, we will log the action.
        # A real implementation would create a cleanup job/pod.
        logger.info(f"PV {name} with reclaim policy 'Delete'. Manual cleanup of path may be required.")
        

@kopf.on.field('persistentvolume', field='status.phase', labels={'app.kubernetes.io/managed-by': 'pg-reflinker'})
def on_pv_phase_change(old, new, name, **kwargs):
    """
    When a PV becomes Released, if the reclaim policy is Retain, we should
    clean up the hostPath directory.
    """
    if new == 'Released':
        pv = v1.read_persistent_volume(name)
        if pv.spec.persistent_volume_reclaim_policy == 'Retain':
            # Similar to the delete handler, this would require a cleanup job.
            # For now, we just log it.
            pass

@kopf.on.update('batch', 'v1', 'jobs', labels={'app.kubernetes.io/managed-by': 'pg-reflinker'}, field='status.conditions')
def handle_job_failed(status, name, namespace, logger, **kwargs):
    conditions = status.get('conditions', [])
    for cond in conditions:
        if cond.get('type') == 'Failed' and cond.get('status') == 'True':
            logger.warning(f"Job {name} in namespace {namespace} failed. Cleaning up resources.")
            guid = name.split('-')[-1]
            pv_name = f'pvc-{guid}'
            try:
                v1.delete_persistent_volume(pv_name)
                logger.info(f"Cleaned up PersistentVolume {pv_name} due to job failure.")
            except ApiException as e:
                if e.status == 404:
                    logger.info(f"PersistentVolume {pv_name} already deleted.")
                else:
                    logger.error(f"Failed to delete PersistentVolume {pv_name}: {e}")
            breakguid = name.split('-')[-1]
            pv_name = f'pvc-{guid}'
            try:
                v1.delete_persistent_volume(pv_name)
            except ApiException:
                pass
            break

@kopf.on.delete('persistentvolume', labels={'app.kubernetes.io/managed-by': 'pg-reflinker'})
def handle_pv_delete(name, **kwargs):
    """
    Handle deletion of PVs managed by pg-reflinker to clean up local directories.
    """
    # Get PV info from kwargs
    pv = kwargs.get('body', {})
    spec = pv.get('spec', {})
    reclaim_policy = spec.get('persistentVolumeReclaimPolicy', 'Retain')
    annotations = pv.get('metadata', {}).get('annotations', {})
    
    guid = annotations.get('pg-reflinker/source-backup-label')
    if not guid:
        return
    
    parent_path = HOSTPATH_PREFIX
    
    if reclaim_policy == 'Delete':
        # Create a cleanup Job to delete the local directory
        job_name = f'pg-reflinker-cleanup-{guid}'
        node = annotations.get('pg-reflinker/node')
        cleanup_namespace = annotations.get('pg-reflinker/source-namespace', 'default')
        
        job = client.V1Job(
            api_version='batch/v1',
            kind='Job',
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=cleanup_namespace,
                labels={'app.kubernetes.io/managed-by': 'pg-reflinker'}
            ),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        restart_policy='Never',
                        node_name=node,
                        volumes=[
                            client.V1Volume(
                                name='cleanup-data',
                                host_path=client.V1HostPathVolumeSource(path=parent_path)
                            )
                        ],
                        containers=[
                            client.V1Container(
                                name='cleanup',
                                image='busybox:1.36',
                                command=['sh', '-c', f'rm -rf /cleanup/{guid}'],
                                security_context=client.V1SecurityContext(run_as_user=0, run_as_group=0),
                                volume_mounts=[
                                    client.V1VolumeMount(
                                        name='cleanup-data',
                                        mount_path='/cleanup'
                                    )
                                ]
                            )
                        ]
                    )
                )
            )
        )
        batch_v1.create_namespaced_job(cleanup_namespace, job)

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
    kopf.run(namespace=None)

if __name__ == '__main__':
    main()