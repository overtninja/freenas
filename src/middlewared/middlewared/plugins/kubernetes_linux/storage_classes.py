import os

from kubernetes_asyncio import client

from middlewared.service import CallError, CRUDService, filterable
from middlewared.utils import filter_list

from .k8s import api_client


DEFAULT_STORAGE_CLASS = 'openebs-zfspv-default'


class KubernetesStorageClassService(CRUDService):

    class Config:
        namespace = 'k8s.storage_class'
        private = True

    @filterable
    async def query(self, filters=None, options=None):
        async with api_client() as (api, context):
            return filter_list(
                [d.to_dict() for d in (await context['storage_api'].list_storage_class()).items],
                filters, options
            )

    async def do_create(self, data):
        async with api_client() as (api, context):
            try:
                await context['storage_api'].create_storage_class(data)
            except client.exceptions.ApiException as e:
                raise CallError(f'Failed to create storage class: {e}')

    async def do_update(self, name, data):
        async with api_client() as (api, context):
            try:
                await context['storage_api'].patch_storage_class(name, data)
            except client.exceptions.ApiException as e:
                raise CallError(f'Failed to create storage class: {e}')

    async def do_delete(self, name):
        async with api_client() as (api, context):
            try:
                await context['storage_api'].delete_storage_class(name)
            except client.exceptions.ApiException as e:
                raise CallError(f'Failed to delete storage class: {e}')

    async def setup_default_storage_class(self):
        try:
            await self.setup_default_storage_class_internal()
        except Exception as e:
            # Let's not make this fatal as workloads managed by us will still be functional
            self.logger.error('Failed to setup default storage class: %s', e)

    async def setup_default_storage_class_internal(self):
        storage_ds = os.path.join((await self.middleware.call('kubernetes.config'))['dataset'], 'default_volumes')
        config = {
            'apiVersion': 'storage.k8s.io/v1',
            'kind': 'StorageClass',
            'metadata': {
                'name': DEFAULT_STORAGE_CLASS,
                'annotations': {'storageclass.kubernetes.io/is-default-class': 'true'}
            },
            'parameters': {'fstype': 'zfs', 'poolname': storage_ds},
            'provisioner': 'zfs.csi.openebs.io',
            'allowVolumeExpansion': True,
        }

        if await self.query([
            ['metadata.annotations.storageclass\\.kubernetes\\.io/is-default-class', '=', 'true'],
            ['metadata.name', '=', DEFAULT_STORAGE_CLASS],
        ]):
            await self.middleware.call('k8s.storage_class.update', DEFAULT_STORAGE_CLASS, config)
        else:
            await self.middleware.call('k8s.storage_class.create', config)
