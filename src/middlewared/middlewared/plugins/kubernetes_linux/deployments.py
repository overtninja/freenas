from kubernetes_asyncio import client

from middlewared.schema import Dict, Ref, Str
from middlewared.service import accepts, CallError, CRUDService, filterable
from middlewared.utils import filter_list

from .k8s import api_client


class KubernetesDeploymentService(CRUDService):

    class Config:
        namespace = 'k8s.deployment'
        private = True

    @filterable
    async def query(self, filters=None, options=None):
        async with api_client() as (api, context):
            return filter_list(
                [d.to_dict() for d in (await context['apps_api'].list_deployment_for_all_namespaces()).items],
                filters, options
            )

    @accepts(
        Dict(
            'deployment_create',
            Str('namespace', required=True),
            Dict('body', additional_attrs=True, required=True),
            register=True
        )
    )
    async def do_create(self, data):
        async with api_client() as (api, context):
            try:
                await context['apps_api'].create_namespaced_deployment(namespace=data['namespace'], body=data['body'])
            except client.exceptions.ApiException as e:
                raise CallError(f'Unable to create deployment: {e}')

    @accepts(
        Str('name'),
        Ref('deployment_create'),
    )
    async def do_update(self, name, data):
        async with api_client() as (api, context):
            try:
                await context['apps_api'].patch_namespaced_deployment(
                    name, namespace=data['namespace'], body=data['body']
                )
            except client.exceptions.ApiException as e:
                raise CallError(f'Unable to patch {name} deployment: {e}')

    @accepts(
        Str('name'),
        Dict(
            'deployment_delete_options',
            Str('namespace', required=True),
        )
    )
    async def do_delete(self, name, options):
        async with api_client() as (api, context):
            try:
                await context['apps_api'].delete_namespaced_deployment(name, options['namespace'])
            except client.exceptions.ApiException as e:
                raise CallError(f'Unable to delete deployment: {e}')
