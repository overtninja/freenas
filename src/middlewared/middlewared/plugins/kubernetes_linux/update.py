import ipaddress
import os

import middlewared.sqlalchemy as sa

from middlewared.schema import Dict, IPAddr, Str
from middlewared.service import accepts, job, private, ConfigService, ValidationErrors


class KubernetesModel(sa.Model):
    __tablename__ = 'services_kubernetes'

    id = sa.Column(sa.Integer(), primary_key=True)
    pool = sa.Column(sa.String(255), default=None, nullable=True)
    cluster_cidr = sa.Column(sa.String(128), default='172.16.0.0/16')
    service_cidr = sa.Column(sa.String(128), default='172.17.0.0/16')
    cluster_dns_ip = sa.Column(sa.String(128), default='172.17.0.10')
    route_v4_interface = sa.Column(sa.String(128), nullable=True)
    route_v4_gateway = sa.Column(sa.String(128), nullable=True)
    route_v6_interface = sa.Column(sa.String(128), nullable=True)
    route_v6_gateway = sa.Column(sa.String(128), nullable=True)
    node_ip = sa.Column(sa.String(128), default='0.0.0.0')
    cni_config = sa.Column(sa.JSON(type=dict))


class KubernetesService(ConfigService):

    class Config:
        datastore = 'services.kubernetes'
        datastore_extend = 'kubernetes.k8s_extend'

    @private
    async def k8s_extend(self, data):
        data['dataset'] = os.path.join(data['pool'], 'ix-applications') if data['pool'] else None
        data.pop('cni_config')
        return data

    @private
    async def validate_data(self, data, schema):
        verrors = ValidationErrors()

        if data['pool'] and not await self.middleware.call('pool.query', [['name', '=', data['pool']]]):
            verrors.add(f'{schema}.pool', 'Please provide a valid pool configured in the system.')

        if ipaddress.ip_address(data['cluster_dns_ip']) not in ipaddress.ip_network(data['service_cidr']):
            verrors.add(f'{schema}.cluster_dns_ip', 'Must be in range of "service_cidr".')

        if data['node_ip'] not in await self.bindip_choices():
            verrors.add(f'{schema}.node_ip', 'Please provide a valid IP address.')

        for k, _ in await self.validate_interfaces(data):
            verrors.add(f'{schema}.{k}', 'Please specify a valid interface.')

        for k in ('route_v4_', 'route_v6_'):
            gateway = data[f'{k}_gateway']
            interface = data[f'{k}_interface']
            if (not gateway and not interface) or (gateway and interface):
                continue
            for k2 in ('gateway', 'interface'):
                verrors.add(f'{schema}.{k}{k2}', f'{k}_gateway and {k}_interface must be specified together.')

        verrors.check()

    @private
    async def validate_interfaces(self, data):
        errors = []
        interfaces = {i['name']: i for i in await self.middleware.call('interface.query')}
        for k in filter(
            lambda k: k in data and data[k] not in interfaces, ('route_v4_interface', 'route_v6_interface')
        ):
            errors.append((k, data[k]))
        return errors

    @accepts(
        Dict(
            'kubernetes_update',
            Str('pool', empty=False, null=True),
            IPAddr('cluster_cidr', cidr=True),
            IPAddr('service_cidr', cidr=True),
            IPAddr('cluster_dns_ip'),
            IPAddr('node_ip'),
            Str('route_v4_interface', null=True),
            Str('route_v4_gateway', null=True, v6=False),
            Str('route_v6_interface', null=True),
            IPAddr('route_v6_gateway', null=True, v4=False),
            update=True,
        )
    )
    @job(lock='kubernetes_update')
    async def do_update(self, job, data):
        """
        `pool` must be a valid ZFS pool configured in the system. Kubernetes service will initialise the pool by
        creating datasets under `pool_name/ix-applications`.

        `cluster_cidr` is the CIDR to be used for default NAT network between workloads.

        `service_cidr` is the CIDR to be used for kubernetes services which are an abstraction and refer to a
        logically set of kubernetes pods.

        `cluster_dns_ip` is the IP of the DNS server running for the kubernetes cluster. It must be in the range
        of `service_cidr`.

        Specifying values for `cluster_cidr`, `service_cidr` and `cluster_dns_ip` are permanent and a subsequent change
        requires re-initialisation of the applications. To clarify, system will destroy old `ix-applications` dataset
        and any data within it when any of the values for the above configuration change.

        `node_ip` is the IP address which the kubernetes cluster will assign to the TrueNAS node. It defaults to
        0.0.0.0 and the cluster in this case will automatically manage which IP address to use for managing traffic
        for default NAT network. If it is desired that traffic uses a certain interface / ip address, that IP address
        can be specified and the NAT network will use related IP address and it's routes to manage the traffic.
        """
        old_config = await self.config()
        old_config.pop('dataset')
        config = old_config.copy()
        config.update(data)

        await self.validate_data(config, 'kubernetes_update')

        if len(set(old_config.items()) ^ set(config.items())) > 0:
            config['cni_config'] = {}
            await self.middleware.call('datastore.update', self._config.datastore, old_config['id'], config)
            await self.middleware.call('kubernetes.status_change')

        return await self.config()

    @accepts()
    async def bindip_choices(self):
        """
        Returns ip choices for Kubernetes service to use.
        """
        return {
            d['address']: d['address'] for d in await self.middleware.call(
                'interface.ip_in_use', {'static': True, 'any': True}
            )
        }
