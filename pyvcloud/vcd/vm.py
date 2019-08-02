# VMware vCloud Director Python SDK
# Copyright (c) 2017-2018 VMware, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from copy import deepcopy

from lxml import etree
from lxml import objectify

from pyvcloud.vcd.client import E, E_OVF
from pyvcloud.vcd.client import EntityType
from pyvcloud.vcd.client import IpAddressMode
from pyvcloud.vcd.client import NSMAP
from pyvcloud.vcd.client import QueryResultFormat
from pyvcloud.vcd.client import RelationType
from pyvcloud.vcd.client import ResourceType
from pyvcloud.vcd.client import VCLOUD_STATUS_MAP
from pyvcloud.vcd.client import VmNicProperties
from pyvcloud.vcd.exceptions import EntityNotFoundException
from pyvcloud.vcd.exceptions import InvalidParameterException
from pyvcloud.vcd.exceptions import InvalidStateException
from pyvcloud.vcd.exceptions import MultipleRecordsException
from pyvcloud.vcd.exceptions import OperationNotSupportedException
from pyvcloud.vcd.utils import tag


class VM(object):
    """A helper class to work with Virtual Machines."""

    def __init__(self, client, href=None, resource=None):
        """Constructor for VM object.

        :param pyvcloud.vcd.client.Client client: the client that will be used
            to make REST calls to vCD.
        :param str href: href of the vm.
        :param lxml.objectify.ObjectifiedElement resource: object containing
            EntityType.VM XML data representing the vm.
        """
        self.client = client
        if href is None and resource is None:
            raise InvalidParameterException(
                'VM initialization failed as arguments are either invalid or'
                ' None')
        self.href = href
        self.resource = resource
        if resource is not None:
            self.href = resource.get('href')

    async def get_resource(self):
        """Fetches the XML representation of the vm from vCD.

        Will serve cached response if possible.

        :return: object containing EntityType.VM XML data representing the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        if self.resource is None:
            await self.reload()
        return self.resource

    async def reload(self):
        """Reloads the resource representation of the vm.

        This method should be called in between two method invocations on the
        VM object, if the former call changes the representation of the
        vm in vCD.
        """
        self.resource = await self.client.get_resource(self.href)
        if self.resource is not None:
            self.href = self.resource.get('href')

    async def get_vc(self):
        """Returns the vCenter where this vm is located.

        :return: name of the vCenter server.

        :rtype: str
        """
        await self.get_resource()
        env = self.resource.xpath('ovfenv:Environment', namespaces=NSMAP)
        if len(env) > 0:
            return env[0].get('{' + NSMAP['ve'] + '}vCenterId')
        return None

    async def get_cpus(self):
        """Returns the number of CPUs in the vm.

        :return: number of cpus (int) and number of cores per socket (int) of
            the vm.

        :rtype: dict
        """
        # await self.get_resource()
        # return {
        #     'num_cpus':
        #     int(self.resource.VmSpecSection.NumCpus.text),
        #     'num_cores_per_socket':
        #     int(self.resource.VmSpecSection.NumCoresPerSocket.text)
        # }

        uri = self.href + '/virtualHardwareSection/cpu'
        item = await self.client.get_resource(uri)
        return {
            'num_cpus': item[tag('rasd')('VirtualQuantity')],
            'num_cores_per_socket': item[tag('vmw')('CoresPerSocket')],
        }

    async def get_memory(self):
        """Returns the amount of memory in MB.

        :return: amount of memory in MB.

        :rtype: int
        """
        # await self.get_resource()
        # return int(
        #     self.resource.VmSpecSection.MemoryResourceMb.Configured.text)
        uri = self.href + '/virtualHardwareSection/memory'
        item = await self.client.get_resource(uri)
        return int(item[tag('rasd')('VirtualQuantity')])

    async def is_vmtools_installed(self):
        r = await self.get_resource()
        return hasattr(self.resource.RuntimeInfoSection, 'VMWareTools')

    async def modify_cpu(self, virtual_quantity, cores_per_socket=None):
        """Updates the number of CPUs of a vm.

        :param int virtual_quantity: number of virtual CPUs to configure on the
            vm.
        :param int cores_per_socket: number of cores per socket.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that updates the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        uri = self.href + '/virtualHardwareSection/cpu'
        if cores_per_socket is None:
            cores_per_socket = virtual_quantity
        item = await self.client.get_resource(uri)
        item['{' + NSMAP['rasd'] + '}ElementName'] = \
            '%s virtual CPU(s)' % virtual_quantity
        item['{' + NSMAP['rasd'] + '}VirtualQuantity'] = virtual_quantity
        item['{' + NSMAP['vmw'] + '}CoresPerSocket'] = cores_per_socket
        return await self.client.put_resource(uri, item, EntityType.RASD_ITEM.value)

    async def modify_memory(self, virtual_quantity):
        """Updates the memory of a vm.

        :param int virtual_quantity: number of MB of memory to configure on the
            vm.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that updates the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        uri = self.href + '/virtualHardwareSection/memory'
        item = await self.client.get_resource(uri)
        item['{' + NSMAP['rasd'] + '}ElementName'] = \
            '%s virtual CPU(s)' % virtual_quantity
        item['{' + NSMAP['rasd'] + '}VirtualQuantity'] = virtual_quantity
        return await self.client.put_resource(uri, item, EntityType.RASD_ITEM.value)

    async def get_power_state(self, vm_resource=None):
        """Returns the status of the vm.

        :param lxml.objectify.ObjectifiedElement vm_resource: object
            containing EntityType.VM XML data representing the vm whose
            power state we want to retrieve.

        :return: The status of the vm, the semantics of the value returned is
            captured in pyvcloud.vcd.client.VCLOUD_STATUS_MAP

        :rtype: int
        """
        if vm_resource is None:
            vm_resource = await self.get_resource()
        return int(vm_resource.get('status'))

    async def is_powered_on(self, vm_resource=None):
        """Checks if a vm is powered on or not.

        :param lxml.objectify.ObjectifiedElement vm_resource: object
            containing EntityType.VM XML data representing the vm whose
            power state we want to check.

        :return: True if the vm is powered on else False.

        :rtype: bool
        """
        return await self.get_power_state(vm_resource) == 4

    async def is_powered_off(self, vm_resource=None):
        """Checks if a vm is powered off or not.

        :param lxml.objectify.ObjectifiedElement vm_resource: object
            containing EntityType.VM XML data representing the vm whose
            power state we want to check.

        :return: True if the vm is powered off else False.

        :rtype: bool
        """
        return await self.get_power_state(vm_resource) == 8

    async def is_suspended(self, vm_resource=None):
        """Checks if a vm is suspended or not.

        :param lxml.objectify.ObjectifiedElement vm_resource: object
            containing EntityType.VM XML data representing the vm whose
            power state we want to check.

        :return: True if the vm is suspended else False.

        :rtype: bool
        """
        return await self.get_power_state(vm_resource) == 3

    async def is_deployed(self, vm_resource=None):
        """Checks if a vm is deployed or not.

        :param lxml.objectify.ObjectifiedElement vm_resource: object
            containing EntityType.VM XML data representing the vm whose
            power state we want to check.

        :return: True if the vm is deployed else False.

        :rtype: bool
        """
        return await self.get_power_state(vm_resource) == 2

    async def get_storage_profile_id(self, vm_resource=None):
        resource = vm_resource or await self.get_resource()
        return resource.StorageProfile.get('id')

    async def set_storage_profile(self, storage_profile):
        return await self.client.post_linked_resource(
            self.resource, ResourceType.ORG_VDC_STORAGE_PROFILE,
            EntityType.VMW_PVDC_STORAGE_PROFILE.value, storage_profile)

    async def _perform_power_operation(self,
                                 rel,
                                 operation_name,
                                 media_type=None,
                                 contents=None):
        """Perform a power operation on the vm.

        Perform one of the following power operations on the vm.
        Power on, Power off, Deploy, Undeploy, Shutdown, Reboot, Power reset.

        :param pyvcloud.vcd.client.RelationType rel: relation of the link in
            the vm resource that will be triggered for the power operation.
        :param str operation_name: name of the power operation to perform. This
            value will be used while logging error messages (if any).
        :param str media_type: media type of the link in
            the vm resource that will be triggered for the power operation.
        :param lxml.objectify.ObjectifiedElement contents: payload for the
            linked operation.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is tracking the power operation on the
            vm.

        :rtype: lxml.objectify.ObjectifiedElement

        :raises OperationNotSupportedException: if the power operation can't be
            performed on the vm.
        """
        vm_resource = await self.get_resource()
        try:
            return await self.client.post_linked_resource(vm_resource, rel,
                                                    media_type, contents)
        except OperationNotSupportedException:
            power_state = await self.get_power_state(vm_resource)
            raise OperationNotSupportedException(
                'Can\'t {0} vm. Current state of vm: {1}.'.format(
                    operation_name, VCLOUD_STATUS_MAP[power_state]))

    async def shutdown(self):
        """Shutdown the vm.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task shutting down the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        return await self._perform_power_operation(
            rel=RelationType.POWER_SHUTDOWN, operation_name='shutdown')

    async def reboot(self):
        """Reboots the vm.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task rebooting the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        return await self._perform_power_operation(
            rel=RelationType.POWER_REBOOT, operation_name='reboot')

    async def power_on(self):
        """Powers on the vm.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is powering on the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        return await self._perform_power_operation(
            rel=RelationType.POWER_ON, operation_name='power on')

    async def power_off(self):
        """Powers off the vm.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is powering off the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        return await self._perform_power_operation(
            rel=RelationType.POWER_OFF, operation_name='power off')

    async def power_reset(self):
        """Powers reset the vm.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is power resetting the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        return await self._perform_power_operation(
            rel=RelationType.POWER_RESET, operation_name='power reset')

    async def deploy(self, power_on=True, force_customization=False, deployment_lease_seconds=None):
        """Deploys the vm.

        Deploying the vm will allocate all resources assigned to the vm. If an
        attempt is made to deploy an already deployed vm, an exception will be
        raised.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is deploying the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        deploy_vm_params = E.DeployVAppParams()
        deploy_vm_params.set('powerOn', str(power_on).lower())
        deploy_vm_params.set('forceCustomization',
                             str(force_customization).lower())
        if deployment_lease_seconds is not None:
            deploy_vm_params.set('deploymentLeaseSeconds',
                                 str(deployment_lease_seconds).lower())
        return await self._perform_power_operation(
            rel=RelationType.DEPLOY,
            operation_name='deploy',
            media_type=EntityType.DEPLOY.value,
            contents=deploy_vm_params)

    async def undeploy(self, action='default'):
        """Undeploy the vm.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is undeploying the vm.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        params = E.UndeployVAppParams(E.UndeployPowerAction(action))
        return await self._perform_power_operation(
            rel=RelationType.UNDEPLOY,
            operation_name='undeploy',
            media_type=EntityType.UNDEPLOY.value,
            contents=params)

    async def get_hot_add_enabled(self):
        resource = await self.get_resource()
        VmCapabilities = resource.VmCapabilities
        return {
            'MemoryHotAddEnabled': json.loads(VmCapabilities.MemoryHotAddEnabled.text),
            'CpuHotAddEnabled': json.loads(VmCapabilities.CpuHotAddEnabled.text),
        }

    async def set_hot_add_enabled(self, *, memory, cpu):
        resource = await self.get_resource()
        VmCapabilities = resource.VmCapabilities
        VmCapabilities.MemoryHotAddEnabled = json.dumps(memory)
        VmCapabilities.CpuHotAddEnabled = json.dumps(cpu)
        objectify.deannotate(VmCapabilities)
        etree.cleanup_namespaces(VmCapabilities)
        return await self.client.put_resource(
            VmCapabilities.Link.get('href'), VmCapabilities, VmCapabilities.Link.get('type')
        )

    async def snapshot_create(self, memory=None, quiesce=None, name=None):
        """Create a snapshot of the vm.

        :param bool memory: True, if the snapshot should include the virtual
            machine's memory.
        :param bool quiesce: True, if the file system of the virtual machine
            should be quiesced before the snapshot is created. Requires VMware
            tools to be installed on the vm.
        :param str name: name of the snapshot.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is creating the snapshot.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        snapshot_vm_params = E.CreateSnapshotParams()
        if memory is not None:
            snapshot_vm_params.set('memory', str(memory).lower())
        if quiesce is not None:
            snapshot_vm_params.set('quiesce', str(quiesce).lower())
        if name is not None:
            snapshot_vm_params.set('name', str(name).lower())
        return await self.client.post_linked_resource(
            self.resource, RelationType.SNAPSHOT_CREATE,
            EntityType.SNAPSHOT_CREATE.value, snapshot_vm_params)

    async def snapshot_revert_to_current(self):
        """Reverts a virtual machine to the current snapshot, if any.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is reverting the snapshot.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        return await self.client.post_linked_resource(
            self.resource, RelationType.SNAPSHOT_REVERT_TO_CURRENT, None, None)

    async def snapshot_remove_all(self):
        """Removes all user created snapshots of a virtual machine.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task removing the snapshots.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        return await self.client.post_linked_resource(
            self.resource, RelationType.SNAPSHOT_REMOVE_ALL, None, None)

    async def add_nic(self, adapter_type, is_primary, is_connected, network_name,
                ip_address_mode, ip_address):
        """Adds a nic to the VM.

        :param str adapter_type: nic adapter type.
            One of NetworkAdapterType values.
        :param bool is_primary: True, if its a primary nic of the VM.
        :param bool is_connected: True, if the nic has to be connected.
        :param str network_name: name of the network to be connected to.
        :param str ip_address_mode: One of DHCP|POOL|MANUAL|NONE.
        :param str ip_address: to be set an ip in case of MANUAL mode.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task adding  a nic.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        # get network connection section.
        net_conn_section = (await self.get_resource()).NetworkConnectionSection
        idx_to_remove = []
        if hasattr(
            net_conn_section,
            tag('vcloud')('NetworkConnection')
        ):
            for idx, network_connection in enumerate(
                    getattr(
                        net_conn_section,
                        tag('vcloud')('NetworkConnection')
                    )
            ):
                if network_connection.get('network') == 'none':
                    idx_to_remove.append(idx)
            for idx in reversed(idx_to_remove):
                del net_conn_section.NetworkConnection[idx]
        nic_index = 0
        insert_index = net_conn_section.index(
            net_conn_section[tag('ovf')('Info')]) + 1
        # check if any nics exists
        if hasattr(net_conn_section, 'PrimaryNetworkConnectionIndex'):
            # calculate nic index and create the networkconnection object.
            indices = [None] * 10
            insert_index = net_conn_section.index(
                net_conn_section.PrimaryNetworkConnectionIndex) + 1
            if hasattr(net_conn_section, 'NetworkConnection'):
                for nc in net_conn_section.NetworkConnection:
                    indices[int(nc.NetworkConnectionIndex.
                                text)] = nc.NetworkConnectionIndex.text
            nic_index = indices.index(None)
            if is_primary:
                net_conn_section.PrimaryNetworkConnectionIndex = \
                    E.PrimaryNetworkConnectionIndex(nic_index)
        else:
            setattr(
                net_conn_section,
                tag('ovf')('PrimaryNetworkConnectionIndex'),
                E.PrimaryNetworkConnectionIndex(nic_index)
            )

        net_conn = E.NetworkConnection(network=network_name)
        net_conn.append(E.NetworkConnectionIndex(nic_index))
        if ip_address_mode == IpAddressMode.MANUAL.value:
            net_conn.append(E.IpAddress(ip_address))
        net_conn.append(E.IsConnected(is_connected))
        net_conn.append(E.IpAddressAllocationMode(ip_address_mode))
        net_conn.append(E.NetworkAdapterType(adapter_type))
        net_conn_section.insert(insert_index, net_conn)
        await self.client.put_linked_resource(
            net_conn_section, RelationType.EDIT,
            EntityType.NETWORK_CONNECTION_SECTION.value, net_conn_section)
        return nic_index

    async def set_primary_nic(self, index):
        # get network connection section.
        net_conn_section = (await self.get_resource()).NetworkConnectionSection
        # check if any nics exists
        if hasattr(net_conn_section, 'PrimaryNetworkConnectionIndex'):
            net_conn_section.PrimaryNetworkConnectionIndex = \
                    E.PrimaryNetworkConnectionIndex(index)

        return await self.client.put_linked_resource(
            net_conn_section, RelationType.EDIT,
            EntityType.NETWORK_CONNECTION_SECTION.value, net_conn_section)

    async def list_nics(self):
        """Lists all the nics of the VM.

        :return: list of nics with the following properties as a dictionary.
            nic index, is primary, is connected, connected network,
            ip address allocation mode, ip address, network adapter type

        :rtype: list
        """
        nics = []
        await self.get_resource()
        if hasattr(self.resource.NetworkConnectionSection,
                   'PrimaryNetworkConnectionIndex'):
            primary_index = self.resource.NetworkConnectionSection.\
                PrimaryNetworkConnectionIndex.text
        else:
            primary_index = None

        net_conn_section = self.resource.NetworkConnectionSection
        if hasattr(net_conn_section, 'NetworkConnection'):
            for nc in net_conn_section.NetworkConnection:
                try:
                    nic = {}
                    nic[VmNicProperties.INDEX.value] = nc.NetworkConnectionIndex.text
                    nic[VmNicProperties.CONNECTED.value] = nc.IsConnected.text
                    nic[VmNicProperties.PRIMARY.value] = (
                        primary_index == nc.NetworkConnectionIndex.text)
                    nic[VmNicProperties.ADAPTER_TYPE.
                        value] = nc.NetworkAdapterType.text
                    nic[VmNicProperties.NETWORK.value] = nc.get(
                        VmNicProperties.NETWORK.value)
                    nic[VmNicProperties.IP_ADDRESS_MODE.
                        value] = nc.IpAddressAllocationMode.text
                    nic[VmNicProperties.MAC_ADDRESS.
                        value] = nc.MACAddress.text
                    if hasattr(nc, 'IpAddress'):
                        nic[VmNicProperties.IP_ADDRESS.value] = nc.IpAddress.text
                    nics.append(nic)
                except AttributeError:
                    pass
        return nics

    async def delete_nic(self, index):
        """Deletes a nic from the VM.

        :param int index: index of the nic to be deleted.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task adding  a nic.

        :rtype: lxml.objectify.ObjectifiedElement

        :raises: InvalidParameterException: if a nic with the given index is
            not found in the VM.
        """
        # get network connection section.
        net_conn_section = (await self.get_resource()).NetworkConnectionSection

        indices = [None] * 10
        nic_not_found = True
        # find the nic with the given index
        for nc in net_conn_section.NetworkConnection:
            if int(nc.NetworkConnectionIndex.text) == index:
                net_conn_section.remove(nc)
                nic_not_found = False
            else:
                indices[int(nc.NetworkConnectionIndex.
                            text)] = nc.NetworkConnectionIndex.text

        if nic_not_found:
            raise InvalidParameterException(
                'Nic with index \'%s\' is not found in the VM \'%s\'' %
                (index, (await self.get_resource()).get('name')))

        # now indices will have all existing nic indices
        prim_nic = next((i for i in indices if i is not None), None)
        if prim_nic:
            net_conn_section.PrimaryNetworkConnectionIndex = \
                E.PrimaryNetworkConnectionIndex(prim_nic)
        return await self.client.put_linked_resource(
            net_conn_section, RelationType.EDIT,
            EntityType.NETWORK_CONNECTION_SECTION.value, net_conn_section)

    async def get_product_section(self, args=None):
        if args:
            keys = set(args)
        else:
            keys = None

        uri = self.href + '/productSections'
        product_section_list = await self.client.get_resource(uri)
        product_section = getattr(
            product_section_list,
            tag('ovf')('ProductSection'),
            None
        )
        result = dict()
        if product_section is not None:
            for property in getattr(
                product_section,
                tag('ovf')('Property'),
                []
            ):
                key = property.get(tag('ovf')('key'))
                if keys is None or key in keys:
                    try:
                        result[key] = getattr(
                            property,
                            tag('ovf')('Value')
                        ).get(tag('ovf')('value'))
                    except AttributeError:
                        result[key] = None
        return result

    async def del_product_section(self, keys):
        uri = self.href + '/productSections'
        product_section_list = await self.client.get_resource(uri)
        product_section = getattr(
            product_section_list,
            tag('ovf')('ProductSection'),
            None
        )
        if product_section is not None:
            for i, property in enumerate(getattr(
                    product_section,
                    tag('ovf')('Property'),
                    []
            )):
                if property.get(tag('ovf')('key')) in keys:
                    product_section.remove(property)

            setattr(
                product_section_list,
                tag('ovf')('ProductSection'),
                product_section
            )

            objectify.deannotate(product_section_list)
            etree.cleanup_namespaces(product_section_list)

            await self.client.put_resource(
                uri,
                product_section_list,
                EntityType.PRODUCT_SECTION_LIST.value
            )

    async def add_product_section(self, **kwargs):
        uri = self.href + '/productSections'
        product_section_list = await self.client.get_resource(uri)
        product_section = getattr(
            product_section_list,
            tag('ovf')('ProductSection'),
            None
        )
        if product_section is None:
            product_section = E_OVF.ProductSection()
            product_section.append(
                E_OVF.Info()
            )
        for key, value in kwargs.items():
            product_section.append(
                E_OVF.Property(
                    **{
                        tag('ovf')('key'): key,
                        tag('ovf')('type'): 'string',
                        tag('ovf')('userConfigurable'): 'true',
                    }
                )
            )
            try:
                getattr(
                    product_section,
                    tag('ovf')('Property')
                )[-1].Label = key
            except AttributeError:
                getattr(
                    product_section,
                    tag('ovf')('Property')
                )[-1].append(E_OVF.Label())
                getattr(
                    product_section,
                    tag('ovf')('Property')
                )[-1].Label = key
            getattr(
                product_section,
                tag('ovf')('Property')
            )[-1].append(
                E_OVF.Value(
                    **{
                        '{' + NSMAP['ovf'] + '}value': value,
                    }
                )
            )

        setattr(
            product_section_list,
            tag('ovf')('ProductSection'),
            product_section
        )

        objectify.deannotate(product_section_list)
        etree.cleanup_namespaces(product_section_list)

        await self.client.put_resource(
            uri,
            product_section_list,
            EntityType.PRODUCT_SECTION_LIST.value
        )

    async def modify_product_section(self, **kwargs):
        uri = self.href + '/productSections'
        product_section_list = await self.client.get_resource(uri)
        product_section = getattr(
            product_section_list,
            tag('ovf')('ProductSection'),
            None
        )
        if product_section is None:
            product_section = E_OVF.ProductSection()
            product_section.append(
                E_OVF.Info()
            )

        if hasattr(
            product_section,
            tag('ovf')('Property')
        ):
            idx_list = []
            for idx, prop in enumerate(getattr(
                product_section,
                tag('ovf')('Property')
            )):
                if prop.get(tag('ovf')('key')) in kwargs.keys():
                    idx_list.append(idx)
            for idx in reversed(idx_list):
                del getattr(
                    product_section,
                    tag('ovf')('Property')
                )[idx]

        for key, value in kwargs.items():
            product_section.append(
                E_OVF.Property(
                    **{
                        tag('ovf')('key'): key,
                        tag('ovf')('type'): 'string',
                        tag('ovf')('userConfigurable'): 'true',
                    }
                )
            )
            try:
                getattr(
                    product_section,
                    tag('ovf')('Property')
                )[-1].Label = key
            except AttributeError:
                getattr(
                    product_section,
                    tag('ovf')('Property')
                )[-1].append(E_OVF.Label())
                getattr(
                    product_section,
                    tag('ovf')('Property')
                )[-1].Label = key
            getattr(
                product_section,
                tag('ovf')('Property')
            )[-1].append(
                E_OVF.Value(
                    **{
                        '{' + NSMAP['ovf'] + '}value': value,
                    }
                )
            )

        setattr(
            product_section_list,
            tag('ovf')('ProductSection'),
            product_section
        )

        objectify.deannotate(product_section_list)
        etree.cleanup_namespaces(product_section_list)

        await self.client.put_resource(
            uri,
            product_section_list,
            EntityType.PRODUCT_SECTION_LIST.value
        )

    async def suspend(self):
        """Suspend the vm.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is suspending the VM.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        return await self._perform_power_operation(
            rel=RelationType.POWER_SUSPEND, operation_name='suspend')

    async def add_disk(self, name, size, storage_policy_id, bus_type, bus_sub_type):
        """Add a virtual disk to a virtual machine

        It assumes that the vm has already at least one virtual hard disk
        and will attempt to create another one with similar characteristics.

        :param str name: name of the disk to be added.
        :param int size: size of the disk to be added, in MBs.
        :param str storage_policy_id: storage_policy_id of the disk to be added.
        :param str bus_type: bus_type of the disk to be added.
            Bus type. One of:
                5 (IDE)
                6 (SCSI)
                20 (SATA)
        :param str bus_sub_type: bus_sub_type of the disk to be added.
            Hard disk controller type. One of:
                buslogic
                lsilogic
                lsilogicsas
                VirtualSCSI
                vmware.sata.ahci
        :return: disk_id

        :rtype: lxml.objectify.ObjectifiedElement
        """

        storage_policy_href = f'{self.client._uri}/api/vdcStorageProfile/' + storage_policy_id.split(':')[-1]

        resource = await self.get_resource()
        disk_list = await self.client.get_resource(
            resource.get('href') + '/virtualHardwareSection/disks')
        last_disk = None
        for disk in disk_list.Item:
            if disk[tag('rasd')('Description')] == 'Hard disk':
                last_disk = disk
        assert last_disk is not None
        new_disk = deepcopy(last_disk)
        addr = int(str(
            last_disk['{' + NSMAP['rasd'] + '}AddressOnParent'])) + 1
        instance_id = int(str(
            last_disk[tag('rasd')('InstanceID')])) + 1
        new_disk[tag('rasd')('AddressOnParent')] = addr
        new_disk[tag('rasd')('ElementName')] = 'Hard disk %s' % addr
        new_disk[tag('rasd')('InstanceID')] = instance_id
        new_disk[tag('rasd')('VirtualQuantity')] = \
            size * 1024 * 1024
        new_disk[tag('rasd')('HostResource')].set(
            tag('vcloud')('capacity'), str(size))
        new_disk[tag('rasd')('ElementName')] = name
        if storage_policy_href != new_disk[tag('rasd')('HostResource')].get(
            tag('vcloud')('storageProfileHref')
        ):
            new_disk[tag('rasd')('HostResource')].set(
                tag('vcloud')('storageProfileOverrideVmDefault'), 'true')
            new_disk[tag('rasd')('HostResource')].set(
                tag('vcloud')('storageProfileHref'), storage_policy_href)
        new_disk[tag('rasd')('HostResource')].set(
            tag('vcloud')('busType'), bus_type)
        new_disk[tag('rasd')('HostResource')].set(
            tag('vcloud')('busSubType'), bus_sub_type)

        disk_list.append(new_disk)

        await self.client.put_resource(
            resource.get('href') + '/virtualHardwareSection/disks', disk_list,
            EntityType.RASD_ITEMS_LIST.value)
        return instance_id

    async def modify_disk(self, disk_id, size=None,
                          storage_policy_id=None, parent=None,
                          address_on_parent=None, bus_type=None,
                          bus_sub_type=None):
        """
        Change size of disk
        :param disk_id - new disk ID, like 2000
        :param size - new size of disk in MB
        :param storage_policy_id
        :param parent
        :param address_on_parent
        :param int bus_type: bus_type of the disk to be added.
            Bus type. One of:
                5 (IDE)
                6 (SCSI)
                20 (SATA)
        :param int bus_sub_type: bus_sub_type of the disk to be added.
            Hard disk controller type. One of:
                buslogic
                lsilogic
                lsilogicsas
                VirtualSCSI
                vmware.sata.ahci
        """
        if storage_policy_id is not None:
            storage_policy_href = f'{self.client._uri}/api/vdcStorageProfile/' + storage_policy_id.split(':')[-1]
        else:
            storage_policy_href = None

        assert size is not None or storage_policy_href is not None or \
            parent is not None or address_on_parent is not None or bus_sub_type is not None
        disk_list = await self.client.get_resource(
            (await self.get_resource()).get('href') + '/virtualHardwareSection/disks')
        for idx, disk in enumerate(disk_list.Item):
            # if disk[tag('rasd')('Description')] == 'Hard disk' and int(disk[tag('rasd')('InstanceID')]) == disk_id:
            if int(disk[tag('rasd')('InstanceID')].text) == disk_id:
                disk_idx = idx
                disk_resource = disk
                break
        else:
            raise EntityNotFoundException("Disk", disk_id)
        if size is not None:
            disk_resource[tag('rasd')('VirtualQuantity')] = str(size * 1024 * 1024)
            disk_resource[tag('rasd')('HostResource')].set(
                tag('vcloud')('capacity'), str(size)
            )
        if storage_policy_href is not None:
            disk_resource[tag('rasd')('HostResource')].set(
                tag('vcloud')('storageProfileHref'), storage_policy_href
            )
            disk_resource[tag('rasd')('HostResource')].set(
                tag('vcloud')('storageProfileOverrideVmDefault'),
                'true'
            )
        if parent is not None:
            disk_resource[tag('rasd')('Parent')] = parent
        if address_on_parent is not None:
            disk_resource[tag('rasd')('AddressOnParent')] = address_on_parent
        if bus_type is not None:
            disk_resource[tag('rasd')('HostResource')].set(
                tag('vcloud')('busType'), bus_type)
        if bus_sub_type is not None:
            disk_resource[tag('rasd')('HostResource')].set(
                tag('vcloud')('BusSubType', str(bus_sub_type))
            )

        del disk_list.Item[disk_idx]
        disk_list.append(disk_resource)

        return await self.client.put_resource(
            (await self.get_resource()).get('href') + '/virtualHardwareSection/disks', disk_list,
            EntityType.RASD_ITEMS_LIST.value)

    async def get_mks_ticket(self):
        uri = (await self.get_resource()).get('href') + '/screen/action/acquireMksTicket'
        resource = await self.client.post_resource(uri, None, EntityType.MKS_TICKET.value)
        return {
               'host': str(resource['Host']),
               'vmx': str(resource['Vmx']),
               'ticket': str(resource['Ticket']),
               'port': str(resource['Port']),
        }

    async def get_ticket(self):
        uri = (await self.get_resource()).get('href') + '/screen/action/acquireTicket'
        resource = await self.client.post_resource(uri, None, EntityType.TICKET.value)
        return {
               'screen_ticket': str(resource.text),
        }

    async def delete_disk(self, disk_id):
        disk_list = await self.client.get_resource(
            (await self.get_resource()).get('href') + '/virtualHardwareSection/disks')
        for idx, disk_resource in enumerate(disk_list.Item):
            if int(disk_resource[tag('rasd')('InstanceID')].text) == disk_id:
                del disk_list.Item[idx]
                break
        else:
            raise EntityNotFoundException(disk_id)
        return await self.client.put_resource(
            (await self.get_resource()).get('href') + '/virtualHardwareSection/disks', disk_list,
            EntityType.RASD_ITEMS_LIST.value)

    async def get_disks(self):
        disk_list = await self.client.get_resource(
            (await self.get_resource()).get('href') + '/virtualHardwareSection/disks')
        return [item for item in disk_list.Item if item[tag('rasd')('ResourceType')].text == '17']

    async def get_medias(self):
        disk_list = await self.client.get_resource(
            (await self.get_resource()).get('href') + '/virtualHardwareSection/media')
        return [item for item in disk_list.Item if item.find(tag('rasd')('HostResource'))]

    async def get_disk(self, disk_id):
        disk_list = await self.client.get_resource(
            (await self.get_resource()).get('href') + '/virtualHardwareSection/disks')
        for disk_resource in disk_list.Item:
            if int(getattr(
                disk_resource,
                tag('rasd')('InstanceID')
            ).text) == disk_id:
                return disk_resource
        raise EntityNotFoundException(disk_id)


    async def detach_disk(self, disk_href):
        """Detach the independent disk from the vm with the given name.

        :param str disk_href: href of the disk to be detached.
        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task of dettaching the disk.

        :rtype: lxml.objectify.ObjectifiedElement

        :raises: EntityNotFoundException: if the named vm or disk cannot be
            located.
        """
        disk_attach_or_detach_params = E.DiskAttachOrDetachParams(
            E.Disk(type=EntityType.DISK.value, href=disk_href))
        vm_resource = await self.get_resource()
        return await self.client.post_linked_resource(
            vm_resource, RelationType.DISK_DETACH,
            EntityType.DISK_ATTACH_DETACH_PARAMS.value,
            disk_attach_or_detach_params)

    async def discard_suspended_state(self):
        """Discard suspended state of the vm.

        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is discarding suspended state
                    of VM.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        return await self.client.post_linked_resource(
            self.resource, RelationType.DISCARD_SUSPENDED_STATE, None, None)

    async def install_vmware_tools(self):
        """Install vmware tools in the vm.

        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is installing vmware tools in VM

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        return await self.client.post_linked_resource(
            self.resource, RelationType.INSTALL_VMWARE_TOOLS, None, None)

    async def insert_cd_from_catalog(self, media_href):
        """Insert CD from catalog to the vm.

        :param: media href to insert to VM

        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is inserting CD to VM.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        vm_resource = await self.get_resource()
        media_insert_params = E.MediaInsertOrEjectParams(
            E.Media(href=media_href))
        return await self.client.post_linked_resource(
            vm_resource, RelationType.INSERT_MEDIA,
            EntityType.MEDIA_INSERT_OR_EJECT_PARAMS.value, media_insert_params)

    async def eject_cd(self, media_href):
        """Insert CD from catalog to the vm.

        :param: media href to eject from VM

        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is inserting CD to VM.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        vm_resource = await self.get_resource()
        media_eject_params = E.MediaInsertOrEjectParams(
            E.Media(href=media_href))
        return await self.client.post_linked_resource(
            vm_resource, RelationType.EJECT_MEDIA,
            EntityType.MEDIA_INSERT_OR_EJECT_PARAMS.value, media_eject_params)

    async def upgrade_virtual_hardware(self):
        """Upgrade virtual hardware of vm.

        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is installing vmware tools in VM

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        return await self.client.post_linked_resource(
            self.resource, RelationType.UPGRADE, None, None)

    async def consolidate(self):
        """Consolidate VM.

        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is consolidating VM

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        return await self.client.post_linked_resource(
            self.resource, RelationType.CONSOLIDATE, None, None)

    async def copy_to(self, source_vapp_name, target_vapp_name, target_vm_name):
        """Copy VM from one vApp to another.
        :param: str source vApp name
        :param: str target vApp name
        :param: str target VM name
        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is copying VM
        :rtype: lxml.objectify.ObjectifiedElement
        """
        return await self._clone(source_vapp_name=source_vapp_name,
                           target_vapp_name=target_vapp_name,
                           target_vm_name=target_vm_name,
                           source_delete=False)

    async def move_to(self, source_vapp_name, target_vapp_name, target_vm_name):
        """Move VM from one vApp to another.
        :param: str source vApp name
        :param: str target vApp name
        :param: str target VM name
        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is moving VM
        :rtype: lxml.objectify.ObjectifiedElement
        """
        return await self._clone(source_vapp_name=source_vapp_name,
                           target_vapp_name=target_vapp_name,
                           target_vm_name=target_vm_name,
                           source_delete=True)

    async def _clone(self, source_vapp_name, target_vapp_name, target_vm_name,
               source_delete):
        """Clone VM from one vApp to another.
        :param: str source vApp name
        :param: str target vApp name
        :param: str target VM name
        :param: bool source delete option
        :return: an object containing EntityType.TASK XML data which represents
                 the asynchronous task that is copying VM
        :rtype: lxml.objectify.ObjectifiedElement
        """
        from pyvcloud.vcd.vapp import VApp
        vm_resource = await self.get_resource()
        resource_type = ResourceType.VAPP.value
        if (await self.is_powered_off(vm_resource)) or source_delete:
            records1 = await self.___validate_vapp_records(
                vapp_name=source_vapp_name, resource_type=resource_type)

            source_vapp_href = records1[0].get('href')

            records2 = await self.___validate_vapp_records(
                vapp_name=target_vapp_name, resource_type=resource_type)

            target_vapp_href = records2[0].get('href')

            source_vapp = VApp(self.client, href=source_vapp_href)
            target_vapp = VApp(self.client, href=target_vapp_href)
            await target_vapp.reload()
            spec = {
                'vapp': (await source_vapp.get_resource()),
                'source_vm_name': (await source_vapp.get_vm()).get('name'),
                'target_vm_name': target_vm_name
            }
            return await target_vapp.add_vms([spec],
                                       deploy=False,
                                       power_on=False,
                                       all_eulas_accepted=True,
                                       source_delete=source_delete
                                       )
        else:
            raise InvalidStateException("VM Must be powered off.")

    # async def copy_to(self, source_vapp_name, target_vapp_name, target_vm_name, deploy=False,
    #                   power_on=False, all_eulas_accepted=False):
    #     """Copy VM from one vApp to another.
    #
    #     :param: str source vApp name
    #     :param: str target vApp name
    #     :param: str target VM name
    #
    #     :return: an object containing EntityType.TASK XML data which represents
    #                 the asynchronous task that is copying VM
    #
    #     :rtype: lxml.objectify.ObjectifiedElement
    #     """
    #     return await self._clone(source_vapp_name=source_vapp_name,
    #                              target_vapp_name=target_vapp_name,
    #                              target_vm_name=target_vm_name,
    #                              deploy=deploy,
    #                              power_on=power_on,
    #                              all_eulas_accepted=all_eulas_accepted,
    #                              source_delete=False)
    #
    # async def move_to(self, source_vapp_name, target_vapp_name, target_vm_name):
    #     """Move VM from one vApp to another.
    #
    #     :param: str source vApp name
    #     :param: str target vApp name
    #     :param: str target VM name
    #
    #     :return: an object containing EntityType.TASK XML data which represents
    #                 the asynchronous task that is moving VM
    #
    #     :rtype: lxml.objectify.ObjectifiedElement
    #     """
    #     return await self._clone(source_vapp_name=source_vapp_name,
    #                        target_vapp_name=target_vapp_name,
    #                        target_vm_name=target_vm_name,
    #                        source_delete=True)
    #
    # async def _clone(self, source_vapp_name, target_vapp_name, target_vm_name,
    #                  deploy=False, power_on=False, all_eulas_accepted=False,
    #                  source_delete=False):
    #     """Clone VM from one vApp to another.
    #
    #     :param: str source vApp name
    #     :param: str target vApp name
    #     :param: str target VM name
    #     :param: bool source delete option
    #
    #     :return: an object containing EntityType.TASK XML data which represents
    #              the asynchronous task that is copying VM
    #
    #     :rtype: lxml.objectify.ObjectifiedElement
    #     """
    #     from pyvcloud.vcd.vapp import VApp
    #     vm_resource = await self.get_resource()
    #     resource_type = ResourceType.VAPP.value
    #     if (await self.is_powered_off(vm_resource)) or source_delete:
    #         records1 = await self.___validate_vapp_records(
    #             vapp_name=source_vapp_name, resource_type=resource_type)
    #
    #         source_vapp_href = records1[0].get('href')
    #
    #         records2 = await self.___validate_vapp_records(
    #             vapp_name=target_vapp_name, resource_type=resource_type)
    #
    #         target_vapp_href = records2[0].get('href')
    #
    #         source_vapp = VApp(self.client, href=source_vapp_href)
    #         target_vapp = VApp(self.client, href=target_vapp_href)
    #         await target_vapp.reload()
    #         spec = {
    #             'vapp': (await source_vapp.get_resource()),
    #             'source_vm_name': (await self.get_resource()).get('name'),
    #             'target_vm_name': target_vm_name
    #         }
    #         return await target_vapp.add_vms([spec],
    #                                    deploy=deploy,
    #                                    power_on=power_on,
    #                                    all_eulas_accepted=all_eulas_accepted,
    #                                    source_delete=source_delete)
    #     else:
    #         raise InvalidStateException("VM Must be powered off.")

    async def change_name(self, name):
        """Edit name and description of the vApp.

        :param str name: New name of the vApp. It is mandatory.

        :return: object containing EntityType.TASK XML data
            representing the asynchronous task.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        if name is None or name.isspace():
            raise InvalidParameterException("Name can't be None or empty")
        vm = await self.get_resource()
        vm.set('name', name.strip())

        return await self.client.put_resource(
            self.href, vm, EntityType.VM.value)

    async def set_guest_customization_section(self, **kwargs):
        await self.reload()
        uri = self.href + '/guestCustomizationSection'
        item = await self.client.get_resource(uri)
        last_tag = item.find(tag('ovf')('Info'))

        for xml_tag in item.getchildren():
            if (
                    xml_tag.tag != tag('vcloud')('Link')
                    and xml_tag.tag != tag('ovf')('Info')
            ):
                item.remove(xml_tag)
                if xml_tag.tag not in {
                    tag('vcloud')(key)
                    for key in kwargs.keys()
                }:
                    kwargs[xml_tag.tag.split('}')[-1]] = xml_tag.text
        for tag_key in [
            'Enabled',
            'ChangeSid',
            'VirtualMachineId',
            'JoinDomainEnabled',
            'UseOrgSettings',
            'DomainName',
            'DomainUserName',
            'DomainUserPassword',
            'MachineObjectOU',
            'AdminPasswordEnabled',
            'AdminPasswordAuto',
            'AdminPassword',
            'ResetPasswordRequired',
            'CustomizationScript',
            'ComputerName',
        ]:
            if kwargs.get(tag_key) is not None:
                k = tag_key
                v = kwargs.get(tag_key)
                if v is not None:
                    element = getattr(E, tag('vcloud')(k))(v)
                    last_tag.addnext(element)
                    last_tag = element

        objectify.deannotate(item)
        etree.cleanup_namespaces(item)
        await self.client.put_resource(
            uri,
            item,
            EntityType.GUEST_CUSTOMIZATION_SECTION.value
        )


    async def ___validate_vapp_records(self, vapp_name, resource_type):
        name_filter = ('name', vapp_name)
        q1 = self.client.get_typed_query(
            resource_type,
            query_result_format=QueryResultFormat.REFERENCES,
            equality_filter=name_filter)
        records = list(await q1.execute())
        if records is None or len(records) == 0:
            raise EntityNotFoundException(
                'Vapp with name \'%s\' not found.' % vapp_name)
        elif len(records) > 1:
            raise MultipleRecordsException("Found multiple vapp named "
                                           "'%s'," % vapp_name)

        return records

    async def delete(self):
        """Delete the VM.

        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is deleting the VM.
        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        return await self.client.delete_linked_resource(self.resource,
                                                        RelationType.REMOVE, None)

    def general_setting_detail(self):
        """Get the details of VM general setting.

        :return: Dictionary having VM general setting details.
        e.g.
        {'Name': 'testvm', 'Computer Name': 'testvm1', 'Storage Policy' : '*',
        'OS': 'Microsoft Windows Server 2016 (64-bit)', 'Boot Delay': 0,
        'OS Family': 'windows9Server64Guest', 'Enter BIOS Setup': False}
        :rtype: dict
        """
        general_setting = {}
        self.get_resource()
        general_setting['Name'] = self.resource.get('name')
        general_setting['Computer Name'] = \
            self.resource.GuestCustomizationSection.ComputerName
        if hasattr(self.resource, 'Description'):
            general_setting['Description'] = self.resource.Description
        general_setting['OS Family'] = self.resource[
            '{' + NSMAP['ovf'] +
            '}OperatingSystemSection'].get('{' + NSMAP['vmw'] + '}osType')
        general_setting['OS'] = self.resource[
            '{' + NSMAP['ovf'] + '}OperatingSystemSection'].Description
        general_setting['Boot Delay'] = self.resource.BootOptions.BootDelay
        general_setting[
            'Enter BIOS Setup'] = self.resource.BootOptions.EnterBIOSSetup
        general_setting['Storage Policy'] = self.resource.StorageProfile.get(
            'name')
        return general_setting

    def list_storage_profile(self):
        """Get the list of storage profile in VM.

        :return: list of stotage profile name of storage profile.
        e.g.
        [{'name': 'diskpolicy1'}, {'name': 'diskpolicy2'}]
        :rtype: list
        """
        storage_profile = []
        self.get_resource()
        if hasattr(self.resource.VmSpecSection, 'DiskSection'):
            if hasattr(self.resource.VmSpecSection.DiskSection,
                       'DiskSettings'):
                for disk_setting in \
                        self.resource.VmSpecSection.DiskSection.DiskSettings:
                    if hasattr(disk_setting, 'StorageProfile'):
                        storage_profile.append({
                            'name':
                            disk_setting.StorageProfile.get('name')
                        })
        return storage_profile

    def reload_from_vc(self):
        """Reload a VM from VC.

        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is reloading VM from VC

        :rtype: lxml.objectify.ObjectifiedElement
        """
        self.get_resource()
        return self.client.post_linked_resource(
            self.resource, RelationType.RELOAD_FROM_VC, None, None)

    def check_compliance(self):
        """Check compliance of a VM.

        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is checking compliance of VM

        :rtype: lxml.objectify.ObjectifiedElement
        """
        self.get_resource()
        return self.client.post_linked_resource(
            self.resource, RelationType.CHECK_COMPLIANCE, None, None)

    async def customize_at_next_power_on(self):
        """Customize VM at next power on.

        :return: returns 204 No content

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        return await self.client.post_linked_resource(
            self.resource, RelationType.CUSTOMIZE_AT_NEXT_POWERON, None, None)

    async def update_general_setting(self,
                               name=None,
                               description=None,
                               computer_name=None,
                               boot_delay=None,
                               enter_bios_setup=None,
                               storage_policy_href=None):
        """Update general settings of VM .

        :param str name: name of VM.
        :param str description: description of VM.
        :param str computer_name: computer name of VM.
        :param int boot_delay: boot delay of VM.
        :param bool enter_bios_setup: enter bios setup of VM.
        :param str storage_policy_href: storage policy href.
        :return: an object containing EntityType.TASK XML data which represents
            the asynchronous task that is updating general setting of VM.
        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        if name is not None:
            self.resource.set('name', name)
        guest_customization = self.resource.GuestCustomizationSection
        if computer_name is not None:
            guest_customization.remove(guest_customization.ComputerName)
            cn = E.ComputerName(computer_name)
            guest_customization.ResetPasswordRequired.addnext(cn)
        if description is not None:
            if hasattr(self.resource, 'Description'):
                self.resource.remove(self.resource.Description)
            desc = E.Description(description)
            self.resource.VmSpecSection.addprevious(desc)
        if boot_delay is not None:
            if hasattr(self.resource.BootOptions, 'BootDelay'):
                self.resource.BootOptions.remove(
                    self.resource.BootOptions.BootDelay)
            bd = E.BootDelay(boot_delay)
            self.resource.BootOptions.EnterBIOSSetup.addprevious(bd)
        if enter_bios_setup is not None:
            if hasattr(self.resource.BootOptions, 'EnterBIOSSetup'):
                self.resource.BootOptions.remove(
                    self.resource.BootOptions.EnterBIOSSetup)
            ebs = E.EnterBIOSSetup(enter_bios_setup)
            self.resource.BootOptions.BootDelay.addnext(ebs)
        if storage_policy_href is not None:
            storage_policy_res = await self.client.get_resource(storage_policy_href)
            self.resource.StorageProfile.set('href', storage_policy_href)
            self.resource.StorageProfile.set('id',
                                             storage_policy_res.get('id'))
            self.resource.StorageProfile.set('name',
                                             storage_policy_res.get('name'))
        return await self.client.post_linked_resource(
            self.resource, RelationType.RECONFIGURE_VM, EntityType.VM.value,
            self.resource)

    def power_on_and_force_recustomization(self):
        """Recustomize VM at power on.

        :return: an object containing EntityType.TASK XML data which represents
                    the asynchronous task that is force recustomize VM on
                    power on operation

        :rtype: lxml.objectify.ObjectifiedElement
        """
        return self.deploy(power_on=True, force_customization=True)

    async def get_guest_customization_status(self):
        """Get guest customization status.

        :return: returns status of GC.

        :rtype: String
        """
        await self.get_resource()
        uri = self.href + '/guestcustomizationstatus/'
        gc_status_resource = await self.client.get_resource(uri)
        return gc_status_resource.GuestCustStatus

    async def get_guest_customization_section(self):
        """Get guest customization section.

        :return: returns lxml.objectify.ObjectifiedElement resource: object
            containing EntityType.GUESTCUSTOMIZATIONSECTION XML data
            representing the guestcustomizationsection.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        uri = self.href + '/guestCustomizationSection/'
        return await self.client.get_resource(uri)

    async def enable_guest_customization(self, is_enabled=False):
        """Enable guest customization.

        :param: bool is_enabled: if True, it will enable guest customization.
            If False, it will disable guest customization

        :return: returns lxml.objectify.ObjectifiedElement resource: object
            containing EntityType.GUESTCUSTOMIZATIONSECTION XML data
            representing the guestcustomizationsection.

        :rtype: lxml.objectify.ObjectifiedElement
        """
        await self.get_resource()
        gc_section = await self.get_guest_customization_section()
        if hasattr(gc_section, 'Enabled'):
            gc_section.Enabled = E.Enabled(is_enabled)
        uri = self.href + '/guestCustomizationSection/'
        return await self.client.\
            put_resource(uri, gc_section,
                         EntityType.GUEST_CUSTOMIZATION_SECTION.value)

    def list_virtual_hardware_section(self, is_cpu=True, is_memory=True,
                                      is_disk=False, is_media=False,
                                      is_networkCards=False):
        """Get virtual hardware section.

        :param: bool is_cpu: if True, it will provide CPU information
        :param: bool is_memory: if True, it will provide memory information
        :param: bool is_disk: if True, it will provide disk information
        :param: bool is_media: if True, it will provide media information
        :param: bool is_networkCards: if True, it will provide network card
                                      information

        :return: dict having virtual hardware section details.

        :rtype: dict
        """
        vhs_info = {}
        self.get_resource()
        if is_cpu:
            uri = self.href + '/virtualHardwareSection/cpu'
            cpu_resource = self.client.get_resource(uri)
            vhs_info['cpuVirtualQuantity'] = cpu_resource[
                '{' + NSMAP['rasd'] + '}VirtualQuantity']
            vhs_info['cpuCoresPerSocket'] = cpu_resource[
                '{' + NSMAP['vmw'] + '}CoresPerSocket']

        if is_memory:
            uri = self.href + '/virtualHardwareSection/memory'
            memory_resource = self.client.get_resource(uri)
            vhs_info['memoryVirtualQuantityInMb'] = memory_resource[
                '{' + NSMAP['rasd'] + '}VirtualQuantity']

        if is_disk:
            uri = self.href + '/virtualHardwareSection/disks'
            disk_list = self.client.get_resource(uri)
            disk_count = 0
            for disk in disk_list.Item:
                if disk['{' + NSMAP['rasd'] + '}Description'] == 'Hard disk':
                    vhs_info['diskElementName' + str(disk_count)] = disk[
                        '{' + NSMAP['rasd'] + '}ElementName']
                    vhs_info['diskVirtualQuantityInBytes' + str(disk_count)] \
                        = disk[
                        '{' + NSMAP['rasd'] + '}VirtualQuantity']
                    disk_count = disk_count + 1

        if is_media:
            uri = self.href + '/virtualHardwareSection/media'
            media_list = self.client.get_resource(uri)
            media_count = 0
            for media in media_list.Item:
                if media['{' +
                         NSMAP['rasd'] + '}Description'] == 'CD/DVD Drive':
                    if media['{' +
                             NSMAP['rasd'] + '}HostResource'].text is not None:
                        vhs_info['mediaCdElementName' + str(media_count)] = \
                            media['{' + NSMAP['rasd'] + '}ElementName']
                        vhs_info['mediaCdHostResource' + str(media_count)] = \
                            media['{' + NSMAP['rasd'] + '}HostResource']
                        media_count = media_count + 1
                        continue
                if media['{' +
                         NSMAP['rasd'] + '}Description'] == 'Floppy Drive':
                    if media['{' +
                             NSMAP['rasd'] + '}HostResource'].text is not None:
                        vhs_info['mediaFloppyElementName' +
                                 str(media_count)] = \
                            media['{' + NSMAP['rasd'] + '}ElementName']
                        vhs_info['mediaFloppyHostResource'
                                 + str(media_count)] = \
                            media['{' + NSMAP['rasd'] + '}HostResource']
                        media_count = media_count + 1
                        continue

        if is_networkCards:
            uri = self.href + '/virtualHardwareSection/networkCards'
            ncards_list = self.client.get_resource(uri)
            ncard_count = 0
            for ncard in ncards_list.Item:
                if ncard['{' + NSMAP['rasd'] + '}Connection'] is not None:
                    vhs_info['ncardElementName' + str(ncard_count)] = ncard[
                        '{' + NSMAP['rasd'] + '}ElementName']
                    vhs_info['nCardConnection' + str(ncard_count)] = ncard[
                        '{' + NSMAP['rasd'] + '}Connection']
                    vhs_info['nCardIpAddressingMode' + str(ncard_count)] = \
                        ncard.xpath('rasd:Connection', namespaces=NSMAP)[
                            0].attrib.get(
                            '{' + NSMAP['vcloud'] + '}ipAddressingMode')
                    vhs_info['nCardIpAddress' + str(ncard_count)] = \
                        ncard.xpath('rasd:Connection', namespaces=NSMAP)[
                            0].attrib.get('{' + NSMAP['vcloud'] + '}ipAddress')
                    vhs_info[
                        'nCardPrimaryNetworkConnection' + str(ncard_count)] = \
                        ncard.xpath('rasd:Connection', namespaces=NSMAP)[
                            0].attrib.get(
                            '{' + NSMAP['vcloud'] +
                            '}primaryNetworkConnection')
                    vhs_info['nCardAddress' + str(ncard_count)] = ncard[
                        '{' + NSMAP['rasd'] + '}Address']
                    vhs_info['nCardAddressOnParent' + str(ncard_count)] = \
                        ncard[
                        '{' + NSMAP['rasd'] + '}AddressOnParent']
                    vhs_info['nCardAutomaticAllocation' + str(ncard_count)] = \
                        ncard['{' + NSMAP['rasd'] + '}AutomaticAllocation']
                    vhs_info['nCardResourceSubType' + str(ncard_count)] = \
                        ncard['{' + NSMAP['rasd'] + '}ResourceSubType']
                    ncard_count = ncard_count + 1

        return vhs_info

    def get_compliance_result(self):
        """Get compliance result of a VM.

        :return: an object containing EntityType.ComplianceResult XML data
                     which contains compliance status of VM

        :rtype: lxml.objectify.ObjectifiedElement
        """
        self.get_resource()
        return self.client. \
            get_linked_resource(self.resource, rel=RelationType.DOWN,
                                media_type=EntityType.COMPLIANCE_RESULT.value)

    def list_all_current_metrics(self):
        """List current metrics of a VM.

        :return: dict which contains current metrics of VM

        :rtype: dict
        """
        metrics_info = {}
        self.get_resource()
        metrics_list = self.client. \
            get_linked_resource(self.resource, rel=RelationType.DOWN,
                                media_type=EntityType.CURRENT_USAGE.value)
        metric_count = 0
        for metric in metrics_list.Metric:
            metrics_info['metric_name' + str(metric_count)] = metric. \
                get('name')
            metrics_info['metric_unit' + str(metric_count)] = metric. \
                get('unit')
            metrics_info['metric_value' + str(metric_count)] = metric.get(
                'value')
            metric_count = metric_count + 1
        return metrics_info

    def list_current_metrics_subset(self, metric_pattern=None):
        """List current metrics subset of a VM.

        :return: dict which contains current metrics subset of VM

        :rtype: dict
        """
        metrics_info = {}
        self.get_resource()
        current_usage_spec = E.CurrentUsageSpec(
            E.MetricPattern(metric_pattern))
        metrics_list = self.client. \
            post_linked_resource(self.resource, rel=RelationType.METRICS,
                                 media_type=EntityType.CURRENT_USAGE.value,
                                 contents=current_usage_spec)

        metric_count = 0
        for metric in metrics_list.Metric:
            metrics_info['metric_name' + str(metric_count)] = metric. \
                get('name')
            metrics_info['metric_unit' + str(metric_count)] = metric. \
                get('unit')
            metrics_info['metric_value' + str(metric_count)] = metric.get(
                'value')
            metric_count = metric_count + 1
        return metrics_info

    def list_all_historic_metrics(self):
        """List historic metrics of a VM.

        :return: dict which contains historic metrics of VM

        :rtype: dict
        """
        metrics_info = {}
        self.get_resource()
        metrics_list = self.client. \
            get_linked_resource(self.resource, rel=RelationType.DOWN,
                                media_type=EntityType.HISTORIC_USAGE.value)
        metric_count = 0
        for metric in metrics_list.MetricSeries:
            metrics_info['metric_name' + str(metric_count)] = metric. \
                get('name')
            metrics_info['expected_interval' + str(metric_count)] = metric. \
                get('expectedInterval')
            metrics_info['metric_unit' + str(metric_count)] = metric.get(
                'unit')
            metric_count = metric_count + 1
        return metrics_info
