#!/usr/bin/env python3

import asyncio
from functools import partial
import time
import uuid
from contextlib import contextmanager, asynccontextmanager

import pytest
import requests
from envparse import env
from lxml import etree

from pyvcloud.vcd.client import BasicLoginCredentials
from pyvcloud.vcd.client import Client, MetadataValueType,\
    MetadataVisibility, MetadataDomain, ResourceType,\
    NetworkAdapterType
from pyvcloud.vcd.client import EntityType
from pyvcloud.vcd.org import Org
from pyvcloud.vcd.task import Task, TaskStatus
from pyvcloud.vcd.vapp import VApp, RelationType
from pyvcloud.vcd.vdc import VDC
from pyvcloud.vcd.vm import VM


env.read_envfile()

host = env('host')
org = env('org')
user = env('user')
password = env('password')
vdc_name = env('vdc_name')

# Disable warnings from self-signed certificates.
requests.packages.urllib3.disable_warnings()


def _save_xml_to_file(resource, fn):
    with open(fn, 'wb') as f:
        f.write(
            etree.tostring(resource, pretty_print=True)
        )


@asynccontextmanager
async def client_vdc():
    client = Client(host,
                    api_version='31.0',
                    verify_ssl_certs=False,
                    log_file='pyvcloud.log',
                    log_requests=True,
                    log_headers=True,
                    log_bodies=True)
    login_credentials = BasicLoginCredentials(
        user,
        globals()['org'],
        password
    )
    await client.set_credentials(
        login_credentials
    )

    org_resource = await client.get_org()
    org = Org(client, resource=org_resource)

    print("Fetching VDC...")
    vdc_resource = await org.get_vdc(vdc_name)
    vdc = VDC(client, resource=vdc_resource)
    yield client, org, vdc
    print("Logging out")
    await client.logout()

@pytest.fixture()
async def vapp():
    async with client_vdc() as (client, org, vdc):
        name = uuid.uuid4().hex[:5]
        await vdc.instantiate_vapp(
            name,
            'Test',
            'Ubuntu 18.04 x64 v3 (minimal requirements)',
        )

        await vdc.reload()
        vapp_xml = await vdc.get_vapp(name)
        vapp = VApp(vdc.client, name=name, resource=vapp_xml)

        yield vapp

        await asyncio.sleep(1.0)
        await vdc.reload()
        await vapp.reload()

        await vdc.delete_vapp_by_id(vapp.id, force=True)


@pytest.fixture()
async def vapp_off():
    async with client_vdc() as (client, org, vdc):
        name = uuid.uuid4().hex[:5]
        await vdc.instantiate_vapp(
            name,
            'Test',
            'Ubuntu 18.04 x64 v3 (minimal requirements)',
            deploy=False,
            power_on=False,
        )

        await vdc.reload()
        vapp_xml = await vdc.get_vapp(name)
        vapp = VApp(vdc.client, name=name, resource=vapp_xml)

        yield vapp

        await vdc.reload()
        await asyncio.sleep(1.0)
        await vapp.reload()
        await vdc.delete_vapp(vapp.name, force=True)


@pytest.fixture
async def template(vapp):
    async with client_vdc() as (client, org, vdc):
        template_name = uuid.uuid4().hex[:10]
        vdc_resource = await org.get_vdc(vdc_name)
        await org.capture_vapp(
            vdc_resource,
            vapp.href,
            template_name,
            'Test template'
        )

        yield template_name

        await vdc.reload()
        href = await vdc.get_resource_href(
            template_name,
            entity_type=EntityType.VAPP_TEMPLATE
        )
        await client.delete_resource(href, force=True)


@pytest.mark.asyncio
async def test_suspend_on_off(vapp):
    # await asyncio.sleep(1)
    await vapp.reload()
    await vapp._perform_power_operation(
        rel=RelationType.POWER_SUSPEND, operation_name='power suspend')

    # await asyncio.sleep(5)
    await vapp.reload()

    await vapp._perform_power_operation(
        rel=RelationType.DISCARD_SUSPENDED_STATE, operation_name='discard suspend')


@pytest.mark.asyncio
async def test_create_delete_getlist_vapp(vapp):
    async with client_vdc() as (_, _, vdc):
        vapps = await vdc.list_resources(EntityType.VAPP)
        apps = set([vapp['name'] for vapp in vapps])
        assert vapp.name in apps


@pytest.mark.asyncio
async def test_poweroff_shutdown(vapp):
    await vapp.reload()
    await vapp.power_off()
    await vapp.reload()

    await vapp.power_on()
    await vapp.reload()

    await vapp.shutdown()


@pytest.mark.asyncio
async def test_add_resources(vapp):
    async with client_vdc() as (client, _, vdc):
        # time.sleep(1)
        await vapp.reload()

        await vapp.power_off()
        await vdc.reload()
        for vm_xml in await vapp.get_all_vms():
            vm = VM(client, resource=vm_xml)

            # Modify memory
            await vm.modify_memory(2024)

            await vm.reload()
            mem = await vm.get_memory()
            assert mem == 2024

            await vm.modify_memory(1024)
            await vm.reload()
            mem = await vm.get_memory()
            assert mem == 1024

            # Modify CPU
            await vm.modify_cpu(4, 2)

            await vm.reload()
            result = await vm.get_cpus()
            assert result['num_cpus'] == 4
            assert result['num_cores_per_socket'] == 2

            # Create a disk
            disk_id = await vdc.create_disk(
                name='TestName',
                size=1024 * 1024 * 50,  # 50 MB
                description='Test description'
            )
            await vdc.reload()
            try:
                disk = await vdc.get_disk(disk_id=disk_id)

                assert disk.get('name') == 'TestName'
                assert int(disk.get('size')) == 1024 * 1024 * 50
                assert disk.Description == 'Test description'

                # Attach disk
                await vapp.attach_disk_to_vm(disk_href=disk.get('href'),
                                                vm_name=vm_xml.get('name'))

                # Detach disk
                await vapp.detach_disk_from_vm(disk_href=disk.get('href'),
                                                vm_name=vm_xml.get('name'))

                # Update disk
                await vdc.update_disk(
                    disk_id=disk_id,
                    new_name='New name',
                    new_size=1024 * 1024 * 100,  # 100 MB
                    new_description='New description',
                )

                disk = await vdc.get_disk(disk_id=disk_id)

                assert disk.get('id') == disk_id
                assert disk.get('name') == 'New name'
                assert int(disk.get('size')) == 1024 * 1024 * 100
                assert disk.Description == 'New description'
            finally:
                # Delete disk
                await vdc.delete_disk(disk_id=disk_id)


@pytest.mark.asyncio
async def test_snapshot(vapp):
    async with client_vdc() as (client, _, vdc):
        await vapp.reload()

        await vapp.power_off()
        await vdc.reload()

        for vm_xml in await vapp.get_all_vms():
            vm = VM(client, resource=vm_xml)

            # Create snapshot
            await vm.snapshot_create(name='TestSnapshot')

            # Check is there a snapshot
            await vm.reload()
            assert len((await vm.get_resource()).SnapshotSection.Snapshot)

            # Revert to current shapshot
            await vm.reload()
            await vm.snapshot_revert_to_current()

            # Remove all shapshot
            await vm.reload()
            await vm.snapshot_remove_all()


@pytest.mark.asyncio
async def test_status(vapp_off):
    assert await vapp_off.is_powered_off() == True
    assert await vapp_off.is_powered_on() == False
    assert await vapp_off.is_suspended() == False
    assert await vapp_off.is_deployed() == False

    await vapp_off.deploy(True)

    await vapp_off.reload()
    assert (await vapp_off.get_resource()).get('deployed') == 'true'
    assert await vapp_off.is_powered_on() == True

    await vapp_off.reload()
    await vapp_off.undeploy('powerOff')

    await vapp_off.reload()
    assert (await vapp_off.get_resource()).get('deployed') == 'false'

    assert await vapp_off.is_powered_off() == True
    assert await vapp_off.is_powered_on() == False
    assert await vapp_off.is_suspended() == False


@pytest.mark.asyncio
async def test_change_name(vapp_off):
    name = 'testChangeName'

    await vapp_off.edit_name_and_description(name)

    await vapp_off.reload()
    assert vapp_off.name == name


@pytest.mark.asyncio
async def test_get_vdc_list():
    async with client_vdc() as (client, org, vdc):
        l = org.list_vdcs()
        l = list(map(lambda xml: VDC(client, resource=xml), l))
        assert vdc_name in (vdc.name for vdc in l)


@pytest.mark.asyncio
async def test_template(template):
    async with client_vdc() as (client, org, vdc):
        assert sum([
            dic['name'] == template for dic in await vdc.list_resources(
                EntityType.VAPP_TEMPLATE
            )
        ]) == 1


@pytest.mark.asyncio
async def test_network(vapp_off):
    """
    Test create, connect and remove network.
    """
    test_network_name = 'test_network4'
    async with client_vdc() as (client, org, vdc):
        try:
            # Create route network
            await vdc.create_routed_vdc_network(
                test_network_name,
                'cloudmng-dev-edge',
                '47.243.181.201/29',
                'Test network description'
            )

            # Check that network in list
            l = await vdc.list_orgvdc_network_records()
            assert any(
                dic['name'] == test_network_name for dic in l
            )

            # Connect vapp to network
            await vapp_off.connect_org_vdc_network(test_network_name)

            # Get current vm
            vm = VM(client, resource=(await vapp_off.get_all_vms())[0])

            # Add a nic
            await vm.add_nic(
                NetworkAdapterType.VMXNET3.value,
                False,
                True,
                test_network_name,
                'DHCP',
                ''
            )

            # Check nic in VM
            assert test_network_name in (dic['network'] for dic in await vm.list_nics())
        finally:
            await vdc.reload()
            await vdc.delete_routed_orgvdc_network(test_network_name, force=True)


@pytest.mark.asyncio
async def test_get_vapp_by_id(vapp):
    vapp_name = vapp.name
    vapp_id = vapp.id
    async with client_vdc() as (client, org, vdc):
        vapp_resource = await vdc.get_vapp_by_id(vapp_id)
        vapp = VApp(client, resource=vapp_resource)
        assert vapp_name == vapp.name


@pytest.mark.asyncio
async def test_guest_customization_section(vapp):
    vm_resource = (await vapp.get_all_vms())[0]
    vm = VM(vapp.client, resource=vm_resource)
    dic = await vm.get_guest_customization_section()
    for field_name in ('id', 'name', 'password'):
        assert field_name in dic
    await vm.set_guest_customization_section(
        Adminpassword='123',
        Adminpasswordauto=False,
        Adminpasswordenabled=True,
    )
    await vm.reload()
    dic2 = await vm.get_guest_customization_section()
    assert dic2['Adminpassword'] == '123'
    for field_name in ('id', 'name'):
        assert dic[field_name] == dic2[field_name]
