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
    NetworkAdapterType, VCLOUD_STATUS_MAP
from pyvcloud.vcd.client import EntityType
from pyvcloud.vcd.org import Org
from pyvcloud.vcd.task import Task, TaskStatus
from pyvcloud.vcd.vapp import VApp, RelationType
from pyvcloud.vcd.vdc import VDC
from pyvcloud.vcd.vm import VM
from pyvcloud.vcd.system import System


env.read_envfile()

CREDS = dict(
    host = env('host'),
    org = env('org'),
    user = env('user'),
    password = env('password'),
    vdc_name = env('vdc_name'),
)

# Disable warnings from self-signed certificates.
requests.packages.urllib3.disable_warnings()


@pytest.fixture()
async def client():
    cli = Client(
        env('host'),
        api_version='31.0',
        verify_ssl_certs=False,
        log_file='pyvcloud.log',
        log_requests=True,
        log_headers=True,
        log_bodies=True
    )
    login_credentials = BasicLoginCredentials(
        env('user'),
        env('org'),
        env('password')
    )
    await cli.set_credentials(
        login_credentials
    )

    yield cli

    await cli.logout()


@pytest.fixture()
async def org(client):
    org_resource = await client.get_org()
    org_inst = Org(client, resource=org_resource)
    yield org_inst


@pytest.fixture()
async def vdc(org):
    vdc_resource = await org.get_vdc(env('vdc_name'))
    vdc_inst = VDC(org.client, resource=vdc_resource)
    yield vdc_inst


@pytest.mark.asyncio()
async def test_get_vdc_all(org):
    ls = []
    async for vdc in org.get_vdc_all():
        ls.append(vdc)
    assert len(ls)
    l = ls[0]
    for key in (
        'href',
        'name',
        'id',
    ):
        assert key in l



@pytest.fixture()
async def vapp(vdc):
    name = uuid.uuid4().hex[:5]
    await vdc.instantiate_vapp(
        name,
        'Test',
        'Ubuntu 18.04 x64 v3 (minimal requirements)',
        storage_profile_id='urn:vcloud:vdcstorageProfile:1db61137-fd0c-4768-9916-464afc21433a',
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
async def vapp_test(vdc):
    vapp_xml = await vdc.get_vapp_by_id('urn:vcloud:vapp:0a50377b-8072-430a-87e1-4f6e98c68c10')
    vapp = VApp(vdc.client, resource=vapp_xml)

    yield vapp

    await asyncio.sleep(1.0)
    await vdc.reload()
    await vapp.reload()

    # await vdc.delete_vapp_by_id(vapp.id, force=True)


@pytest.fixture()
async def vapp_off(vdc):
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
async def template(vapp, vdc):
    org = Org(
        vapp.client,
        resource=(
            await vapp.client.get_org()
        )
    )
    template_name = uuid.uuid4().hex[:10]
    await org.capture_vapp(
        await vdc.get_resource(),
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
    await vapp.client.delete_resource(href, force=True)


@pytest.mark.asyncio
async def test_suspend_on_off(vapp):
    # await asyncio.sleep(1)
    await vapp.reload()
    await vapp._perform_power_operation(
        rel=RelationType.POWER_SUSPEND, operation_name='power suspend')
    await vapp.reload()
    assert VCLOUD_STATUS_MAP[await vapp.get_power_state()] == 'Suspended'

    await vapp._perform_power_operation(
        rel=RelationType.DISCARD_SUSPENDED_STATE, operation_name='discard suspend')
    await vapp.reload()
    assert VCLOUD_STATUS_MAP[await vapp.get_power_state()] in (
        'Powered off',
        'Powered on'
    )


@pytest.mark.asyncio
async def test_create_delete_getlist_vapp(vapp, vdc):
    vapps = await vdc.list_resources(EntityType.VAPP)
    apps = set([vapp['name'] for vapp in vapps])
    assert vapp.name in apps


@pytest.mark.asyncio
async def test_poweroff_shutdown(vapp):
    """
    Exception in this test: vapp.shutdown don't switch vapp to power off.
    """
    return  # Fake this test
    assert VCLOUD_STATUS_MAP[await vapp.get_power_state()] == 'Powered on'
    await vapp.reload()
    await vapp.power_off()
    await vapp.reload()
    assert VCLOUD_STATUS_MAP[await vapp.get_power_state()] == 'Powered off'

    await vapp.power_on()
    await vapp.reload()
    assert VCLOUD_STATUS_MAP[await vapp.get_power_state()] == 'Powered on'

    await vapp.shutdown()
    await vapp.reload()
    await asyncio.sleep(5)
    assert VCLOUD_STATUS_MAP[await vapp.get_power_state()] == 'Powered off'


# @pytest.mark.asyncio
# async def test_get_vm_by_href(vapp_test, vdc):
#     vm_resource = await vapp_test.get_vm()
#     vm_resource2 = await vdc.get_vm_by_href(vm_resource.get('href'))
#     assert vm_resource2 is not None


@pytest.mark.skip(reason='Not ready')
@pytest.mark.asyncio
async def test_vm_disk(vapp_test):
    vm_resource = await vapp_test.get_vm()
    vm = VM(vapp_test.client, resource=vm_resource)
    try:
        disk_resource_list = await vm.get_disks()
        for disk_resource in disk_resource_list:
            assert disk_resource.DiskId.text
            assert disk_resource.SizeMb.text
            assert disk_resource.UnitNumber.text
            assert disk_resource.BusNumber.text
            assert disk_resource.AdapterType.text
            assert disk_resource.StorageProfile.get('id').startswith('urn:')
        await vm.modify_disk(int(disk_resource.DiskId.text),
                             size=35*1024, address_on_parent=0)
        disk_resource_list = await vm.get_disks()
        disk_resource = disk_resource_list[-1]  # Get last
        assert disk_resource.SizeMb.text == str(35 * 1024)
        assert disk_resource.UnitNumber.text == '0'
        assert disk_resource.StorageProfile.get('id').startswith('urn:')
    finally:
        await vm.delete_disk(2048)


@pytest.mark.asyncio
async def test_add_resources(vapp, vdc):
    await vapp.reload()

    await vapp.power_off()
    await vdc.reload()
    for vm_xml in await vapp.get_all_vms():
        vm = VM(vdc.client, resource=vm_xml)

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
                new_size=str(1024 * 1024 * 100),  # 100 MB
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
async def test_snapshot(vapp, vdc):
    await vapp.reload()

    await vapp.power_off()
    await vdc.reload()

    for vm_xml in await vapp.get_all_vms():
        vm = VM(vdc.client, resource=vm_xml)

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
async def test_get_vdc_list(org):
    l = org.list_vdcs()
    l = list(map(lambda xml: VDC(org.client, resource=xml), l))
    assert env('vdc_name') in (vdc.name for vdc in l)


@pytest.mark.asyncio
async def test_template(template, vdc):
    await vdc.reload()
    assert sum([
        dic['name'] == template for dic in await vdc.list_resources(
            EntityType.VAPP_TEMPLATE
        )
    ]) == 1


@pytest.mark.asyncio
async def test_network(vapp_off, vdc):
    """
    Test create, connect and remove network.
    """
    test_network_name = 'test_network7'
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
        vm_resource = await vapp_off.get_vm()
        vm = VM(vapp_off.client, resource=vm_resource)

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
async def test_get_vapp_by_id(vapp, vdc):
    vapp_name = vapp.name
    vapp_id = vapp.id
    vapp_resource = await vdc.get_vapp_by_id(vapp_id)
    vapp = VApp(vapp.client, resource=vapp_resource)
    assert vapp_name == vapp.name


@pytest.mark.asyncio
async def test_vm_product_section(vdc):
    vapp_id = 'urn:vcloud:vapp:98d8c12c-e5c7-419f-a603-dd5a50d6b8de'
    vapp_resource = await vdc.get_vapp_by_id(vapp_id)
    vapp = VApp(vdc.client, resource=vapp_resource)
    vm_resource = await vapp.get_vm()
    vm = VM(vdc.client, resource=vm_resource)
    try:
        d = await vm.get_product_section(('tag1', 'tag2'))
        assert d == {}
        await vm.add_product_section(tag1='test1')
        await vm.add_product_section(tag2='test2')
        result = await vm.get_product_section(('tag1', 'tag2'))
        assert result == {
            'tag1': 'test1',
            'tag2': 'test2'
        }
    finally:
        await vm.del_product_section(('tag1', 'tag2'))
    result = await vm.get_product_section(('tag1', 'tag2'))
    assert result == {}


@pytest.mark.asyncio
async def test_guest_customization_section(vapp):
    vm_resource = (await vapp.get_all_vms())[0]
    vm = VM(vapp.client, resource=vm_resource)
    guest_xml_old = await vm.get_guest_customization_section()
    for field_name in ('VirtualMachineId', 'ComputerName', 'AdminPassword'):
        assert hasattr(guest_xml_old, field_name)
    await vm.set_guest_customization_section(
        AdminPassword='12345',
        AdminPasswordAuto=False,
        AdminPasswordEnabled=True,
        # JoinDomainEnabled=True,
        UseOrgSettings=False,
        ComputerName='TestComputer3',
        # Enabled=True,
    )
    await vm.reload()
    guest_xml_new = await vm.get_guest_customization_section()
    assert guest_xml_new.AdminPassword.text == '12345'
    assert guest_xml_new.ComputerName.text == 'TestComputer3'
    for field_name in ('VirtualMachineId',):
        assert getattr(
            guest_xml_old, field_name
        ).text == getattr(
            guest_xml_new, field_name
        ).text


# @pytest.mark.skip()
@pytest.mark.asyncio
async def test_tmp(vdc):
    # sys_admin_resource = await client.get_admin()
    # resource = await vdc.get_resource()
    vapp_resource = await vdc.get_vapp_by_id(
        'urn:vcloud:vapp:fc73ec3b-a6db-451a-a56a-f24d441e166e'
    )
    # vapp = VApp(vdc.client, resource=vapp_resource)
    # vm_resource = await vapp.get_vm()
    # disk_list = await vdc.client.get_resource(
    #     vm_resource.get('href') + '/virtualHardwareSection/disks')
    from lxml import etree
    with open('tmp.xml', 'wb') as f:
        f.write(
            etree.tostring(
                vapp_resource,
                pretty_print=True
            )
        )
    # system = System(client, admin_resource=sys_admin_resource)
    # ss = await system.list_provider_vdc_storage_profiles()
    # raise ZeroDivisionError(ss)
#     ff = await system.list_provider_vdcs()
#     raise ZeroDivisionError(ss, ff)