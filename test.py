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
    # raise ZeroDivisionError(login_credentials.__dict__)
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
        result = await vdc.instantiate_vapp(
            name,
            'Test',
            'Ubuntu 18.04 x64 v3 (minimal requirements)',
        )
        await client.get_task_monitor().wait_for_success(
            task=result.Tasks.Task[0],
        )

        await vdc.reload()
        vapp_xml = await vdc.get_vapp(name)
        vapp = VApp(vdc.client, name=name, resource=vapp_xml)

        yield vapp

        await asyncio.sleep(1.0)
        await vdc.reload()
        await vapp.reload()

        result = await vdc.delete_vapp_by_id(vapp.id, force=True)
        await client.get_task_monitor().wait_for_success(
            task=result,
        )


@pytest.fixture()
async def vapp_off():
    async with client_vdc() as (client, org, vdc):
        name = uuid.uuid4().hex[:5]
        result = await vdc.instantiate_vapp(
            name,
            'Test',
            'Ubuntu 18.04 x64 v3 (minimal requirements)',
            deploy=False,
            power_on=False,
        )
        await client.get_task_monitor().wait_for_success(
            task=result.Tasks.Task[0],
        )

        await vdc.reload()
        vapp_xml = await vdc.get_vapp(name)
        vapp = VApp(vdc.client, name=name, resource=vapp_xml)

        yield vapp

        await vdc.reload()
        await asyncio.sleep(1.0)
        await vapp.reload()
        result = await vdc.delete_vapp(vapp.name, force=True)

        await client.get_task_monitor().wait_for_success(
            task=result,
        )


@pytest.fixture
async def template(vapp):
    async with client_vdc() as (client, org, vdc):
        template_name = uuid.uuid4().hex[:10]
        vdc_resource = await org.get_vdc(vdc_name)
        result = await org.capture_vapp(
            vdc_resource,
            vapp.href,
            template_name,
            'Test template'
        )
        await client.get_task_monitor().wait_for_success(
            task=result.Tasks.Task[0],
        )

        yield template_name

        await vdc.reload()
        href = await vdc.get_resource_href(
            template_name,
            entity_type=EntityType.VAPP_TEMPLATE
        )
        result = await client.delete_resource(href, force=True)

        await client.get_task_monitor().wait_for_success(
            task=result,
        )


@pytest.mark.asyncio
async def test_suspend_on_off(vapp):
    # await asyncio.sleep(1)
    await vapp.reload()
    result = await vapp._perform_power_operation(
        rel=RelationType.POWER_SUSPEND, operation_name='power suspend')
    await vapp.client.get_task_monitor().wait_for_success(
        task=result,
    )

    # await asyncio.sleep(5)
    await vapp.reload()

    result = await vapp._perform_power_operation(
        rel=RelationType.DISCARD_SUSPENDED_STATE, operation_name='discard suspend')
    await vapp.client.get_task_monitor().wait_for_success(
        task=result,
    )


@pytest.mark.asyncio
async def test_create_delete_getlist_vapp(vapp):
    async with client_vdc() as (_, _, vdc):
        vapps = await vdc.list_resources(EntityType.VAPP)
        apps = set([vapp['name'] for vapp in vapps])
        assert vapp.name in apps


@pytest.mark.asyncio
async def test_poweroff_shutdown(vapp):
    await vapp.reload()
    result = await vapp.power_off()
    await vapp.client.get_task_monitor().wait_for_success(
        task=result,
    )
    await vapp.reload()

    result = await vapp.power_on()
    await vapp.client.get_task_monitor().wait_for_status(
        task=result,
    )
    await vapp.reload()

    result = await vapp.shutdown()
    await vapp.client.get_task_monitor().wait_for_success(
        task=result,
    )


@pytest.mark.asyncio
async def test_add_resources(vapp):
    async with client_vdc() as (client, _, vdc):
        # time.sleep(1)
        await vapp.reload()

        result = await vapp.power_off()
        await vapp.client.get_task_monitor().wait_for_success(
            task=result,
        )
        # time.sleep(1)
        await vdc.reload()
        for vm_xml in await vapp.get_all_vms():
            vm = VM(client, resource=vm_xml)

            # Modify memory
            result = await vm.modify_memory(2024)
            await vapp.client.get_task_monitor().wait_for_success(
                task=result,
            )

            await vm.reload()
            mem = await vm.get_memory()
            assert mem == 2024

            # vm.reload()
            result = await vm.modify_memory(1024)
            await vapp.client.get_task_monitor().wait_for_success(
                task=result,
            )
            await vm.reload()
            mem = await vm.get_memory()
            assert mem == 1024

            # Modify CPU
            result = await vm.modify_cpu(4, 2)
            await vapp.client.get_task_monitor().wait_for_success(
                task=result,
            )

            await vm.reload()
            result = await vm.get_cpus()
            assert result['num_cpus'] == 4
            assert result['num_cores_per_socket'] == 2

            # Create a disk
            result = await vdc.create_disk(
                name='TestName',
                size=1024 * 1024 * 50,  # 50 MB
                description='Test description'
            )
            disk_id = result.get('id')

            await vapp.client.get_task_monitor().wait_for_success(
                task=result.Tasks.Task[0],
            )

            await vdc.reload()
            disk = await vdc.get_disk(disk_id=disk_id)

            assert disk.get('name') == 'TestName'
            assert int(disk.get('size')) == 1024 * 1024 * 50
            assert disk.Description == 'Test description'

            # Attach disk
            result = await vapp.attach_disk_to_vm(disk_href=disk.get('href'),
                                            vm_name=vm_xml.get('name'))
            await client.get_task_monitor().wait_for_success(
                task=result,
            )

            # Detach disk
            result = await vapp.detach_disk_from_vm(disk_href=disk.get('href'),
                                            vm_name=vm_xml.get('name'))
            await client.get_task_monitor().wait_for_success(
                task=result,
            )

            # Update disk
            result = await vdc.update_disk(
                disk_id=disk_id,
                new_name='New name',
                new_size=1024 * 1024 * 100,  # 100 MB
                new_description='New description',
            )
            await client.get_task_monitor().wait_for_success(
                task=result,
            )

            disk = await vdc.get_disk(disk_id=disk_id)

            assert disk.get('id') == disk_id
            assert disk.get('name') == 'New name'
            assert int(disk.get('size')) == 1024 * 1024 * 100
            assert disk.Description == 'New description'

            # Delete disk
            result = await vdc.delete_disk(disk_id=disk_id)
            await vapp.client.get_task_monitor().wait_for_success(
                task=result,
            )


@pytest.mark.asyncio
async def test_snapshot(vapp):
    async with client_vdc() as (client, _, vdc):
        await vapp.reload()

        result = await vapp.power_off()
        await vapp.client.get_task_monitor().wait_for_success(
            task=result,
        )
        await vdc.reload()

        for vm_xml in await vapp.get_all_vms():
            vm = VM(client, resource=vm_xml)

            # Create snapshot
            result = await vm.snapshot_create(name='TestSnapshot')
            await vapp.client.get_task_monitor().wait_for_success(
                task=result,
            )

            # Check is there a snapshot
            await vm.reload()
            assert len((await vm.get_resource()).SnapshotSection.Snapshot)

            # Revert to current shapshot
            await vm.reload()
            result = await vm.snapshot_revert_to_current()
            await vapp.client.get_task_monitor().wait_for_success(
                task=result,
            )

            # Remove all shapshot
            await vm.reload()
            result = await vm.snapshot_remove_all()
            await vapp.client.get_task_monitor().wait_for_success(
                task=result,
            )


@pytest.mark.asyncio
async def test_status(vapp_off):
    assert await vapp_off.is_powered_off() == True
    assert await vapp_off.is_powered_on() == False
    assert await vapp_off.is_suspended() == False
    assert await vapp_off.is_deployed() == False

    result = await vapp_off.deploy(True)
    await vapp_off.client.get_task_monitor().wait_for_success(
        task=result,
    )

    await vapp_off.reload()
    assert (await vapp_off.get_resource()).get('deployed') == 'true'
    assert await vapp_off.is_powered_on() == True

    await vapp_off.reload()
    result = await vapp_off.undeploy('powerOff')
    await vapp_off.client.get_task_monitor().wait_for_success(
        task=result,
    )

    await vapp_off.reload()
    assert (await vapp_off.get_resource()).get('deployed') == 'false'

    assert await vapp_off.is_powered_off() == True
    assert await vapp_off.is_powered_on() == False
    assert await vapp_off.is_suspended() == False


@pytest.mark.asyncio
async def test_change_name(vapp_off):
    name = 'testChangeName'

    result = await vapp_off.edit_name_and_description(name)
    await vapp_off.client.get_task_monitor().wait_for_success(
        task=result,
    )

    await vapp_off.reload()
    assert vapp_off.name == name


@pytest.mark.asyncio
async def test_get_vdc_list():
    async with client_vdc() as (client, org, vdc):
        l = org.list_vdcs()
        l = list(map(lambda xml: VDC(client, resource=xml), l))
        for vdc in l:
            assert vdc.name == vdc_name


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
            result = await vdc.create_routed_vdc_network(
                test_network_name,
                'cloudmng-dev-edge',
                '47.243.181.201/29',
                'Test network description'
            )
            await client.get_task_monitor().wait_for_success(
                task=result.Tasks.Task[0],
            )

            # Check that network in list
            l = await vdc.list_orgvdc_network_records()
            assert any(
                dic['name'] == test_network_name for dic in l
            )

            # Connect vapp to network
            result = await vapp_off.connect_org_vdc_network(test_network_name)
            await client.get_task_monitor().wait_for_success(
                task=result,
            )

            # Get current vm
            vm = VM(client, resource=(await vapp_off.get_all_vms())[0])

            # Add a nic
            result = await vm.add_nic(
                NetworkAdapterType.VMXNET3.value,
                False,
                True,
                test_network_name,
                'DHCP',
                ''
            )
            await client.get_task_monitor().wait_for_success(
                task=result,
            )

            # Check nic in VM
            assert test_network_name in (dic['network'] for dic in await vm.list_nics())
        finally:
            result = await vdc.delete_routed_orgvdc_network(test_network_name, force=True)
            await client.get_task_monitor().wait_for_success(
                task=result,
            )


@pytest.mark.asyncio
async def test_get_vapp_by_id(vapp):
    vapp_name = vapp.name
    vapp_id = vapp.id
    async with client_vdc() as (client, org, vdc):
        vapp_resource = await vdc.get_vapp_by_id(vapp_id)
        vapp = VApp(client, resource=vapp_resource)
        assert vapp_name == vapp.name
