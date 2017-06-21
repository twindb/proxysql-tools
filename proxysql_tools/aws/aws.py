# pylint: skip-file
import os
import time
from ConfigParser import NoOptionError
from subprocess import Popen

import netifaces
import requests
import boto3
from botocore.exceptions import ClientError

from proxysql_tools import LOG

DEVICE_INDEX = 1


def get_my_instance_id():
    r = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
    r.raise_for_status()
    return r.content


def get_network_interface(ip):
    client = boto3.client('ec2')
    response = client.describe_network_interfaces(
        Filters=[
            {
                'Name': 'addresses.private-ip-address',
                'Values': [
                    ip,
                ]
            },
        ]
    )
    network_interface_id = \
        response['NetworkInterfaces'][0]['NetworkInterfaceId']
    return network_interface_id


def ensure_local_interface_is_gone(local_interface):
    while local_interface in netifaces.interfaces():
        pass


def get_network_interface_state(network_interface):
    client = boto3.client('ec2')
    response = client.describe_network_interfaces(
        NetworkInterfaceIds=[network_interface]
    )
    status = response['NetworkInterfaces'][0]['Attachment']['Status']
    LOG.debug('Interface %s, status = %s', network_interface, status)
    return status


def network_interface_attached(network_interface):
    """
    Check whether network interface is attached

    :param network_interface: network interface id
    :return: True or False
    """
    try:
        status = get_network_interface_state(network_interface)
        return status == 'attached'
    except KeyError:
        return False


def detach_network_interface(network_interface):

    def get_attachment_id(boto_client, interface):
        response = boto_client.describe_network_interfaces(
            NetworkInterfaceIds=[
                interface
            ]
        )
        return response['NetworkInterfaces'][0]['Attachment']['AttachmentId']

    client = boto3.client('ec2')
    client.detach_network_interface(
        AttachmentId=get_attachment_id(client, network_interface)
    )


def ensure_network_interface_is_detached(network_interface):
    try:
        while get_network_interface_state(network_interface) != 'detached':
            time.sleep(1)
    except KeyError:
        pass


def attach_network_interface(network_interface, instance_id):
    client = boto3.client('ec2')
    for _ in xrange(10):
        try:
            client.attach_network_interface(
                NetworkInterfaceId=network_interface,
                InstanceId=instance_id,
                DeviceIndex=DEVICE_INDEX
            )
        except ClientError:
            time.sleep(1)


def configure_local_interface(local_interface, ip, netmask):

    cmd = [
        'ifconfig',
        local_interface,
        'inet',
        ip,
        'netmask',
        netmask
    ]
    for _ in xrange(10):
        os.environ['PATH'] = "%s:/sbin" % os.environ['PATH']
        env = os.environ
        proc = Popen(cmd, env=env)
        proc.communicate()
        if proc.returncode:
            time.sleep(1)
        else:
            return


def aws_notify_master(cfg):
    """The function moves network interface to local instance and brings it up.
    Steps:

    - Detach network interface if attached to anywhere.
    - Attach the network interface to the local instance.
    - Configure IP address on this instance

    :param cfg: config object
    """
    try:
        os.environ['AWS_ACCESS_KEY_ID'] = cfg.get('aws', 'aws_access_key_id')
        os.environ['AWS_SECRET_ACCESS_KEY'] = cfg.get('aws',
                                                      'aws_secret_access_key')
        os.environ['AWS_DEFAULT_REGION'] = cfg.get('aws', 'aws_default_region')
    except NoOptionError:
        LOG.error('aws_access_key_id, aws_secret_access_key and '
                  'aws_default_region must be defined in '
                  'aws section of the config file.')
        exit(-1)

    instance_id = get_my_instance_id()
    try:
        ip = cfg.get('proxysql', 'virtual_ip')
        netmask = cfg.get('proxysql', 'virtual_netmask')

        network_interface = get_network_interface(ip)

        if network_interface_attached(network_interface):
            detach_network_interface(network_interface)

        local_interface = "eth%d" % DEVICE_INDEX
        ensure_local_interface_is_gone(local_interface)

        ensure_network_interface_is_detached(network_interface)

        attach_network_interface(network_interface, instance_id)

        configure_local_interface(local_interface, ip, netmask)
    except NoOptionError as err:
        LOG.error('virtual_ip and virtual_netmask must be defined in '
                  'proxysql section of the config file.')
        LOG.error(err)
