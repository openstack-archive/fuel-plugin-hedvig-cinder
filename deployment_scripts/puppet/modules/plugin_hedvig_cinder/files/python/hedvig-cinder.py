#!/usr/bin/python
# vim: tabstop=4 shiftwidth=4 softtabstop=4
# Copyright (c) 2012 Hedvig, Inc.
# Copyright (c) 2012 OpenStack LLC.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Volume driver for Hedvig Block Storage.
"""

import netifaces
import os
import socket
import subprocess
import sys
import time

from cinder import exception
from cinder import context
from cinder import utils
from cinder.db.sqlalchemy import api
from cinder.i18n import _, _LE, _LI, _LW
from cinder.volume import driver
from cinder.volume import volume_types
from cinder.volume import qos_specs

from oslo.config import cfg
from oslo_concurrency import processutils
from oslo_log import log as logging

from urlparse import urlparse
from cinder.volume.drivers.hedvig.HedvigConfig import HedvigConfig
from cinder.volume.drivers.hedvig.HedvigOpCb import *
import hedvigpyc as hc

LOG = logging.getLogger(__name__)

hedvig_opts = [
    cfg.StrOpt('hedvig_metadata_server',
               help='The hostname (or IP address) of a hedvig metadata node in the cluster'),
]

CONF = cfg.CONF
CONF.register_opts(hedvig_opts)

class HedvigISCSIDriver(driver.ISCSIDriver):
    """Hedvig iSCSI volume driver."""
    defaultVolumeSizeInGB_ = 1
    defaultVolumeBlkSize_ = 4096
    toGB_ = 1024 * 1024 * 1024
    defaultCreatedBy_ = "OpenStack Cinder"
    defaultExportedBlkSize_ = 512

    def __init__(self, *args, **kwargs):
        super(HedvigISCSIDriver, self).__init__(*args, **kwargs)
        self.group_stats = {}
        self.configuration.append_config_values(hedvig_opts)
    
    def check_for_setup_error(self):
        pass

    def do_setup(self, context):
        self.context_ = context
        LOG.debug('Context: %s', context)
        self.hedvig_metadata_server = getattr(self.configuration, 'hedvig_metadata_server')
        LOG.info(_LI('Initializing hedvig cinder driver with server - %s'), self.hedvig_metadata_server)
        HedvigOpCb.hedvigpycInit(self.hedvig_metadata_server, 8)

    def get_volume_stats(self, refresh=False):
        # we need to get get stats for server.
        total_capacity = float(1024)
        used_capacity = float(0)
        free_capacity = total_capacity - used_capacity
        self.group_stats = {"volume_backend_name": "hedvig",
                            "vendor_name" : "Hedvig Inc",
                            "driver_version" : "0.99",
                            "storage_protocol" : "ISCSI",
                            "total_capacity_gb": total_capacity,
                            "free_capacity_gb": free_capacity,
                            "reserved_percentage": 0,
                            "QoS_support": True}
        return self.group_stats

    def hedvigGetVolumeDetails(self, volume):
        volName = volume['name']
        project = volume['project_id']
        displayName = volume['display_name']
        displayDescription = volume['display_description']
        if project == None:
            project = ""
        if displayName == None:
            displayName = ""
        if displayDescription == None:
            displayDescription = ""
        description = project + "\n" + displayName + "\n" + displayDescription
        size = long(volume['size'])
        if size == 0:
            size = HedvigISCSIDriver.defaultVolumeSizeInGB_
        size = size * HedvigISCSIDriver.toGB_
        return volName, description, size

    def create_volume(self, volume):
        """
        Driver entry point for creating a new volume.
        """
        LOG.debug("Creating new volume: %s", volume)
        name, description, size = self.hedvigGetVolumeDetails(volume)
        volumeTypeId = volume["volume_type_id"]
        volumeType = None
	try:
            LOG.debug("Retrieving volume type details for id: %s", volumeTypeId)
            volumeType = api.volume_type_get(self.context_, volumeTypeId)
            LOG.debug("Volume type is: %s", volumeType)
        except Exception as e:
            LOG.error(_LE('Failed to retrieve volume type details for id: %s'), volumeTypeId)
            raise exception.HedvigDriverException()
        try:
            LOG.debug("Volume name: %s, description: %s, size: %s", name, description, size)
            self.hedvigCreateVirtualDisk(name, description, size, volumeType)
        except Exception as e:
            LOG.error(_LE('Failed to create volume. name: %s, description: %s, size: %s'), name, description, size)
            raise exception.HedvigDriverException()

    def delete_volume(self, volume):
        """
        Driver entry point for deleting volume.
        """
        LOG.debug("Deleting volume: %s", volume)
        name = volume['name']
        try:
            self.hedvigDeleteVirtualDisk(name)
        except Exception as e:
            LOG.error(_LE('Failed to delete volume. volume: %s'), volume)
            raise exception.HedvigDriverException()

    def get_export(self, volume):
        """
        Get the iSCSI export details for a volume.
        """
        LOG.debug("volume: %s", volume)
        pass

    def ensure_export(self, context, volume):
        """
        Driver entry point to get the export info for an existing volume.
        Irrelevant for Hedvig. Export is created during attachment to instance.
        """
        LOG.debug("context: %s, volume: %s", context, volume)
        pass

    def create_export(self, context, volume):
        """
        Driver entry point to get the export info for a new volume.
        Irrelevant for Hedvig. Export is created during attachment to instance.
        """
        LOG.debug("context: %s, volume: %s", context, volume)
        pass

    def remove_export(self, context, volume):
        """
        Driver entry point to remove an export for a volume.
        Irrelevant for Hedvig. Export should be deleted on detachment.
        """
        LOG.debug("context: %s, volume: %s", context, volume)
        pass

    def initialize_connection(self, volume, connector):
        """
        Driver entry point to attach a volume to an instance.

        Assign any created volume to a compute node/controllerVM so that it can be
        attached to a instance.
        This driver returns a driver_volume_type of 'iscsi'.
        The format of the driver data is defined as follows -- similar to _get_iscsi_properties.
        Example return value:

            {
                'driver_volume_type': 'iscsi'
                'data': {
                    'target_discovered': True,
                    'target_iqn': 'iqn.2010-10.com.hedvig:target',
                    'target_portal': '192.168.108.143:3260',
                    'target_lun': '1',
                    'volume_id': 2,
                }
            }

        """
        LOG.info(_LI('Initializing connection. volume: %(volume)s, connector: %(connector)s'), {'volume': volume, 'connector': connector})
        try:
            computeHost = socket.getfqdn(connector['host'])
            volName = volume['name']
            tgtHost = self.hedvigLookupTgt(computeHost)
            lunnum = self.hedvigGetLun(tgtHost, volName)
            if lunnum == -1:
                LOG.error(_LE('Failed to get lun for volume: %(volume)s, hedvig controller: %(tgtHost)s'), {'volume': volume, 'tgtHost': tgtHost})
                raise Exception("Failed to add lun")

            # Addaccess to the mgmt interface addr and iqn of the compute host
            compute_host_iqn = self.hedvigGetIqn(computeHost)
            self.hedvigAddAccess(tgtHost, lunnum, compute_host_iqn)
            compute_mgmt_addr = socket.gethostbyname(socket.getfqdn(computeHost))
            self.hedvigAddAccess(tgtHost, lunnum, socket.gethostbyname(socket.getfqdn(computeHost)))

            # Addaccess to both storage and mgmt interface addrs for
            # iscsi discovery to succeed
            interfaces = netifaces.interfaces()
            storage_addr = None
            mgmt_addr = None
            if 'br-storage' in interfaces and netifaces.AF_INET in netifaces.ifaddresses('br-storage'):
                storage_interface = netifaces.ifaddresses('br-storage')[netifaces.AF_INET]
                if storage_interface and 'addr' in storage_interface[0]:
                    storage_addr = storage_interface[0]['addr']
            if 'br-mgmt' in interfaces and netifaces.AF_INET in netifaces.ifaddresses('br-mgmt'):
                mgmt_interface = netifaces.ifaddresses('br-mgmt')[netifaces.AF_INET]
                if mgmt_interface and 'addr' in mgmt_interface[0]:
                    mgmt_addr = mgmt_interface[0]['addr']
            if mgmt_addr and (compute_mgmt_addr != mgmt_addr):
                self.hedvigAddAccess(tgtHost, lunnum, mgmt_addr)
            if storage_addr and mgmt_addr and (storage_addr != mgmt_addr):
                self.hedvigAddAccess(tgtHost, lunnum, storage_addr)

            targetName, portal = self.hedvigDoIscsiDiscovery(tgtHost, lunnum)
            iscsi_properties = ({'target_discovered': True,
                                 'target_iqn': targetName,
                                 'target_portal': portal,
                                 'target_lun': lunnum })
            LOG.debug("volName: %s, connector: %s, iscsi_properties: %s", volName, connector, iscsi_properties)
            return {'driver_volume_type': 'iscsi', 'data': iscsi_properties}
        except Exception as e:
            LOG.error(_LE('Volume assignment to connect failed. volume: %(volume)s, connector: %(connector)s'), {'volume': volume, 'connector': connector})
            raise exception.HedvigDriverException()

    def terminate_connection(self, volume, connector, **kwargs):
        """
        Driver entry point to detach volume from instance.
        """
        LOG.debug("Terminating connection. volume: %s, connector: %s", volume, connector)
        try:
            computeHost = socket.getfqdn(connector['host']) #connector['host']
            volName = volume['name']
            tgtHost = self.hedvigLookupTgt(computeHost)
            if tgtHost == '':
                LOG.error(_LE('Failed to get hedvig controller for compute host: %s'), computeHost)
                raise Exception("Failed to get hedvig controller for compute host: %s" % (computeHost))
            lunnum = self.hedvigGetLun(tgtHost, volName)
            if lunnum == -1:
                LOG.error(_LE('Failed to get lun for volume: %s'), volume)
                raise Exception("Failed to get lun for volume: %s" % (volume))
            self.hedvigDeleteLun(tgtHost, volName, lunnum)
        except Exception as e:
            LOG.error(_LE('Failed to terminate connection. volume: %(volume)s, connector: %(connector)s'), {'volume': volume, 'connector': connector})
            raise exception.HedvigDriverException()

    def hedvigLookupTgt(self, host):
        """
        Get the tgt instance associated with the compute host.
        """
        try:
            LOG.debug("Looking up hedvig controller for compute host: %s", host)
            hedvigGetTgtInstanceOp = HGetTgtInstanceOpCb(host)
            tgtMap = hedvigGetTgtInstanceOp.getTgtInstance()
            tgt = None
            if "block" in tgtMap:
                tgtList = tgtMap["block"]
                if len(tgtList) > 0:
                    tgt = tgtList[0]
            if not tgt:
                raise exception.NotFound(name=host)
            LOG.debug("Found hedvig controller: %s, for host: %s", tgt, host)
            return tgt
        except Exception as e:
            LOG.error(_LE('Failed to get hedvig controller for compute host: %s'), host)
            raise e

    def hedvigDoIscsiLogout(self, targetName, portal, toThrow=False):
        try:
            (retOut, retErr) = utils.execute("iscsiadm", "--mode", "node", "--targetname", targetName, "--portal", portal, "--logout", run_as_root=True)
            if retErr != None and len(retErr) > 0:
                LOG.error(_LE('Failed iscsi logout. targetName: %(targetName)s, portal: %(portal)s'), {'targetName': targetName, 'portal': portal})
        except processutils.ProcessExecutionError as e:
                LOG.error(_LE('Error occurred during iscsi logout. targetName: %(targetName)s, portal: %(portal)s, stdout: %(stdout)s, stderr: %(stderr)s, exit_code: %(exit_code)s'), {'targetName': targetName, 'portal': portal, 'stdout': e.stdout, 'stderr': e.stderr, 'exit_code': e.exit_code})
                if toThrow:
                    raise e

    def hedvigDoIscsiDiscovery(self, tgtHost, lunNum):
        LOG.debug("Do iscsi discovery, hedvig controller: %s, lun: %s", tgtHost, lunNum)
        try:
            (retOut , retErr) = utils.execute("iscsiadm", "-m" , "discovery", "-t", "sendtargets", "-p", tgtHost, run_as_root=True)
            LOG.debug("Done iscsi discovery, hedvig controller: %s, lun: %s, output: %s, error: %s", tgtHost, lunNum, retOut, retErr)
            if retErr != None and len(retErr) > 0:
                # logout not in use targets
                for line in retErr.rstrip(" ").split("\n"):
                    if "target:" not in line:
                        continue
                    line = line[line.find("target:") + len("target:") :]
                    line = line[:line.find("],")]
                    targetName, portal = line.split("portal:")
                    targetName = targetName.strip(" ").strip(",")
                    portal = portal.strip(" ").strip(",")
                    self.hedvigDoIscsiLogout(targetName, portal)
            targetName = None
            portal = None
            if retOut != None and len(retOut) > 0:
                expectedTarget = tgtHost + "-" + str(lunNum)
                for line in retOut.strip(" ").split("\n"):
                    if expectedTarget in line:
                        portal, targetName = line.strip(" ").split(" ")
                        portal = portal.strip(" ").split(",")[0]
                        targetName = targetName.strip(" ")
                        break
        except processutils.ProcessExecutionError as e:
            LOG.error(_LE('Error occurred during iscsi discovery. stdout: %(stdout)s, stderr: %(stderr)s, exit_code: %(exit_code)s'), {'stdout': e.stdout, 'stderr': e.stderr, 'exit_code': e.exit_code})
            raise e
        if targetName == None or portal == None:
            raise Exception("Failed iscsi discovery. hedvig controller: %s, lun: %s" % (tgtHost, lunNum))
        return targetName, portal

    def hedvigDeleteLun(self, tgtHost, vDiskName, lunnum):
        LOG.debug("Deleting lun. hedvig controller: %s, vDiskName: %s, lun: %s", tgtHost, vDiskName, lunnum)
        tgtPort = HedvigConfig.getInstance().getHedvigControllerPort()
        hedvigDeleteLunOp = HDeleteLunOpCb(tgtHost, tgtPort, vDiskName, lunnum)
        result = hedvigDeleteLunOp.deleteLun()    
        return result

    def hedvigGetLun(self, tgtHost, vDiskName):
        """
        Looks up lun based on tgthost and vDiskName.
        If lun does not exist then call addLun and return the lun number.
        If lun exists, just return the lun number.
        """
        LOG.debug("Getting lun. hedvig controller: %s, vDiskName: %s", tgtHost, vDiskName)
        tgtPort = HedvigConfig.getInstance().getHedvigControllerPort()
        hedvigGetLunOp = HGetLunCinderOpCb(tgtHost, tgtPort, vDiskName)
        lunNo = hedvigGetLunOp.getLunCinder()
        if lunNo > -1:
            LOG.debug("Found lun: %s", str(lunNo))
            return lunNo
       
        # If the lun is not found, add lun for the vdisk
        hedvigDescribeVirtualDiskOp = HDescribeVirtualDiskOpCb(vDiskName)
        vDiskInfo = hedvigDescribeVirtualDiskOp.describeVirtualDisk()
        tgtPort = HedvigConfig.getInstance().getHedvigControllerPort()
        hedvigAddLunOp = HAddLunOpCb(tgtHost, tgtPort, vDiskInfo)
        lunNo = hedvigAddLunOp.addLun()
        LOG.debug("Found lun: %s", str(lunNo))
	return lunNo

    def hedvigGetIqn(self, hostname):
        """
        Looks up the iqn for the given host.
        """
        LOG.debug("Getting IQN for hostname: %s", hostname)
        hedvigGetIqnOp = HGetIqnOpCb(hostname)
        iqn = hedvigGetIqnOp.getIqn()
	LOG.debug("Got IQN: %s, for hostname: %s", iqn, hostname)
        return iqn
       
    def hedvigAddAccess(self, tgtHost, lun, initiator):
        """
        Adds access to LUN for initiator's ip/iqn
        """
        LOG.debug("Adding access. hedvig controller: %s, lun: %s, initiator: %s", tgtHost, lun, initiator)
        try:
            tgtPort = HedvigConfig.getInstance().getHedvigControllerPort()
            hedvigAddAccessOp = HAddAccessOpCb(tgtHost, tgtPort, lun, initiator)
            result = hedvigAddAccessOp.addAccess()
            return result
        except Exception as e:
            LOG.error(_LE('Failed to add access. hedvig controller: %(controller)s, lun: %(lun)s, initiator: %(initiator)s'), {'controller': tgtHost, 'lun': lun, 'initiator': initiator})
            raise e

    def create_snapshot(self, snapshot):
        """
        Driver entry point for creating a snapshot.
        """
        try:
            ctxt = context.get_admin_context()
            volName = snapshot['volume_name']
            snapshotName = snapshot['name']
            project = snapshot['project_id']
            snapshotId = snapshot['id']
            LOG.debug("Creating snapshot. volName: %s, snapshotName: %s, project: %s, snapshotId: %s", volName, snapshotName, project, snapshotId)
            hedvigSnapshotName = self.hedvigCreateSnapshot(volName)
            # Set the snapshot name to ret value.
            LOG.debug("Updating snapshot name. snapshotId: %s, snapshotName: %s, newName: %s", snapshotId, snapshotName, hedvigSnapshotName)
            self.db.snapshot_update(ctxt, snapshotId, {'display_name': str(hedvigSnapshotName)})
        except Exception as e:
            LOG.error(_LE('Failed to create snapshot. volName: %(volName)s, snapshotName: %(snapshotName)s, project: %(project)s, snapshotId: %(snapshotId)s'), {'volName': volName, 'snapshotName': snapshotName, 'project': project, 'snapshotId': snapshotId})
            raise exception.HedvigDriverException()

    def detach_volume(self, context, volume, attachment):
        LOG.info(_LI('Detaching volume. context: %(context)s, volume: %(volume)s, attachment: %(attachment)s'), {'context': context, 'volume': volume, 'attachment': attachment})
        return

    def hedvigCreateSnapshot(self, vDiskName):
        """
        Hedvig call to create snapshot of vdisk.
        """
        hedvigDescribeVirtualDiskOp = HDescribeVirtualDiskOpCb(vDiskName)
        vDiskInfo = hedvigDescribeVirtualDiskOp.describeVirtualDisk()
        snapshotName = None
	LOG.debug("Creating snapshot. mount location: %s", vDiskInfo.mntLocation)
        if vDiskInfo.mntLocation == "":
            LOG.debug("vDisk: %s is not mounted", vDiskName)
            hedvigSnapshotOp = HSnapshotOpCb(vDiskName)
            snapInfo = hedvigSnapshotOp.snapshot()
            snapshotName = snapInfo.snapshot
        else:
            tgtPort = HedvigConfig.getInstance().getHedvigControllerPort()
            hedvigSnapshotFromTgtOp = HSnapshotFromTgtOpCb(tgtHost, tgtPort, vDiskName)
            tgtHost = vDiskInfo.mntLocation.split(":")[0]
            LOG.debug("vDisk: %s is mounted on tgtHost: %s", vDiskName, tgtHost)
            snapshotName = hedvigSnapshotFromTgtOp.snapshotFromtgt()
        return snapshotName

    def delete_snapshot(self, snapshot):
        """
        Driver entry point for deleting a snapshot.
        """
        volName = snapshot['volume_name']
        snapshotName = snapshot['display_name']
        project = snapshot['project_id']
        LOG.debug("Deleting snapshot. volName: %s, snapshotName: %s, project: %s", volName, snapshotName, project)
        try:
            hedvigDeleteSnapshotOp = HDeleteSnapshotOpCb(snapshotName)
            hedvigDeleteSnapshotOp.deleteSnapshot()
        except Exception as e:
            LOG.error(_LE('Failed to delete snapshot. volName: %(volName)s, snapshotName: %(snapshotName)s, project: %(project)s'), {'volName': volName, 'snapshotName': snapshotName, 'project': project})
            raise exception.HedvigDriverException()

    def create_volume_from_snapshot(self, volume, snapshot):
        """
        Driver entry point for creating a new volume from a snapshot.
        This is the same as cloning.
        """
        name, description, size = self.hedvigGetVolumeDetails(volume)
        snapshotName = snapshot['display_name']
        volumeTypeId = volume["volume_type_id"]
        volumeType = None
        try:
            LOG.debug("Retrieving volume type details for id: %s", volumeTypeId)
            volumeType = api.volume_type_get(self.context_, volumeTypeId)
            LOG.debug("Volume type is: %s", volumeType)
        except Exception as e:
            LOG.error(_LE('Failed to retrieve volume type details for id: %s'), volumeTypeId)
            raise exception.HedvigDriverException()
	try:
            LOG.info(_LI('Creating volume from snapshot. snapshotName: %s, name: %s, description: %s, size: %s'), snapshotName, name, description, size)
	    self.hedvigCloneSnapshot(snapshotName, name, description, size, volumeType)
        except Exception as e:
            # LOG.exception(_LE('Failed to create volume from snapshot. snapshotName: %(snapshotName)s, name: %(name)s, description: %(description)s, size: %(size)s'), {'snapshotName': snapshotName, 'name': name, 'description': description, 'size': size})
            raise exception.HedvigDriverException()

    def check_for_export(self, context, volume_id):
        """
        Not relevant to Hedvig
        """
        pass

    def extend_volume(self, volume, newSize):
        """
        Resizes virtual disk.
        newSize should be greater than current size.
        """
        name, description, size = self.hedvigGetVolumeDetails(volume)
        newSize = int(newSize) * HedvigISCSIDriver.toGB_
        LOG.info(_LI('Resizing virtual disk. name: %(name)s, newSize: %(newSize)s'), {'name': name, 'newSize': newSize})
        try:
            hedvigDescribeVirtualDiskOp = HDescribeVirtualDiskOpCb(name)
            vDiskInfo = hedvigDescribeVirtualDiskOp.describeVirtualDisk()
            if vDiskInfo.size > newSize:
                LOG.error(_LE('Cannot reduce virtual disk size. currentSize: %(currentSize)s, newSize: %(newSize)s'), {'currentSize': vDiskInfo.size, 'newSize': newSize})
                raise Exception("Cannot reduce virtual disk size. currentSize: %s, newSize: %s" % (vDiskInfo.size, newSize))
            size = vDiskInfo.size
            vDiskInfo.size = newSize
            hedvigResizeVirtualDiskOp = HResizeVirtualDiskOpCb(vDiskInfo)
            hedvigResizeVirtualDiskOp.resizeVirtualDisk()
        except Exception as e:
            LOG.error(_LE('Cannot reduce virtual disk size. currentSize: %(currentSize)s, newSize: %(newSize)s'), {'currentSize': size, 'newSize': newSize})
            raise exception.HedvigDriverException()

    def hedvigCreateVirtualDisk(self, name, description, size, volumeType):
        LOG.info(_LI('Creating virtual disk. name: %(name)s, description: %(description)s, size: %(size)s'), {'name': name, 'description': description, 'size': size})
        vDiskInfo = hc.VDiskInfo()
        vDiskInfo.vDiskName = str(name)
        vDiskInfo.blockSize = HedvigISCSIDriver.defaultVolumeBlkSize_
        vDiskInfo.size = size
        vDiskInfo.createdBy = str(HedvigISCSIDriver.defaultCreatedBy_)
        vDiskInfo.description = str(description)
        vDiskInfo.residence = HedvigConfig.getInstance().getCinderDiskResidence()
        vDiskInfo.replicationFactor = HedvigConfig.getInstance().getCinderReplicationFactor()
        vDiskInfo.replicationPolicy = HedvigConfig.getInstance().getCinderReplicationPolicy()
        vDiskInfo.clusteredfilesystem = False
        vDiskInfo.exportedBlockSize = HedvigISCSIDriver.defaultExportedBlkSize_
        vDiskInfo.cacheEnable = HedvigConfig.getInstance().isCinderCacheEnabled()
        vDiskInfo.diskType = hc.DiskType.BLOCK
        vDiskInfo.immutable = False
        vDiskInfo.dedup = HedvigConfig.getInstance().isCinderDedupEnabled()
        vDiskInfo.compressed = HedvigConfig.getInstance().isCinderCompressEnabled()
        vDiskInfo.cloudEnabled = False
        vDiskInfo.cloudProvider = 0
        vDiskInfo.isClone = False
        vDiskInfo.consistency = hc.Consistency.STRONG
        if "extra_specs" in volumeType:
	    qos = {}
	    qos_specs_id = volumeType.get('qos_specs_id')
            specs = volumeType.get('extra_specs')
	    if qos_specs_id is not None:
            	kvs = qos_specs.get_qos_specs(self.context_, qos_specs_id)['specs']	
	    	for key, value in kvs.items():
		    if "dedup_enable" in key:
		        val = HedvigConfig.parseAndGetBooleanEntry(value)
		        if val != None:
                    	    vDiskInfo.dedup = val
	    	    if "compressed_enable" in key:
                        val = HedvigConfig.parseAndGetBooleanEntry(value)
                        if val != None:
                            vDiskInfo.compressed = True
	    	    if "cache_enable" in key:
                        val = HedvigConfig.parseAndGetBooleanEntry(value)
                        if val != None:
                            vDiskInfo.cacheEnable = val
		    if "replication_factor" in key:
                        val = int(value)
                        if val > 0:
                            vDiskInfo.replicationFactor = val
		    if "replication_policy" in key:
                        val = HedvigConfig.parseAndGetReplicationPolicy(value)
                        if val != None:
                            vDiskInfo.replicationPolicy = val
		    if "disk_residence" in key:
                        val = HedvigConfig.parseAndGetDiskResidence(value)
                        if val != None:
                            vDiskInfo.residence = val
        	    if "replication_policy_info" in key:
			val = value.split(',')
			if len(val) != 0:
			    vDiskInfo.replicationPolicyInfo = hc.ReplicationPolicyInfo()
			    vDiskInfo.replicationPolicyInfo.dataCenterNames = hc.buffer_vector()
			    for dataCenter in val:
				vDiskInfo.replicationPolicyInfo.dataCenterNames.append(str(dataCenter))
	hedvigCreateVirtualDiskOp = HCreateVirtualDiskOpCb(vDiskInfo)
        hedvigCreateVirtualDiskOp.createVirtualDisk()

    def hedvigDeleteVirtualDisk(self, name):
        LOG.info(_LI('Deleting virtual disk. name - %s'), name)
        hedvigDeleteVirtualDiskOp = HDeleteVirtualDiskOpCb(name)
        hedvigDeleteVirtualDiskOp.deleteVirtualDisk()

    def hedvigCloneSnapshot(self, snapshotName, name, description, size, volumeType):
        LOG.debug("Cloning a snapshot. snapshotName: %s, name: %s, description: %s, size: %s", snapshotName, name, description, size)
        LOG.info(_LI('Cloning a snapshot. snapshotName: %(snapshotName)s, name: %(name)s, description: %(description)s, size: %(size)s'), {'snapshotName': snapshotName, 'name': name, 'description': description, 'size': size})
	vDiskInfo = hc.VDiskInfo()
        cloneInfo = hc.CloneInfo()
        baseDisk = snapshotName.split("$")[0]
        baseVersion = snapshotName.split("$")[2]
	hedvigDescribeVirtualDiskOp = HDescribeVirtualDiskOpCb(baseDisk)
        parentVDiskInfo = hedvigDescribeVirtualDiskOp.describeVirtualDisk()
        vDiskInfo.vDiskName = str(name)
        cloneInfo.typeOfClone = hc.TypeOfClone.Shallow
        vDiskInfo.clusteredfilesystem = parentVDiskInfo.clusteredfilesystem
        vDiskInfo.blockSize = parentVDiskInfo.blockSize
        vDiskInfo.exportedBlockSize = parentVDiskInfo.exportedBlockSize
	if parentVDiskInfo.size > size:
            LOG.error(_LE('Failed to clone snapshot. Size of the clone: %(cloneSize)s is less than the parent virtual disk size: %(parentSize)s'), {'cloneSize': size, 'parentSize': parentVDiskInfo.size})
            raise Exception("Specified size: %s is less than parent virtual disk size: %s" % (size, parentVDiskInfo.size))
        vDiskInfo.size = size
        vDiskInfo.createdBy = parentVDiskInfo.createdBy
        vDiskInfo.description = str(description)
        vDiskInfo.residence = parentVDiskInfo.residence
        vDiskInfo.replicationFactor = parentVDiskInfo.replicationFactor
        vDiskInfo.replicationPolicy = parentVDiskInfo.replicationPolicy
        cloneInfo.snapshot = str(snapshotName)
        cloneInfo.baseVersion = int(baseVersion)
        cloneInfo.baseDisk = str(baseDisk)
        vDiskInfo.cloneInfo = cloneInfo
        vDiskInfo.isClone = True
        vDiskInfo.scsisn = 0
        vDiskInfo.cacheEnable = parentVDiskInfo.cacheEnable
        vDiskInfo.diskType = parentVDiskInfo.diskType
        vDiskInfo.cloudProvider = parentVDiskInfo.cloudProvider
        vDiskInfo.cloudEnabled = parentVDiskInfo.cloudEnabled
        vDiskInfo.dedup = parentVDiskInfo.dedup
        vDiskInfo.consistency = hc.Consistency.STRONG
        if "extra_specs" in volumeType:
            qos = {}
            qos_specs_id = volumeType.get('qos_specs_id')
            specs = volumeType.get('extra_specs')  
            if qos_specs_id is not None:
                kvs = qos_specs.get_qos_specs(self.context_, qos_specs_id)['specs']
                for key, value in kvs.items():
                    if "dedup_enable" in key:
                        val = HedvigConfig.parseAndGetBooleanEntry(value)
                        if val != None:
                            vDiskInfo.dedup = val
                    if "compressed_enable" in key:
                        val = HedvigConfig.parseAndGetBooleanEntry(value)
                        if val != None:
                            vDiskInfo.compressed = True
                    if "cache_enable" in key:
                        val = HedvigConfig.parseAndGetBooleanEntry(value)
                        if val != None:
                            vDiskInfo.cacheEnable = val
                    if "replication_factor" in key:
                        val = int(value)
                        if val > 0:
                            vDiskInfo.replicationFactor = val
                    if "replication_policy" in key:
                        val = HedvigConfig.parseAndGetReplicationPolicy(value)
                        if val != None:
                            vDiskInfo.replicationPolicy = val
                    if "disk_residence" in key:
                        val = HedvigConfig.parseAndGetDiskResidence(value)
                        if val != None:
                            vDiskInfo.residence = val
                    if "replication_policy_info" in key:
                        val = value.split(',')
                        if len(val) != 0:
                            vDiskInfo.replicationPolicyInfo = hc.ReplicationPolicyInfo()
                            vDiskInfo.replicationPolicyInfo.dataCenterNames = hc.buffer_vector()
                            for dataCenter in val:
                                vDiskInfo.replicationPolicyInfo.dataCenterNames.append(str(dataCenter))
	hedvigCreateVirtualDiskOp = HCreateVirtualDiskOpCb(vDiskInfo)
        hedvigCreateVirtualDiskOp.createVirtualDisk()

