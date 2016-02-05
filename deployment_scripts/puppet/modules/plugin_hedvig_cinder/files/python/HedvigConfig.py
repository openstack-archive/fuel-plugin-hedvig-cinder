from eventlet.green import threading
import socket
import os
import hedvigpyc as hc

from oslo_log import log as logging

LOG = logging.getLogger(__name__)

TO_MEGABYTES = 1024 * 1024
TO_GIGABYTES = TO_MEGABYTES * 1024

class ReplicationPolicy:
  Agnostic = 0
  RackAware = 1
  DataCenterAware = 2

  _VALUES_TO_NAMES = {
    0: "Agnostic",
    1: "RackAware",
    2: "DataCenterAware",
  }

  _NAMES_TO_VALUES = {
    "Agnostic": 0,
    "RackAware": 1,
    "DataCenterAware": 2,
  }

class DiskResidence:
  Flash = 0
  HDD = 1

  _VALUES_TO_NAMES = {
    0: "Flash",
    1: "HDD",
  }

  _NAMES_TO_VALUES = {
    "Flash": 0,
    "HDD": 1,
  }

class HedvigConfig():
    lock_ = threading.RLock()
    instance_ = None

    # Default Port Configuration
    defaultHControllerPort_ = 50000

    # Default Cinder Configuration
    defaultCinderReplicationFactor_ = 3
    defaultCinderDedupEnable_ = False
    defaultCinderCompressEnable_ = False
    defaultCinderCacheEnable_ = False
    defaultCinderDiskResidence_ = DiskResidence.HDD
    defaultCinderReplicationPolicy_ = ReplicationPolicy.Agnostic
    
    def __init__(self):
        self.hcontrollerPort_  = HedvigConfig.defaultHControllerPort_

        self.cinderReplicationFactor_ = HedvigConfig.defaultCinderReplicationFactor_
        self.cinderReplicationPolicy_ = HedvigConfig.defaultCinderReplicationPolicy_
        self.cinderDedupEnable_ = HedvigConfig.defaultCinderDedupEnable_
        self.cinderCompressEnable_ = HedvigConfig.defaultCinderCompressEnable_
        self.cinderCacheEnable_ = HedvigConfig.defaultCinderCacheEnable_
        self.cinderDiskResidence_ = HedvigConfig.defaultCinderDiskResidence_

    @classmethod
    def parseAndGetBooleanEntry(cls, entry):
        entry = entry.strip(" \t\n").lower()
        if entry == "true":
            return True
        elif entry == "false":
            return False
        else:
            return None

    @classmethod
    def parseAndGetReplicationPolicy(cls, strReplicationPolicy):
        strReplicationPolicy = strReplicationPolicy.strip(" \n\t").lower()
        if strReplicationPolicy ==  ReplicationPolicy._VALUES_TO_NAMES[ReplicationPolicy.Agnostic].lower():
            return ReplicationPolicy.Agnostic
        elif strReplicationPolicy == ReplicationPolicy._VALUES_TO_NAMES[ReplicationPolicy.RackAware].lower():
            return ReplicationPolicy.RackAware
        elif strReplicationPolicy == ReplicationPolicy._VALUES_TO_NAMES[ReplicationPolicy.DataCenterAware].lower():
            return ReplicationPolicy.DataCenterAware
        else:
            return None

    @classmethod
    def parseAndGetDiskResidence(cls, strDiskResidence):
        strDiskResidence = strDiskResidence.strip(" \n\t").lower()
        if strDiskResidence == DiskResidence._VALUES_TO_NAMES[DiskResidence.HDD].lower():
            return DiskResidence.HDD
        elif strDiskResidence == DiskResidence._VALUES_TO_NAMES[DiskResidence.Flash].lower():
            return DiskResidence.Flash
        else:
            return None

    @staticmethod
    def getInstance():
        if not HedvigConfig.instance_:
            with HedvigConfig.lock_:
                if not HedvigConfig.instance_:
                    try:
                        HedvigConfig.instance_ = HedvigConfig()
                        LOG.info(_LI('HedvigConfig initialized'))
                    except Exception:
                        LOG.error(_LE('Failed to initialize HedvigConfig'))
                        raise
        return HedvigConfig.instance_

    def getHedvigControllerPort(self):
        return self.hcontrollerPort_;

    def getCinderReplicationFactor(self):
        return self.cinderReplicationFactor_

    def getCinderReplicationPolicy(self):
        return self.cinderReplicationPolicy_

    def isCinderDedupEnabled(self):
        return self.cinderDedupEnable_

    def isCinderCacheEnabled(self):
        return self.cinderCacheEnable_

    def isCinderCompressEnabled(self):
        return self.cinderCompressEnable_

    def getCinderDiskResidence(self):
        return self.cinderDiskResidence_
