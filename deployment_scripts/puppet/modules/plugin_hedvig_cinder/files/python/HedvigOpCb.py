from eventlet.green import os
import eventlet
import eventlet.queue
import os, fcntl
import hedvigpyc as hc
import time

from oslo_log import log as logging

LOG = logging.getLogger(__name__)

class HedvigPipe():

    pipeQ_ = eventlet.queue.Queue()

    def __init__(self):
        rfd, wfd = os.pipe()
        self.rfd_ = rfd
        self.wfd_ = wfd

    def __del__(self):
        fds = [self.wfd_, self.rfd_]
        for fd in fds:
            try:
                os.close(fd)
            except:
                pass

    @classmethod
    def get(cls):
        try:
            entry = HedvigPipe.pipeQ_.get(False)
            HedvigPipe.pipeQ_.task_done()
        except:
            entry = HedvigPipe()
        return entry

    @classmethod
    def put(cls, hedvigPipe):
        HedvigPipe.pipeQ_.put(hedvigPipe)

class HedvigOpCb():
    def __init__(self):
        self.startTime_ = time.time()
        self.hedvigPipe_ = HedvigPipe.get()
        self.rfd_ = self.hedvigPipe_.rfd_
        self.wfd_ = self.hedvigPipe_.wfd_
        fcntl.fcntl(self.rfd_, fcntl.F_SETFL, os.O_NONBLOCK)
        self.id_ = Helper.getId()
        self.pyCallback_ = hc.ObjWithPyCallback()
        self.pyCallback_.setCallback(self)
        self.pyCallback_.wfd_ = self.wfd_

    def __str__(self):
        return (("HedvigBaseCb:id:%s:rfd:%s:wfd:%s") %(self.id_, self.rfd_, self.wfd_))

    def __call__(self, x):
        LOG.debug("Callback received: %s", self)

    def closeFds(self):
        if not (self.wfd_ or self.rfd_):
            return
        fds = [self.wfd_, self.rfd_]
        self.wfd_ = None
        self.rfd_ = None
        for fd in fds:
            if fd:
                try:
                    os.close(fd)
                except:
                    pass
        LOG.debug("Closed Fds: %s", self)

    def waitForResult(self, instance):
        LOG.debug("Waiting for result: %s", self)
        eventlet.hubs.trampoline(self.rfd_, read= True)
        LOG.debug("Received notification for result: %s", self)
        os.read(self.rfd_, 1024)
        HedvigPipe.pipeQ_.put(self.hedvigPipe_)
        self.endTime_ = time.time()

    def __del__(self, instance):
        LOG.debug("Deleting object: %s", self)

    @classmethod
    def hedvigpycInit(cls, pagesSeedMap, workerThreadCount):
	hc.hedvigpycInit(pagesSeedMap, workerThreadCount)

    @classmethod
    def hedvigpycShutdown(cls):
        hc.hedvigpycShutdown()

class HGetTgtInstanceOpCb(HedvigOpCb):

    def __init__(self,host):
        HedvigOpCb.__init__(self)
        self.op_ = hc.HGetTgtInstanceOp()
        self.op_.host_ = str(host)

    def getTgtInstance(self):
        self.pyCallback_.getTgtInstance(self.op_)
        self.waitForResult(self.__class__.__name__)
        return self.op_.tgtMap_

    def __del__(self):
        HedvigOpCb.__del__(self)

class HDescribeVirtualDiskOpCb(HedvigOpCb):

    def __init__(self,vDiskName):
        HedvigOpCb.__init__(self)
        self.op_ = hc.HDescribeVirtualDiskOp()
        self.op_.vDiskName_ = str(vDiskName)

    def describeVirtualDisk(self):
        self.pyCallback_.describeVirtualDisk(self.op_)
        self.waitForResult(self.__class__.__name__)
        return self.op_.vDiskInfo_

    def __del__(self):
        HedvigOpCb.__del__(self)

class HGetLunCinderOpCb(HedvigOpCb):  
  
    def __init__(self,tgtHost,tgtPort,vDiskName):  
        HedvigOpCb.__init__(self)  
        self.op_ = hc.HGetLunCinderOp()  
        self.op_.tgtHost_ = str(tgtHost)
        self.op_.tgtPort_ = tgtPort
        self.op_.vDiskName_ = str(vDiskName)  
  
    def getLunCinder(self):  
        self.pyCallback_.getLunCinder(self.op_)  
        self.waitForResult(self.__class__.__name__)  
        return self.op_.lunNumber_  
    
    def __del__(self):  
        HedvigOpCb.__del__(self)

class HAddLunOpCb(HedvigOpCb):
   
    def __init__(self,tgtHost,tgtPort,vDiskInfo):
        HedvigOpCb.__init__(self)
        LOG.debug("Addlun for virtual disk: %s", vDiskInfo.vDiskName)
        self.op_ = hc.HAddLunOp()
        self.op_.tgtHost_ = str(tgtHost)
        self.op_.tgtPort_ = tgtPort
        self.op_.vDiskInfo_ = hc.VDiskInfo()
	self.op_.vDiskInfo_.dedup = vDiskInfo.dedup
    	self.op_.vDiskInfo_.vDiskName = str(vDiskInfo.vDiskName)
    	self.op_.vDiskInfo_.createdBy = str(vDiskInfo.createdBy)
    	self.op_.vDiskInfo_.size = vDiskInfo.size
    	self.op_.vDiskInfo_.replicationFactor = vDiskInfo.replicationFactor
    	self.op_.vDiskInfo_.blockSize = vDiskInfo.blockSize
    	self.op_.vDiskInfo_.exportedBlockSize = vDiskInfo.exportedBlockSize
    	self.op_.vDiskInfo_.isClone = vDiskInfo.isClone
    	#self.op_.vDiskInfo_.generationNbr = vDiskInfo.generationNbr
    	self.op_.vDiskInfo_.cloudEnabled = vDiskInfo.cloudEnabled
    	self.op_.vDiskInfo_.dedupBuckets = str(vDiskInfo.dedupBuckets)
    	self.op_.vDiskInfo_.masterVDiskName = str(vDiskInfo.masterVDiskName)
    	self.op_.vDiskInfo_.dedup = vDiskInfo.dedup
        self.op_.vDiskInfo_.compressed = vDiskInfo.compressed
    	self.op_.vDiskInfo_.immutable = vDiskInfo.immutable
        self.op_.vDiskInfo_.cacheEnable = vDiskInfo.cacheEnable
    	self.op_.vDiskInfo_.scsisn = vDiskInfo.scsisn
        self.op_.vDiskInfo_.vTreeBuffer = str(vDiskInfo.vTreeBuffer)
    	#self.op_.vDiskInfo_.versionCounter = vDiskInfo.versionCounter
        self.op_.vDiskInfo_.clusteredfilesystem = vDiskInfo.clusteredfilesystem
    	self.op_.vDiskInfo_.description = str(vDiskInfo.description)
        self.op_.vDiskInfo_.mntLocation = str(vDiskInfo.mntLocation)
        self.op_.vDiskInfo_.residence = vDiskInfo.residence
        self.op_.vDiskInfo_.diskType = vDiskInfo.diskType
        self.op_.vDiskInfo_.cloudProvider = vDiskInfo.cloudProvider
    	self.op_.vDiskInfo_.replicationPolicy = vDiskInfo.replicationPolicy
    	#self.op_.vDiskInfo_.targetLocations = hc.set_string()
    	#for location in vDiskInfo.targetLocations:
    	#    self.op_.vDiskInfo_.targetLocations.append(location)
    	#self.op_.vDiskInfo_.cloneInfo = hc.CloneInfo()
    	#self.op_.vDiskInfo_.cloneInfo.baseDisk = str(vDiskInfo.cloneInfo.baseDisk)
    	#self.op_.vDiskInfo_.cloneInfo.snapshot = str(vDiskInfo.cloneInfo.snapshot)
    	#self.op_.vDiskInfo_.cloneInfo.baseVersion = vDiskInfo.cloneInfo.baseVersion
    	#self.op_.vDiskInfo_.cloneInfo.typeOfClone = vDiskInfo.cloneInfo.typeOfClone
    	#self.op_.vDiskInfo_.replicationPolicyInfo = hc.ReplicationPolicyInfo()
    	#self.op_.vDiskInfo_.replicationPolicyInfo.dataCenterNames = hc.buffer_vector()
    	#for dataCenterName in vDiskInfo.replicationPolicyInfo.dataCenterNames:
    	#    self.op_.vDiskInfo_.replicationPolicyInfo.dataCenterNames.append(dataCenterName) 
    
    def addLun(self):
        self.pyCallback_.addLun(self.op_)
        self.waitForResult(self.__class__.__name__)
        return self.op_.lunNumber_
   
    def __del__(self):
        HedvigOpCb.__del__(self)

class HDeleteLunOpCb(HedvigOpCb):

    def __init__(self,tgtHost,tgtPort,vDiskName,lun):
        HedvigOpCb.__init__(self)
        self.op_ = hc.HDeleteLunOp()
        self.op_.tgtHost_ = str(tgtHost)
        self.op_.tgtPort_ = tgtPort
        self.op_.vDiskName_ = str(vDiskName)
        self.op_.lunNumber_ = lun
       
    def deleteLun(self):
        self.pyCallback_.deleteLun(self.op_)
        self.waitForResult(self.__class__.__name__)
        return self.op_.result_
   
    def __del__(self):
        HedvigOpCb.__del__(self)

class HAddAccessOpCb(HedvigOpCb):

    def __init__(self,tgtHost,tgtPort,lun,ip):
        HedvigOpCb.__init__(self)
        self.op_ = hc.HAddAccessOp()
        self.op_.tgtHost_ = str(tgtHost)
        self.op_.tgtPort_ = tgtPort
        self.op_.lunNumber_ = lun
        self.op_.Ip_ = ip
       
    def addAccess(self):
        self.pyCallback_.addAccess(self.op_)
        self.waitForResult(self.__class__.__name__)
        return self.op_.result_
   
    def __del__(self):
        HedvigOpCb.__del__(self)

class HSnapshotFromTgtOpCb(HedvigOpCb):

    def __init__(self,tgtHost,tgtPort,vDiskName):
        HedvigOpCb.__init__(self)
        self.op_ = hc.HSnapshotFromTgtOp()
        self.op_.tgtHost_ = str(tgtHost)
        self.op_.tgtPort_ = tgtPort
        self.op_.vDiskName_ = vDiskName
       
    def snapshotFromTgt(self):
        self.pyCallback_.snapshotFromTgt(self.op_)
        self.waitForResult(self.__class__.__name__)
        return self.op_.snapshot_
   
    def __del__(self):
        HedvigOpCb.__del__(self)

class HSnapshotOpCb(HedvigOpCb):  

    def __init__(self,vDiskName):
        HedvigOpCb.__init__(self)
        self.op_ = hc.HSnapshotOp()  
        self.op_.vDiskName_ = str(vDiskName)
       
    def snapshot(self):
        self.pyCallback_.snapshot(self.op_)
        self.waitForResult(self.__class__.__name__)
        return self.op_.snapshotInfo_
    
    def __del__(self):
        HedvigOpCb.__del__(self)

class HDeleteSnapshotOpCb(HedvigOpCb):

    def __init__(self,snapshotName):
        HedvigOpCb.__init__(self)
        self.op_ = hc.HDeleteSnapshotOp()
        self.op_.snapshotName_ = str(snapshotName)

    def deleteSnapshot(self):
        self.pyCallback_.deleteSnapshot(self.op_)
        self.waitForResult(self.__class__.__name__)

    def __del__(self):
        HedvigOpCb.__del__(self)

class HCreateVirtualDiskOpCb(HedvigOpCb):
       
    def __init__(self, vDiskInfo):
        HedvigOpCb.__init__(self)
        LOG.debug("Creating virtual disk: %s", vDiskInfo.vDiskName)
	self.op_ = hc.HCreateVirtualDiskOp()
        self.op_.vDiskInfo_ = hc.VDiskInfo()
	self.op_.vDiskInfo_.dedup = vDiskInfo.dedup
	self.op_.vDiskInfo_.vDiskName = str(vDiskInfo.vDiskName)
	self.op_.vDiskInfo_.createdBy = vDiskInfo.createdBy
	self.op_.vDiskInfo_.size = vDiskInfo.size
	self.op_.vDiskInfo_.replicationFactor = vDiskInfo.replicationFactor
	self.op_.vDiskInfo_.blockSize = vDiskInfo.blockSize
	self.op_.vDiskInfo_.exportedBlockSize = vDiskInfo.exportedBlockSize
	self.op_.vDiskInfo_.isClone = vDiskInfo.isClone
	self.op_.vDiskInfo_.cloudEnabled = vDiskInfo.cloudEnabled
	self.op_.vDiskInfo_.dedupBuckets = str(vDiskInfo.dedupBuckets)
	self.op_.vDiskInfo_.masterVDiskName = str(vDiskInfo.masterVDiskName)
	self.op_.vDiskInfo_.dedup = vDiskInfo.dedup
    	self.op_.vDiskInfo_.compressed = vDiskInfo.compressed
	self.op_.vDiskInfo_.immutable = vDiskInfo.immutable
    	self.op_.vDiskInfo_.cacheEnable = vDiskInfo.cacheEnable
    	self.op_.vDiskInfo_.vTreeBuffer = str(vDiskInfo.vTreeBuffer)
    	self.op_.vDiskInfo_.clusteredfilesystem = vDiskInfo.clusteredfilesystem
	self.op_.vDiskInfo_.description = str(vDiskInfo.description)
    	self.op_.vDiskInfo_.mntLocation = str(vDiskInfo.mntLocation)
    	self.op_.vDiskInfo_.residence = vDiskInfo.residence
    	self.op_.vDiskInfo_.diskType = vDiskInfo.diskType
    	self.op_.vDiskInfo_.cloudProvider = vDiskInfo.cloudProvider
	self.op_.vDiskInfo_.replicationPolicy = vDiskInfo.replicationPolicy
   
    def createVirtualDisk(self):
        self.pyCallback_.createVirtualDisk(self.op_)
        self.waitForResult(self.__class__.__name__)
       
    def __del__(self):
        HedvigOpCb.__del__(self)

class HResizeVirtualDiskOpCb(HedvigOpCb):

    def __init__(self, vDiskInfo):
        HedvigOpCb.__init__(self)
        LOG.debug("Resizing virtual disk: %s", vDiskInfo.vDiskName)
        self.op_ = hc.HResizeVirtualDiskOp()
        self.op_.vDiskInfo_ = hc.VDiskInfo()
    	self.op_.vDiskInfo_.dedup = vDiskInfo.dedup
    	self.op_.vDiskInfo_.vDiskName = str(vDiskInfo.vDiskName)
    	self.op_.vDiskInfo_.createdBy = vDiskInfo.createdBy
    	self.op_.vDiskInfo_.size = vDiskInfo.size
    	self.op_.vDiskInfo_.replicationFactor = vDiskInfo.replicationFactor
    	self.op_.vDiskInfo_.blockSize = vDiskInfo.blockSize
    	self.op_.vDiskInfo_.exportedBlockSize = vDiskInfo.exportedBlockSize
    	self.op_.vDiskInfo_.isClone = vDiskInfo.isClone
    	self.op_.vDiskInfo_.cloudEnabled = vDiskInfo.cloudEnabled
    	self.op_.vDiskInfo_.dedupBuckets = str(vDiskInfo.dedupBuckets)
    	self.op_.vDiskInfo_.masterVDiskName = str(vDiskInfo.masterVDiskName)
    	self.op_.vDiskInfo_.dedup = vDiskInfo.dedup
        self.op_.vDiskInfo_.compressed = vDiskInfo.compressed
    	self.op_.vDiskInfo_.immutable = vDiskInfo.immutable
        self.op_.vDiskInfo_.cacheEnable = vDiskInfo.cacheEnable
        self.op_.vDiskInfo_.vTreeBuffer = str(vDiskInfo.vTreeBuffer)
        self.op_.vDiskInfo_.clusteredfilesystem = vDiskInfo.clusteredfilesystem
    	self.op_.vDiskInfo_.description = str(vDiskInfo.description)
        self.op_.vDiskInfo_.mntLocation = str(vDiskInfo.mntLocation)
        self.op_.vDiskInfo_.residence = vDiskInfo.residence
        self.op_.vDiskInfo_.diskType = vDiskInfo.diskType
        self.op_.vDiskInfo_.cloudProvider = vDiskInfo.cloudProvider
    	self.op_.vDiskInfo_.replicationPolicy = vDiskInfo.replicationPolicy

    def resizeVirtualDisk(self):
        self.pyCallback_.resizeVirtualDisk(self.op_)
        self.waitForResult(self.__class__.__name__)
  
    def __del__(self):
        HedvigOpCb.__del__(self)

class HDeleteVirtualDiskOpCb(HedvigOpCb):

    def __init__(self, vDiskName):
        HedvigOpCb.__init__(self)
        self.op_ = hc.HDeleteVirtualDiskOp()
        self.op_.vDiskName_ = str(vDiskName)
       
    def deleteVirtualDisk(self):
        self.pyCallback_.deleteVirtualDisk(self.op_)
        self.waitForResult(self.__class__.__name__)
       
    def __del__(self):
        HedvigOpCb.__del__(self)

class HGetIqnOpCb(HedvigOpCb):  

    def __init__(self, hostname):
        HedvigOpCb.__init__(self)
        self.op_ = hc.HGetIqnOp()  
        self.op_.hostname_ = str(hostname)
       
    def getIqn(self):
        self.pyCallback_.getIqn(self.op_)
        self.waitForResult(self.__class__.__name__)
        return self.op_.iqn_
    
    def __del__(self):
        HedvigOpCb.__del__(self)

from Helper import Helper
