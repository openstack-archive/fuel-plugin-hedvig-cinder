from email.Utils import formatdate
import xml.etree.ElementTree as ET
from datetime import datetime
import rfc822
from dateutil import parser
from dateutil import tz
import time
import datetime
from eventlet import getcurrent

class Helper(object):
    def __init__(self, params):
        pass

    @staticmethod
    def getId():
        return id(getcurrent())

from HedvigOpCb import *
