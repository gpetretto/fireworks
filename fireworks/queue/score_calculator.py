# coding: utf-8

from __future__ import unicode_literals

"""
This module contains the contract for defining a score calculator.
"""
import six
import os
import shlex
import string
import subprocess
import threading
import traceback
import abc
import collections
from fireworks.utilities.fw_serializers import FWSerializable, serialize_fw, load_object_from_file, load_object
from fireworks.utilities.fw_utilities import get_fw_logger
from fireworks.fw_config import QUEUEADAPTER_LOC

@six.add_metaclass(abc.ABCMeta)
class ScoreCalculatorBase(FWSerializable):
    def __init__(self, queue_adapter):
        self._queue_adapter = queue_adapter

    @abc.abstractmethod
    def calculate_score(self, qadapter_parameters=None):
        pass

    @classmethod
    def from_qadapter_file(cls, file_path):
        if not file_path:
            file_path = QUEUEADAPTER_LOC

        return cls(load_object_from_file(file_path))

    @serialize_fw
    def to_dict(self):
        d = {'qadapter': self._queue_adapter.to_dict()}

    @classmethod
    def from_dict(cls, m_dict):
        if 'qadapter' in m_dict:
            return cls(load_object(m_dict['qadapter']))
        else:
            return cls.from_qadapter_file(m_dict.get('_qadapter_file'))
