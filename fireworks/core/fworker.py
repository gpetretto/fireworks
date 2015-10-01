# coding: utf-8

from __future__ import unicode_literals

"""
This module contains FireWorker, which encapsulates information about a
computing resource
"""

import json
from fireworks.utilities.fw_serializers import FWSerializable, \
    recursive_serialize, recursive_deserialize, DATETIME_HANDLER
from monty.serialization import loadfn

__author__ = 'Anubhav Jain'
__credits__ = 'Michael Kocher'
__copyright__ = 'Copyright 2012, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Dec 12, 2012'


class FWorker(FWSerializable):

    def __init__(self, name="Automatically generated Worker", category='',
                 query=None, env=None):
        """
        Args:
            name: the name of the resource, should be unique
            category: a String describing the computing resource, does not
                need to be unique
            query: a dict query that restricts the type of Firework this
                resource will run
            env: a dict of special environment variables for the resource.
                This env is passed to running FireTasks as a _fw_env in the
                fw_spec, which provides for abstraction of resource-specific
                commands or settings.  See
                :class:`fireworks.core.firework.FireTaskBase` for information
                on how to use this env variable in FireTasks.
        """
        self.name = name
        self.category = category
        self._query = query if query else {}
        self.env = env if env else {}

    @recursive_serialize
    def to_dict(self):
        return {'name': self.name, 'category': self.category,
                'query': json.dumps(self._query, default=DATETIME_HANDLER),
                'env': self.env}

    @classmethod
    @recursive_deserialize
    def from_dict(cls, m_dict):
        return FWorker(m_dict['name'], m_dict['category'],
                       json.loads(m_dict['query']),
                       m_dict.get("env"))

    @property
    def query(self):
        q = dict(self._query)
        fworker_check = [{"spec._fworker": {"$exists": False}}, {"spec._fworker": None}, {"spec._fworker": self.name}]
        if '$or' in q:
            # TODO: cover case where user sets an $and query?
            q['$and'] = q.get('$and', [])
            q['$and'].extend([{'$or': q.pop('$or')}, {'$or': fworker_check}])
        else:
            q['$or'] = fworker_check
        if self.category:
            q['spec._category'] = self.category
        return q


class RemoteFWorker(FWSerializable):
    def __init__(self, name, priority=0, host=None, port=22, username=None, password=None,  config_dir=None,
                 launchpad_file=None, fworker_file=None, queueadapter_file=None, penalty_calculator=None, pre_command=None,
                 maxjobs_queue=None):
        self.name = name
        self.priority = priority
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.config_dir = config_dir
        self.launchpad_file = launchpad_file
        self.fworker_file = fworker_file
        self.queueadapter_file = queueadapter_file
        self.penalty_calculator = penalty_calculator
        self.pre_command = pre_command
        self.maxjobs_queue = maxjobs_queue

    @recursive_serialize
    def to_dict(self):
        return {'name': self.name, 'priority': self.priority, 'host': self.host,
                'port': self.port, 'username': self.username, 'password': self.password, 'config_dir': self.config_dir,
                'launchpad_file': self.launchpad_file, 'fworker_file': self.fworker_file,
                'queueadapter_file': self.queueadapter_file, 'penalty_calculator': self.penalty_calculator,
                'pre_command': self.pre_command, 'maxjobs_queue': self.maxjobs_queue}

    @classmethod
    @recursive_deserialize
    def from_dict(cls, m_dict):
        return cls(**m_dict)

    def __str__(self):
        return "Remote fireworker: {}".format(self.name)

    @property
    def full_host(self):
        """
        full host created as username@host:port
        lazy property. evaluated only once.
        """
        try:
            return self._full_host
        except AttributeError:
            self._full_host = self.host
            if self.username:
                self._full_host = self.username + '@' + self._full_host
            if self.port:
                self._full_host = self._full_host + ':' + str(self.port)
            return self._full_host

    @classmethod
    def list_from_file(cls, filename):
        """
        Loads a list of RemoteFWorkers from a file
        """

        data = loadfn(filename)
        remote_workers = []
        for worker_dict in data:
            remote_workers.append(cls.from_dict(worker_dict))

        return remote_workers

