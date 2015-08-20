# coding: utf-8

from __future__ import unicode_literals

"""
This module contains FireWorker, which encapsulates information about a
computing resource
"""

import json
from fireworks.utilities.fw_serializers import FWSerializable, \
    recursive_serialize, recursive_deserialize, DATETIME_HANDLER

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
                'query': json.dumps(self.query, default=DATETIME_HANDLER),
                'env': self.env}

    @classmethod
    @recursive_deserialize
    def from_dict(cls, m_dict):
        return FWorker(m_dict['name'], m_dict['category'],
                       json.loads(m_dict['query']),
                       m_dict.get("env"))

    @property
    def query(self):
        q = self._query
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
    def __init__(self, name, priority=0, category=None, host=None, port=None, config_dir=None, username=None,
                 password=None):
        self.name = name
        self.priority = priority
        self.category = category
        self.host = host
        self.port = port
        self.config_dir = config_dir
        self.username = username
        self.password = password

    @recursive_serialize
    def to_dict(self):
        return {'name': self.name, 'priority': self.priority, 'category': self.category,
                'host': self.host, 'port':self.port, 'config_dir': self.config_dir,
                'username': self.username, 'password': self.password}

    @classmethod
    @recursive_deserialize
    def from_dict(cls, m_dict):
        return cls(m_dict['name'], m_dict['priority'], m_dict['category'],
                   m_dict['host'], m_dict['port'], m_dict['config_dir'],
                   m_dict['username'], m_dict['password'])

    def __str__(self):
        out = '\n'.join(['name: '+str(self.name), 'priority: '+str(self.priority), 'categoy: '+str(self.category),
               'host: '+str(self.host), 'port: '+str(self.port), 'config_dir: '+str(self.config_dir),
               'username: '+str(self.username), 'password: '+str(self.password)])
        return out

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
