# coding: utf-8

from __future__ import unicode_literals, division

from fireworks.utilities.fw_utilities import explicit_serialize
from fireworks.queue.penalty_calculator import PenaltyCalculatorBase

@explicit_serialize
class EstimatedDelayPenaltyCalculator(PenaltyCalculatorBase):

    def calculate_penalty(self, qadapter_parameters=None):
        if qadapter_parameters:
            self._queue_adapter.update(qadapter_parameters)

        return self._queue_adapter.job_start_delay


@explicit_serialize
class QueuedJobsPenaltyCalculator(PenaltyCalculatorBase):

    def calculate_penalty(self, qadapter_parameters=None):
        if qadapter_parameters:
            self._queue_adapter.update(qadapter_parameters)

        return self._queue_adapter.tot_njobs_in_queue


@explicit_serialize
class RequestedNodesRatioPenaltyCalculator(PenaltyCalculatorBase):

    def calculate_penalty(self, qadapter_parameters=None):
        if qadapter_parameters:
            self._queue_adapter.update(qadapter_parameters)

        nodes_av = self._queue_adapter.nodes_online
        if not nodes_av:
            return None

        nodes_requested = self._queue_adapter.nodes_requested_in_queue

        return nodes_requested/nodes_av


@explicit_serialize
class RequestedCoresRatioPenaltyCalculator(PenaltyCalculatorBase):

    def calculate_penalty(self, qadapter_parameters=None):
        if qadapter_parameters:
            self._queue_adapter.update(qadapter_parameters)

        cores_av = self._queue_adapter.cores_online
        if not cores_av:
            return None

        cores_requested = self._queue_adapter.cores_requested_in_queue

        return cores_requested/cores_av
