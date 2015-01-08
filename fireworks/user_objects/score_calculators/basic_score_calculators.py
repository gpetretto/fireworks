# coding: utf-8

from __future__ import unicode_literals, division

from fireworks.utilities.fw_utilities import explicit_serialize
from fireworks.queue.score_calculator import ScoreCalculatorBase

@explicit_serialize
class EstimatedDelayScoreCalculator(ScoreCalculatorBase):

    def calculate_score(self, qadapter_parameters=None):
        if qadapter_parameters:
            self._queue_adapter.update(qadapter_parameters)

        return self._queue_adapter.job_start_delay


@explicit_serialize
class QueuedJobsScoreCalculator(ScoreCalculatorBase):

    def calculate_score(self, qadapter_parameters=None):
        if qadapter_parameters:
            self._queue_adapter.update(qadapter_parameters)

        return self._queue_adapter.tot_njobs_in_queue


@explicit_serialize
class RequestedNodesRatioScoreCalculator(ScoreCalculatorBase):

    def calculate_score(self, qadapter_parameters=None):
        if qadapter_parameters:
            self._queue_adapter.update(qadapter_parameters)

        nodes_av = self._queue_adapter.tot_nodes_online
        if not nodes_av:
            return None

        nodes_requested = self._queue_adapter.tot_nodes_in_queue

        return nodes_requested/nodes_av


@explicit_serialize
class RequestedCoresRatioScoreCalculator(ScoreCalculatorBase):

    def calculate_score(self, qadapter_parameters=None):
        if qadapter_parameters:
            self._queue_adapter.update(qadapter_parameters)

        cores_av = self._queue_adapter.tot_cores_online
        if not cores_av:
            return None

        cores_requested = self._queue_adapter.tot_cores_in_queue

        return cores_requested/cores_av
