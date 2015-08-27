# coding: utf-8

from __future__ import unicode_literals

"""
This module is used to submit jobs to a queue on a cluster. It can submit a single job, \
or if used in "rapid-fire" mode, can submit multiple jobs within a directory structure. \
The details of job submission and queue communication are handled using Queueadapter, \
which specifies a QueueAdapter as well as desired properties of the submit script.
"""

import sys
import json
import time
import traceback
import datetime
import os
import logging
from fireworks.utilities.fw_utilities import get_fw_logger, log_exception
from fireworks.fw_config import QUEUE_UPDATE_INTERVAL, RAPIDFIRE_SLEEP_SECS, FW_BLOCK_FORMAT

__author__ = 'Guido Petretto'
__copyright__ = 'Copyright 2012, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Guido Petretto'
__email__ = 'g.petretto@gmail.com'
__date__ = 'Aug 27, 2015'


def distributed_assign_rocket_to_queue(launchpad, workers, strm_lvl='INFO', launcher_dir='.', fw_id=None,
                                       blacklisted_fw_ids=None):
    """
    Submit a single job to the best worker available, based on the calculation of a penalty

    :param launchpad: (LaunchPad)
    :param workers: list of RemoteFWorker
    :param strm_lvl: (str) level at which to stream log messages
    :param launcher_dir: (str) The directory where to submit the job
    :param fw_id: (int) if set, a particular Firework to run
    :param blacklisted_fw_ids: list of blacklisted FW ids that will be ignored
    :return: a boolean, True if a job was submitted and an updated list of blacklisted FW ids
    """

    try:
        import fabric
        from fabric.api import settings, run, cd, parallel, env, execute, prefix
        from fabric.network import disconnect_all
    except ImportError:
        print("Remote options require the Fabric package to be installed!")
        sys.exit(-1)

    blacklisted_fw_ids = [] if blacklisted_fw_ids is None else list(blacklisted_fw_ids)

    l_logger = get_fw_logger('queue.launcher', l_dir=launchpad.logdir, stream_level=strm_lvl)

    if fw_id in blacklisted_fw_ids:
        l_logger.warning("FW {} has been blacklisted.".format(fw_id))
        return None, blacklisted_fw_ids

    workers_dict = {w.full_host: w for w in workers}

    # setup fabric output level based on strm_lvl
    stream_level = getattr(logging, strm_lvl)
    if stream_level >= logging.INFO:
        fabric.state.output['output'] = False
        fabric.state.output['status'] = False
    if stream_level >= logging.WARNING:
        fabric.state.output['running'] = False
    if stream_level >= logging.ERROR:
        fabric.state.output['everything'] = False

    while True:
        if fw_id in blacklisted_fw_ids:
            break
        query = {'fw_id': {'$nin': blacklisted_fw_ids}}
        fw = launchpad._get_a_fw_to_run(query, fw_id, False)
        if not fw:
            break

        requested_worker = fw.spec.get('_fworker', None)
        requested_category = fw.spec.get('_category', None)
        # check workers that can actually run the job
        suitable_workers = []
        if requested_worker:
            for wrk in workers:
                if requested_worker == wrk.name:
                    suitable_workers = [wrk]
                    break
        else:
            for wrk in workers:
                # only workers in the specified category
                if not wrk.category or (requested_category and requested_category == wrk.category):
                    suitable_workers.append(wrk)

        # if no suitable worker, blacklist jobs and try again
        if not suitable_workers:
            blacklisted_fw_ids.append(fw.fw_id)
            l_logger.info("No suitable worker to launch rocket id {}. Job blacklisted.".format(fw.fw_id))
            continue

        @parallel
        def get_penalty(qadapter_parameters, workers_data, fw_id):
            try:
                current_worker = workers_data[env.host_string]
                command = 'lpad '
                if current_worker.config_dir:
                    command += '-c {} '.format(current_worker.config_dir)
                if current_worker.launchpad_file:
                    command += '-l {} '.format(current_worker.launchpad_file)
                command += 'calculate_penalty '
                if current_worker.queueadapter_file:
                    command += '-q {} '.format(current_worker.queueadapter_file)
                if current_worker.fworker_file:
                    command += '-w {} '.format(current_worker.fworker_file)
                if current_worker.penalty_calculator:
                    command += '-pc {} '.format(current_worker.penalty_calculator)
                if qadapter_parameters:
                    command += '-jp {}'.format(json.loads(qadapter_parameters))
                command += '-i {} '.format(fw_id)
                with prefix(current_worker.pre_command or 'true'):
                    out = run(command)
                penalty = out.split()[-1]
                if penalty == 'None':
                    return None
                return float(penalty)
            except:
                l_logger.warning("Error calculating penalty.")
                traceback.print_exc()
                return None

        try:
            # get all the penalties
            env.hosts = []
            env.use_ssh_config = True
            for wrk in suitable_workers:
                env.hosts.append(wrk.full_host)
                if wrk.password:
                    env.passwords[wrk.full_host] = wrk.password
            penalties = execute(get_penalty, fw.spec.get('_queueadapter', None), workers_dict, fw.fw_id)

            # remove workers that returned None and calculate (priority - penalty) for each host
            priorities = {w.full_host: w.priority - penalties[w.full_host]
                          for w in suitable_workers if penalties[w.full_host] is not None}

            # get the best host to submit the job
            if priorities:
                best_host = max(priorities, key=priorities.get)
            else:
                # if no available worker, blacklist the fw and try again
                blacklisted_fw_ids.append(fw.fw_id)
                l_logger.info("No available worker to launch rocket id {}. Job blacklisted.".format(fw.fw_id))
                continue

            # submit the job
            try:
                with settings(host_string=best_host):
                    best_worker =  workers_dict[best_host]
                    with prefix(best_worker.pre_command or 'true'):
                        # create the launcher_dir and qlaunch
                        with prefix('mkdir -p {}'.format(launcher_dir)):
                            command = "qlaunch --launch_dir {} --loglvl {} -r ".format(launcher_dir, strm_lvl)
                            if best_worker.config_dir:
                                command += '-c {} '.format(best_worker.config_dir)
                            if best_worker.launchpad_file:
                                command += '-l {} '.format(best_worker.launchpad_file)
                            if best_worker.queueadapter_file:
                                command += '-q {} '.format(best_worker.queueadapter_file)
                            if best_worker.fworker_file:
                                command += '-w {} '.format(best_worker.fworker_file)
                            command += "singleshot -i {}".format(fw.fw_id)
                            run(command)
                            l_logger.info("Rocket id {} launched on host {} (priority {})."
                                          .format(fw.fw_id, best_host, priorities[best_host]))
                            return True, blacklisted_fw_ids
            except:
                l_logger.error("Error submitting FW id {} to host {}.".format(fw.fw_id, best_host))
                traceback.print_exc()
                l_logger.info("Firework {} blacklisted.".format(fw.fw_id))
                blacklisted_fw_ids.append(fw.fw_id)
                return False, blacklisted_fw_ids
        finally:
            disconnect_all()

    l_logger.info("No rocket to assign. Blacklisted jobs: {}.".format(blacklisted_fw_ids))
    return False, blacklisted_fw_ids


def distributed_rapidfire(launchpad, workers, launch_dir='.', nlaunches=0, sleep_time=None,
                          blacklist_reset_freq=20, strm_lvl='INFO'):
    """
    Submit many jobs to remote queues based on the calculated penalties.

    :param launchpad: (LaunchPad)
    :param workers: list of RemoteFWorker
    :param launch_dir: remote directory where we want to write the blocks
    :param nlaunches: total number of launches desired; "infinite" for loop, 0 for one round
    :param sleep_time: (int) secs to sleep between rapidfire loop iterations
    :param blacklist_reset_freq: (int) number of rounds after which the blacklist is reset
    :param strm_lvl: (str) level at which to stream log messages
    """

    sleep_time = sleep_time if sleep_time else RAPIDFIRE_SLEEP_SECS
    nlaunches = -1 if nlaunches == 'infinite' else int(nlaunches)
    l_logger = get_fw_logger('queue.launcher', l_dir=launchpad.logdir, stream_level=strm_lvl)
    blacklisted_fw_ids = []

    num_launched = 0
    round_counter = 0
    try:
        while True:
            while launchpad._get_a_fw_to_run({'fw_id': {'$nin': blacklisted_fw_ids}}, checkout=False) is not None:
                l_logger.info('Launching a rocket!')

                #prepare the path for the launcher directory
                time_now = datetime.datetime.utcnow().strftime(FW_BLOCK_FORMAT)
                launcher_dir = os.path.join(launch_dir, 'launcher_' + time_now)

                # assign a single job
                success, blacklisted_fw_ids = distributed_assign_rocket_to_queue(launchpad, workers, strm_lvl,
                                                                                 launcher_dir, None, blacklisted_fw_ids)
                if not success:
                    l_logger.info("No rocket launched!")
                else:
                    num_launched += 1
                    if num_launched == nlaunches:
                        break
                # wait for the queue system to update
                l_logger.info('Sleeping for {} seconds...zzz...'.format(QUEUE_UPDATE_INTERVAL))
                time.sleep(QUEUE_UPDATE_INTERVAL)

            if num_launched == nlaunches or nlaunches == 0:
                break

            round_counter += 1
            if round_counter == blacklist_reset_freq:
                l_logger.info('Resetting jobs blacklist.')
                blacklisted_fw_ids = []
                round_counter = 0

            l_logger.info('Finished a round of launches, sleeping for {} secs'.format(sleep_time))
            time.sleep(sleep_time)
            l_logger.info('Checking for Rockets to run...'.format(sleep_time))

    except:
        log_exception(l_logger, 'Error with distributed launcher rapid fire!')