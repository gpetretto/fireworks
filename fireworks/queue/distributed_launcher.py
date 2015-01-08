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
from fireworks.utilities.fw_utilities import get_fw_logger, log_exception
from fireworks.fw_config import QUEUE_UPDATE_INTERVAL, RAPIDFIRE_SLEEP_SECS


def assign_rocket_to_queue(launchpad, strm_lvl='INFO', launcher_dir='.', blacklisted_fw_ids=[]):
    try:
        from fabric.api import settings, run, cd, parallel, env, execute
        from fabric.network import disconnect_all
    except ImportError:
        print("Remote options require the Fabric package to be "
              "installed!")
        sys.exit(-1)

    l_logger = get_fw_logger('queue.launcher', l_dir=launchpad.logdir, stream_level=strm_lvl)

    workers = launchpad.get_fworkers()

    while True:
        fw = launchpad.get_first_fw_to_run(blacklisted_fw_ids)
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
                if not wrk.category or (requested_category and requested_category == wrk.category):
                    suitable_workers.append(wrk)
            suitable_workers = workers

        # if no suitable worker, blacklist jobs and try again
        if not suitable_workers:
            blacklisted_fw_ids.append(fw.fw_id)
            l_logger.info("No suitable worker to launch rocket id {}. Job blacklisted.".format(fw.fw_id))
            continue

        @parallel
        def get_score(qadapter_parameters):
            try:
                commad = 'lpad calculate_score'
                if qadapter_parameters:
                    commad += '-q {}'.format(json.loads(qadapter_parameters))
                out = run(commad)
                score = out.split()[-1]
                if score == 'None':
                    return None
                return float(score)
            except:
                l_logger.warning("Error calculating score.")
                traceback.print_exc()
                return None

        try:
            # get all the scores
            env.hosts = []
            env.use_ssh_config = True
            for wrk in suitable_workers:
                host = wrk.host
                if wrk.username:
                    host = wrk.username+'@'+host
                env.hosts.append(host)
            scores = execute(get_score, fw.spec.get('_queueadapter', None))

            # remove workers that returned None
            scores = dict((k,v) for k,v in scores.iteritems() if v is not None)

            # get the best host to submit the job
            if scores:
                best_host = min(scores, key=scores.get)
                score = scores[best_host]
            else:
                blacklisted_fw_ids.append(fw.fw_id)
                l_logger.info("No available worker to launch rocket id {}. Job blacklisted.".format(fw.fw_id))
                continue

            # submit the job
            with settings(host_string=best_host):
                with cd(launcher_dir):
                    run("qlaunch singleshot -f {}".format(fw.fw_id))
                    l_logger.info("Rocket id {} launched on host {} (score {}).".format(fw.fw_id, best_host, score))
                    return True, blacklisted_fw_ids
        finally:
            disconnect_all()

    l_logger.info("No rocket to assign. Blacklisted jobs: {}.".format(blacklisted_fw_ids))
    return False, blacklisted_fw_ids



def rapidfire(launchpad, launcher_dir='.', nlaunches=0, njobs_queue=10, sleep_time=None,
              blacklist_reset_freq=20, strm_lvl='INFO'):
    """
    Submit many jobs to the queue.

    :param launchpad: (LaunchPad)
    :param launch_dir: directory where we want to write the blocks
    :param nlaunches: total number of launches desired; "infinite" for loop, 0 for one round
    :param sleep_time: (int) secs to sleep between rapidfire loop iterations
    :param reserve: (bool) Whether to queue in reservation mode
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
            while True:
                l_logger.info('Launching a rocket!')

                # assign a single job
                success, blacklisted_fw_ids = assign_rocket_to_queue(launchpad, strm_lvl,
                                                                     launcher_dir, blacklisted_fw_ids)
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

            round_counter +=1
            if round_counter == blacklist_reset_freq:
                l_logger.info('Resetting jobs blacklist.')
                blacklisted_fw_ids = []
                round_counter = 0

            l_logger.info('Finished a round of launches, sleeping for {} secs'.format(sleep_time))
            time.sleep(sleep_time)
            l_logger.info('Checking for Rockets to run...'.format(sleep_time))

    except:
        log_exception(l_logger, 'Error with distributed launcher rapid fire!')