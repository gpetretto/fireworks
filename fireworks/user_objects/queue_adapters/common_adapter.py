# coding: utf-8

from __future__ import unicode_literals

"""
This module implements a CommonAdaptor that supports standard PBS and SGE
queues.
"""
import getpass
import os
import re
import subprocess
from fireworks.queue.queue_adapter import QueueAdapterBase, Command
from fireworks.utilities.fw_serializers import serialize_fw
from fireworks.utilities.fw_utilities import log_exception, log_fancy
import datetime
from collections import OrderedDict

__author__ = 'Anubhav Jain, Michael Kocher, Shyue Ping Ong, David Waroquiers, Felix Brockherde'
__copyright__ = 'Copyright 2012, The Materials Project'
__version__ = '0.1'
__maintainer__ = 'Anubhav Jain'
__email__ = 'ajain@lbl.gov'
__date__ = 'Dec 12, 2012'


class CommonAdapter(QueueAdapterBase):
    """
    An adapter that works on most PBS (including derivatives such as
    TORQUE), SGE, and SLURM queues.
    """
    _fw_name = 'CommonAdapter'
    supported_q_types = {
        "PBS": "qsub",
        "SGE": "qsub",
        "SLURM": "sbatch",
        "LoadLeveler": "llsubmit"
    }

    def __init__(self, q_type, q_name=None, template_file=None, **kwargs):
        """
        :param q_type: The type of queue. Right now it should be either PBS,
                       SGE, SLURM or LoadLeveler.
        :param q_name: A name for the queue. Can be any string.
        :param template_file: The path to the template file. Leave it as
                              None (the default) to use Fireworks' built-in
                              templates for PBS and SGE, which should work
                              on most queues.
        :param **kwargs: Series of keyword args for queue parameters.
        """
        if q_type not in CommonAdapter.supported_q_types:
            raise ValueError(
                "{} is not a supported queue type. "
                "CommonAdaptor supports {}".format(
                    q_type, list(CommonAdapter.supported_q_types.keys())))
        self.q_type = q_type
        self.template_file = os.path.abspath(template_file) if template_file is not None else \
            CommonAdapter._get_default_template_file(q_type)
        self.q_name = q_name or q_type
        self.update(dict(kwargs))

    def _parse_jobid(self, output_str):
        if self.q_type == "SLURM":
            for l in output_str.split("\n"):
                if l.startswith("Submitted batch job"):
                    return int(l.split()[-1])
        if self.q_type == "LoadLeveler":
            # Load Leveler: "llsubmit: The job "abc.123" has been submitted"
            re_string = r"The job \"(.*?)\" has been submitted"
        else:
            # PBS: "1234.whatever",
            # SGE: "Your job 44275 ("jobname") has been submitted"
            re_string = r"(\d+)"
        m = re.search(re_string, output_str)
        if m:
            return m.group(1)
        raise RuntimeError("Unable to parse jobid")

    def _get_status_cmd(self, username):
        if self.q_type == 'SLURM':
            return ['squeue', '-o "%u"', '-u', username]
        elif self.q_type == "LoadLeveler":
            return ['llq', '-u', username]
        else:
            return ['qstat', '-u', username]

    def _parse_njobs(self, output_str, username):
        # TODO: what if username is too long for the output and is cut off?

        if self.q_type == 'SLURM': # this special case might not be needed
            # TODO: currently does not filter on queue name or job state
            outs = output_str.split('\n')
            return len([line.split() for line in outs if username in line])

        if self.q_type == "LoadLeveler":
            if 'There is currently no job status to report' in output_str:
                return 0
            else:
                # last line is: "1 job step(s) in query, 0 waiting, ..."
                return int(output_str.split('\n')[-2].split()[0])

        count = 0
        for l in output_str.split('\n'):
            if l.lower().startswith("job"):
                header = l.split()
                if self.q_type == "PBS":
                    #PBS has a ridiculous two word "Job ID" in header
                    state_index = header.index("S") - 1
                    queue_index = header.index("Queue") - 1
                else:
                    state_index = header.index("state")
                    queue_index = header.index("queue")
            if username in l:
                toks = l.split()
                if toks[state_index] != "C":
                    # note: the entire queue name might be cutoff from the output if long queue name
                    # so we are only ensuring that our queue matches up until cutoff point
                    if "queue" in self and self["queue"][0:len(toks[queue_index])] in toks[queue_index]:
                        count += 1

        return count

    def submit_to_queue(self, script_file):
        """
        submits the job to the queue and returns the job id

        :param script_file: (str) name of the script file to use (String)
        :return: (int) job_id
        """
        if not os.path.exists(script_file):
            raise ValueError(
                'Cannot find script file located at: {}'.format(
                    script_file))

        queue_logger = self.get_qlogger('qadapter.{}'.format(self.q_name))
        submit_cmd = CommonAdapter.supported_q_types[self.q_type]
        # submit the job
        try:
            cmd = [submit_cmd, script_file]
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            p.wait()

            # grab the returncode. PBS returns 0 if the job was successful
            if p.returncode == 0:
                try:
                    job_id = self._parse_jobid(p.stdout.read())
                    queue_logger.info(
                        'Job submission was successful and job_id is {}'.format(
                            job_id))
                    return job_id
                except Exception as ex:
                    # probably error parsing job code
                    log_exception(queue_logger,
                                  'Could not parse job id following {} due to error {}...'
                                  .format(submit_cmd, str(ex)))
            else:
                # some qsub error, e.g. maybe wrong queue specified, don't have permission to submit, etc...
                msgs = [
                    'Error in job submission with {n} file {f} and cmd {c}'.format(
                        n=self.q_name, f=script_file, c=cmd),
                    'The error response reads: {}'.format(p.stderr.read())]
                log_fancy(queue_logger, msgs, 'error')

        except Exception as ex:
            # random error, e.g. no qsub on machine!
            log_exception(queue_logger,
                          'Running the command: {} caused an error...'
                          .format(submit_cmd))

    def get_njobs_in_queue(self, username=None):
        """
        returns the number of jobs currently in the queu efor the user

        :param username: (str) the username of the jobs to count (default is to autodetect)
        :return: (int) number of jobs in the queue
        """
        queue_logger = self.get_qlogger('qadapter.{}'.format(self.q_name))

        # initialize username
        if username is None:
            username = getpass.getuser()

        # run qstat
        qstat = Command(self._get_status_cmd(username))
        p = qstat.run(timeout=5)

        # parse the result
        if p[0] == 0:
            njobs = self._parse_njobs(p[1], username)
            queue_logger.info(
                'The number of jobs currently in the queue is: {}'.format(
                    njobs))
            return njobs

        # there's a problem talking to qstat server?
        msgs = ['Error trying to get the number of jobs in the queue',
                'The error response reads: {}'.format(p[2])]
        log_fancy(queue_logger, msgs, 'error')
        return None

    @staticmethod
    def _get_default_template_file(q_type):
        return os.path.join(os.path.dirname(__file__), '{}_template.txt'.format(q_type))

    @serialize_fw
    def to_dict(self):
        d = dict(self)
        # _fw_* names are used for the specific instance variables.
        d["_fw_q_type"] = self.q_type
        if self.q_name != self.q_type:
            d["_fw_q_name"] = self.q_name
        if self.template_file != CommonAdapter._get_default_template_file(self.q_type):
            d["_fw_template_file"] = self.template_file
        return d

    @classmethod
    def from_dict(cls, m_dict):
        return cls(
            q_type=m_dict["_fw_q_type"],
            q_name=m_dict.get("_fw_q_name"),
            template_file=m_dict.get("_fw_template_file"),
            **{k: v for k, v in m_dict.items() if not k.startswith("_fw")})

    def _parse_hms(self, time_str):
        t = datetime.datetime.strptime(time_str, '%H:%M:%S')
        return datetime.timedelta(hours=t.tm_hour, minutes=t.tm_min, seconds=t.tm_sec).total_seconds()

    def _parse_walltime(self, walltime):
        """
        returns the walltime value in seconds
        """
        # if int, assume is already in seconds
        try:
            seconds = int(walltime)
            return seconds
        except:
            pass

        # HH:MM:SS
        try:
            self._parse_hms(walltime)
        except:
            pass

        raise ValueError("Couldn't parse walltime: {}".format(walltime))

    def _get_estimated_start_delay_cmd(self):
        if self.q_name == "SLURM":
            return ["sbatch", "--test-only", self.get_script_str('.')]
        elif Command(["showstart", "-h"]).run(timeout=5)[0] == 0:
            #check if checkstart exists (requires maui, compatible with PBS, LoadLeveler and SGE)
            num_cores = self['nnodes']*self['ppnode']
            walltime = self._parse_walltime(self['walltime'])
            return ["checkstart", str(num_cores)+"@"+str(walltime)]
        else:
            raise NotImplementedError("Estimated starting time not implemented for queue type {}.".format(self.q_name))

    def _parse_estimated_start_delay(self, output_str):
        out_split = output_str.split()
        if self.q_name == "SLURM":
            start_time = datetime.datetime.strptime(out_split[6], "%Y-%m-%dT%H:%M:%S")
            return max(0, start_time-datetime.datetime.now())
        else:
            delay_index = out_split.index('in')+1
            return self._parse_hms(out_split[delay_index])

    @property
    def job_start_delay(self):
        """
        Estimation of the starting delay for a standard job from the queue manager.
        Return the delay in seconds
        """
        if self.q_name == "SLURM":
            cmd = ["sbatch", "--test-only"]

            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(self.get_script_str('.'))
            status = process.returncode

            if status == 0:
                start_time = datetime.datetime.strptime(stderr.split()[6], "%Y-%m-%dT%H:%M:%S")
                return max(0, (start_time-datetime.datetime.now()).total_seconds())


        elif Command(["checkstart", "-h"]).run(timeout=5)[0] == 0:
            #check if checkstart exists (requires maui, compatible with PBS, LoadLeveler and SGE)
            num_cores = self['nnodes']*self['ppnode']
            walltime = self._parse_walltime(self['walltime'])
            return ["checkstart", str(num_cores)+"@"+str(walltime)]
        else:
            raise NotImplementedError("Estimated starting time not implemented for queue type {}.".format(self.q_name))
        checkstart = Command(self._get_estimated_start_delay_cmd())
        p = checkstart.run(timeout=5)

        # parse the result
        if p[0] == 0:
            time = self._parse_estimated_start_delay(p[1])
            return time

        # there's a problem talking to qstat server?
        msgs = ['Error trying to get the expected delay'
                'The error response reads: {}'.format(stderr)]
        queue_logger = self.get_qlogger('qadapter.{}'.format(self.q_name))
        log_fancy(queue_logger, msgs, 'error')
        return None

    def _get_tot_jobs_in_queue_cmd(self, queue=None):
        if self.q_name == "PBS":
            if queue:
                return ["qstat", "-q", queue]
            else:
                return ["qstat", "-q"]
        elif self.q_name == "SLURM":
            if queue:
                return ["squeue", "-o", "%t", "-p", queue]
            else:
                return ["squeue", "-o", "%t"]
        else:
            raise NotImplementedError("Total number of jobs not implemented for queue type {}.".format(self.q_name))

    def _parse_tot_njobs_in_queue(self, output_str):
        if self.q_name == "PBS":
            return int(output_str.split("\n")[-2].split()[1])
        elif self.q_name == "SLURM":
            outs = output_str.split('\n')
            return len([line.split() for line in outs if 'PD' in line])
        else:
            raise NotImplementedError("Total number of jobs not implemented for queue type {}.".format(self.q_name))

    def get_tot_njobs_in_queue(self, queue=None):
        """
        Helper function to get the total number of queued jobs for the specified queue.
        Sums all the queues if queue=None
        """
        queue_logger = self.get_qlogger('qadapter.{}'.format(self.q_name))

        qstat = Command(self._get_tot_jobs_in_queue_cmd(queue))
        p = qstat.run(timeout=5)

        # parse the result
        if p[0] == 0:
            njobs = self._parse_tot_njobs_in_queue(p[1])
            queue_logger.info(
                'The number of jobs currently in the queue is: {}'.format(
                    njobs))
            return njobs

        # there's a problem talking to qstat server?
        msgs = ['Error trying to get the number of jobs in the queue',
                'The error response reads: {}'.format(p[2])]
        log_fancy(queue_logger, msgs, 'error')
        return None

    @property
    def tot_njobs_in_queue(self):
        return self.get_tot_njobs_in_queue(queue=self.get('queue', None))

    def _get_job_list_cmd(self, usernames='$USER', queues=None, status='Q'):
        if usernames == '$USER':
            usernames = getpass.getuser()
        if isinstance(usernames, str):
            usernames=[usernames]
        if isinstance(queues, str):
            queues = [queues]

        if self.q_type == 'SLURM':
            # -r to expand array jobs
            command = ['squeue', '-r', '-o %i %P %j %u %t %M %l %D %C']
            if usernames:
                command.append('-u')
                command.append(",".join(map(str, usernames)))
            if queues:
                command.append('-p')
                command.append(",".join(map(str, queues)))
            if not status:
                pass
            elif status == 'Q':
                command.extend(['-t', 'PD'])
            elif status == 'R':
                command.extend(['-t', 'R'])
            else:
                raise ValueError('Not supported status value {}.'.format(status))
            return command
        elif self.q_type == "PBS":
            # -t to expand array jobs,
            command = ['qstat', '-t']
            if usernames:
                command.append('-u')
                command.append(",".join(map(str, usernames)))
            if not status:
                command.append('-a')
            elif status == 'Q':
                command.append('-i')
            elif status == 'R':
                command.append('-r')
            else:
                raise ValueError('Not supported status value {}.'.format(status))
            if queues:
                command.append(",".join(map(str, queues)))
            return command
        else:
            raise NotImplementedError("Job list not implemented for queue type {}.".format(self.q_name))

    def _parse_job_list(self, output_str):
        queue_data = []
        if self.q_type == 'SLURM':
            lines = output_str.split('\n')
            # clean the header
            while len(lines) > 0:
                h = lines.pop(0)
                if h.strip().startswith('JOBID'):
                    break
            if len(lines) == 0:
                return queue_data
            for l in lines:
                if not l:
                    continue
                data = l.split()
                queue_data.append({'id': data[0], 'queue': data[1], 'job_name': data[2], 'user': data[3],
                                   'status': data[4], 'elapsed_time': data[5], 'time_limit': data[6],
                                   'n_nodes': data[7], 'n_cores':data[8]})

        elif self.q_type == "PBS":
            lines = output_str.split('\n')
            # clean the header
            while len(lines) > 0:
                h = lines.pop(0)
                if h.strip().startswith('Job'):
                    # remove one more line
                    lines.pop(0)
                    break
            if len(lines) == 0:
                return queue_data
            for l in lines:
                if not l:
                    continue
                data = l.split()
                queue_data.append({'id': data[0], 'queue': data[2], 'job_name': data[3], 'user': data[1],
                                   'status': data[9], 'elapsed_time': '00:00' if data[10] == '--' else data[10],
                                   'time_limit': data[8], 'n_nodes': data[5], 'n_cores':data[6]})

        else:
            raise NotImplementedError("Job list not implemented for queue type {}.".format(self.q_name))

        return queue_data

    def get_parsed_job_list(self, usernames='$USER', queues=None, status='Q'):
        """
        Helper function to get a detailed list of job according to filter parameters

        :param usernames: (str or list) username(s) used to filter the job list. '$USER' to get the current user
        :param queues: (str or list) queue name(s) used to filter the job list
        :param status: (str) job status. 'Q' for queued, 'R' for running, None for all the states
        :return: (list) a list of dictionaries, each containing job details: id, queue, job_name, user, status,
                 elapsed_time, time_limit, n_nodes, n_cores
        """

        qstat = Command(self._get_job_list_cmd(usernames=usernames, queues=queues, status=status))
        p = qstat.run(timeout=5)

        # parse the result
        if p[0] == 0:
            njobs = self._parse_job_list(p[1])
            return njobs

        msgs = ['Error trying to get the job list'
                'The error response reads: {}'.format(p[2])]
        queue_logger = self.get_qlogger('qadapter.{}'.format(self.q_name))
        log_fancy(queue_logger, msgs, 'error')
        return None

    def get_cores_requested_in_queue(self, queues=None):
        """
        Helper function to get the total number of cores requested by queued jobs for the specified queue.
        Sums all the queues if queue=None
        """
        queue_data = self.get_parsed_job_list(usernames=None, queues=queues, status='Q')
        if not queue_data:
            return 0
        else:
            tot_cores = sum(int(i['n_cores']) for i in queue_data)
            return tot_cores

    @property
    def cores_requested_in_queue(self):
        return self.get_cores_requested_in_queue(queues=self.get('queue',None))

    def get_nodes_requested_in_queue(self, queues=None):
        """
        Helper function to get the total number of nodes requested by queued jobs for the specified queue.
        Sums all the queues if queue=None
        """
        queue_data = self.get_parsed_job_list(usernames=None, queues=queues, status='Q')
        if not queue_data:
            return 0
        else:
            tot_nodes = sum(int(i['n_nodes']) for i in queue_data)
            return tot_nodes

    @property
    def nodes_requested_in_queue(self):
        return self.get_nodes_requested_in_queue(queues=self.get('queue', None))

    def _get_nodes_number_cmd(self, queues):
        if isinstance(queues, str):
            queues = [queues]
        if self.q_type == 'SLURM':
            command=['sinfo', '-o %9P %.5a %5A']
            if queues:
                command.append('-p')
                command.append(",".join(map(str, queues)))
        else:
            raise NotImplementedError("Cores number not implemented for queue type {}.".format(self.q_name))
        return command

    def get_nodes_online(self, queues=None):
        """
        Helper function to get the total number of nodes online for the specified queue.
        Sums all the queues if queue=None
        """
        if isinstance(queues, str):
            queues = [queues]
        if self.q_type == 'PBS':
            if queues:
                raise NotImplementedError('Queue option not supported option not supported for PBS queue type')
            cmd = 'echo $(($(pbsnodes -a | grep "state =" | wc -l) - $(pbsnodes -l| wc -l)))'
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = process.communicate()
            status = process.returncode

            if status == 0:
                return int(stdout)
        elif self.q_type == 'SLURM':
            cmd = ['sinfo', '-o %9P %.5a %5A']
            if queues:
                cmd.append('-p')
                cmd.append(",".join(map(str, queues)))
            nodes = Command(self._get_nodes_number_cmd(queues))
            status, stdout, stderr = nodes.run(timeout=5)
            if status == 0:
                nnodes = 0
                for l in stdout.split('\n'):
                    data = l.split()
                    if data and data[1] == 'up':
                        nnodes += sum([int(n) for n in data[2].split('/')])
                return nnodes
        else:
            raise NotImplementedError("Nodes number not implemented for queue type {}.".format(self.q_name))

        msgs = ['Error trying to get the number of nodes'
        'The error response reads: {}'.format(stderr)]
        queue_logger = self.get_qlogger('qadapter.{}'.format(self.q_name))
        log_fancy(queue_logger, msgs, 'error')
        return None

    @property
    def nodes_online(self):
        return self.get_nodes_online(queues=self.get('queue', None))

    def _get_cores_number_cmd(self, queues):
        if isinstance(queues, str):
            queues = [queues]
        if self.q_type == 'SLURM':
            command=['sinfo', '-o %9P %.5a %5c %5A']
            if queues:
                command.append('-p')
                command.append(",".join(map(str, queues)))
        else:
            raise NotImplementedError("Cores number not implemented for queue type {}.".format(self.q_name))
        return command

    def _parse_cores_number(self, output_str):
        if self.q_type == 'SLURM':
            ncores = 0
            for l in output_str.split('\n'):
                data = l.split()
                if data and data[1] == 'up':
                    nnodes = sum([int(n) for n in data[3].split('/')])
                    ncores_per_node = int(re.sub(r"\D", "", data[2]))
                    ncores += ncores_per_node*nnodes
            return ncores
        else:
            raise NotImplementedError("Cores number not implemented for queue type {}.".format(self.q_name))

    def get_cores_online(self, queues=None):
        """
        Helper function to get the total number of cores online for the specified queue.
        Sums all the queues if queue=None
        """
        cores = Command(self._get_cores_number_cmd(queues))
        p = cores.run(timeout=5)
        if p[0] == 0:
            ncores = self._parse_cores_number(p[1])
            return ncores

        msgs = ['Error trying to get the number of cores'
        'The error response reads: {}'.format(p[2])]
        queue_logger = self.get_qlogger('qadapter.{}'.format(self.q_name))
        log_fancy(queue_logger, msgs, 'error')
        return None

    @property
    def cores_online(self):
        return self.get_cores_online(queues=self.get('queue', None))
