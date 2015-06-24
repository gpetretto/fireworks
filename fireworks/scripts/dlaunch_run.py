# coding: utf-8

from __future__ import unicode_literals

"""
A runnable script for distributing rockets launches (a command-line interface to distributed_launcher.py)
"""

from argparse import ArgumentParser
import os
import sys
import time
from fireworks.fw_config import LAUNCHPAD_LOC
from fireworks.core.launchpad import LaunchPad
from fireworks.queue.distributed_launcher import assign_rocket_to_queue, rapidfire


def do_launch(args):
    if not args.launchpad_file and os.path.exists(
            os.path.join(args.config_dir, 'my_launchpad.yaml')):
        args.launchpad_file = os.path.join(args.config_dir, 'my_launchpad.yaml')

    launchpad = LaunchPad.from_file(
        args.launchpad_file) if args.launchpad_file else LaunchPad(
        strm_lvl=args.loglvl)
    args.loglvl = 'CRITICAL' if args.silencer else args.loglvl

    if args.command == 'rapidfire':
        rapidfire(launchpad, args.launch_dir, args.nlaunches, args.sleep, args.blacklist_reset_freq, args.loglvl)
    else:
        assign_rocket_to_queue(launchpad, args.loglvl, args.launch_dir)


def dlaunch():
    m_description = 'This program is used to submit jobs to a remote queueing system in a distributed way,  \
    according to the penalty provided by the available resources. '

    parser = ArgumentParser(description=m_description)
    subparsers = parser.add_subparsers(help='command', dest='command')
    single_parser = subparsers.add_parser('singleshot', help='launch a single rocket to the queue')
    rapid_parser = subparsers.add_parser('rapidfire', help='launch multiple rockets to the queue')

    parser.add_argument("-d", "--daemon",
                        help="Daemon mode. Command is repeated every x "
                             "seconds. Defaults to 0, which means non-daemon "
                             "mode.",
                        type=int,
                        default=0)
    parser.add_argument('--launch_dir', help='remote directory to launch the job', default='.')
    parser.add_argument('--logdir', help='path to a directory for logging', default=None)
    parser.add_argument('--loglvl', help='level to print log messages', default='INFO')
    parser.add_argument('-s', '--silencer', help='shortcut to mute log messages', action='store_true')
    parser.add_argument('-l', '--launchpad_file', help='path to launchpad file', default=LAUNCHPAD_LOC)


    rapid_parser.add_argument('-m', '--maxjobs_queue',
                              help='maximum jobs to keep in queue for this user', default=10,
                              type=int)
    rapid_parser.add_argument('--nlaunches', help='num_launches (int or "infinite"; default 0 is all jobs in DB)', default=0)
    rapid_parser.add_argument('--sleep', help='sleep time between loops', default=None, type=int)
    rapid_parser.add_argument('-br', '--blacklist_reset_freq', default=20, type=int,
                              help='Number of rounds after which the list of blacklisted FW ids is reset')

    args = parser.parse_args()

    interval = args.daemon
    while True:
        do_launch(args)
        if interval > 0:
            print("Next run in {} seconds... Press Ctrl-C to exit at any "
                  "time.".format(interval))
            time.sleep(interval)
        else:
            break

if __name__ == '__main__':
    dlaunch()
