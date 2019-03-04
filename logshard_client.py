#!/usr/bin/env python

import binascii
import getopt
import logging
import os
import subprocess
import sys
import time

import requests

DEFAULT_INTERVAL = 30
DEFAULT_TIMEOUT = 5
DEFAULT_LOG_DIR = '.'

USAGE = """
logshard_client [options] HOST PORT

-c, --command COMMAND
    COMMAND to run when a new log file is created.
    %%s in COMMAND is substituted with the log_file
    absolute path.

-e, --error-log
  File to use for logging errors
  (default = stderr)

-h, --help
  Display this help message and exit

-i, --interval SECS
  Polling interval
  (default = %d secs) 

-l, --log-directory LOG_DIRECTORY
  Write logs to LOG_DIRECTORY
  (default = %s)

-t, --timeout SECS
  Timeout for requests
  (default = %d seconds)
  
""".strip() % (DEFAULT_INTERVAL, DEFAULT_LOG_DIR, DEFAULT_TIMEOUT)

class Logger:
    def __init__(self, fh, url):
        self.fh = fh
        self.url = url

    def log(self, msg):
        utc = time.gmtime()
        self.fh.write('[%s] %s - %s\n' % (time.asctime(utc), self.url, msg))
        self.fh.flush()

def usage(errcode):
    sys.stderr.write('%s\n' % USAGE)
    sys.exit(errcode)


def main(argv):

    shortopts = 'c:e:hi:l:t:'
    longopts = ['command=', 'error-log:' 'help', 'interval=', 'log-directory=',
            'timeout=']

    try:
        opts, args = getopt.getopt(argv[1:], shortopts, longopts)
    except getopt.GetoptError as err:
        print str(err)
        usage(2)

    error_log_fh = sys.stderr
    error_log = None
    interval = DEFAULT_INTERVAL
    log_dir = DEFAULT_LOG_DIR
    timeout = DEFAULT_TIMEOUT
    command = None

    for o, a in opts:
        if o in ('-c', '--command'):
            command = a
        elif o in ('-e', '--error-log'):
            error_log = a
        elif o in ('-h', '--help'):
            usage(0)
        elif o in ('-i', '--interval'):
            interval = int(a)
        elif o in ('-l', '--log-directory'):
            log_dir = a
        elif o in ('-t', '--timeout'):
            timeout = int(a)
        else:
            assert False, "unhandled option"

    if len(args) != 2:
        usage(1)

    url = 'http://%s:%d/shard' % (args[0], int(args[1]))

    if error_log:
        try:
            error_log_fh = open(error_log, 'a')
        except IOError as e:
            sys.stderr.write('failed to open error log %s: %s\n', error_log,
                    e.message)

    logger = Logger(error_log_fh, url)

    while True:
        try:
            r = requests.get(url, timeout=timeout)
        except requests.ConnectionError as e:
            logger.log('connection error')
        except requests.Timeout as e:
            logger.log('request timeout')
        else:
            if r.status_code == 200:
                # first line is log-file
                idx = r.content.find('\n')
                if idx == -1:
                    logger.log('malformed 200 response (first 64 bytes): %s' % binascii.hexlify(r.content[:64]))
                else:
                    log_name = r.content[:idx]
                    payload = buffer(r.content, idx+1)
                    log_path = os.path.abspath(os.path.join(log_dir, log_name))
                    new_file = True
                    if os.path.exists(log_path):
                        new_file = False
                    try:
                        f = open(log_path, 'a')
                    except IOError as e:
                        logger.log('cannot open log file %s: %s', e.message)
                        logger.log('exiting')
                    f.write(payload)
                    f.close()

                    if new_file and command:
                        if '%s' in command:
                            cmd = command % log_path
                        else:
                            cmd = command
                        print 'starting command: ', cmd
                        subprocess.call(cmd, shell=True)

            elif r.status_code == 204:
                # no data available -- nothing to log
                pass
            elif r.status_code == 401:
                logger.log('401 response')
            elif r.status_code == 404:
                logger.log('404 response')
            else:
                logger.log('unrecognized response code: %d' % r.status_code)

        time.sleep(interval)

if __name__ == '__main__':
    main(sys.argv)
