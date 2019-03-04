#!/usr/bin/env python

import BaseHTTPServer
import fcntl
import getopt
import os
import sys
import time
import urlparse

# BaseHTTPServer.HTTPServer(server_address, RequestHandlerClass) < SocketServer.TCPServer < SocketServer.BaseServer
# 
# BaseHTTPServer.BaseHTTPRequestHandler(request, client_address, server)

BLOCK_SIZE = 512 * (2**10)  # 512 K
DEFAULT_LOG_DIR = '.'

USAGE = """
./logserver [options] PORT

Shard a logfile among multiple workers.

Listen on PORT and and monitor the logfile named yyyy-mm-dd.log
in LOGDIR.  Clients that request /shard that are on the IP address
whitelist get a shard of complete lines from the file.  Non-whitelisted
clients receive a 401 response.  If there is not data yet to send,
client receives a 402.  At the start of each new day UTC, the log
file being monitored changes to yyyy-mm-dd.log for that day.

 -a, --access-log-directory ACCESS_LOG_DIRECTORY
    Write access logs to this directory.  The log has the name
    yyyy-mm-dd.access.log.  A new log is generated each day.

 -h, --help
    Display this help message and exit

 -l, --log-directory LOG_DIRECTORY
    The log directory
    A new log of the form yyyy-mm-dd.log should be produced in this directory
    by some outside process every day.
    (Default = current directory)

 -o, --offset OFFSET
    Start serving the file at a given offset.
    (Default = 0)

 -w, --whitelist CONFIG_FILE
    File containing whitelisted IPs.  If a whitelist is not given, all
    clients are served; if specified, only the clients listed in the
    CONFIG_FILE are served, and all others receive a 401 response.
    As a special case, if the whitelist contains no addresses, all
    clients are served.  The format of the file is one dotted-decimal
    IPv4 address per line.  Blank lines and lines starting with '#'
    are ignored.
""".strip()
    
def set_fileobj_nonblocking(fobj):
    fd = fobj.fileno()
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

class LogHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def _send_status_and_headers(self, status_code):
        # send_response also prints a log line, regretfully
        self.send_response(status_code)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Connection', 'close')
        self.end_headers()

    def _send_200(self, payload):
        self.log_message('log=%s, payload_size=%d, new_offset=%d',
                self.server.log_path, len(payload), self.server.log_offset)
        self._send_status_and_headers(200)
        self.wfile.write('%s\n' % os.path.basename(self.server.log_path))
        self.wfile.write(payload)

    def _send_204(self):
        # the 'no content' response
        self._send_status_and_headers(204)

    def _send_401(self):
        self._send_status_and_headers(401)

    def _send_404(self):
        self._send_status_and_headers(404)


    def _send_shard(self):
        if not self.server.log_path:
            self._send_204()
            return
        
        try:
            f = open(self.server.log_path)
        except IOError:
            self._send_204()
            return

        set_fileobj_nonblocking(f)
        f.seek(self.server.log_offset)
        data = f.read(BLOCK_SIZE)
        self.server.log_offset = f.tell()
        f.close()

        payload = ''
        if data.endswith('\n'):
            payload = self.server.partial + data
            self.server.partial = ''
            self._send_200(payload)
        else:
            idx  = data.rfind('\n')
            if idx == -1:
                # send none
                self.server.partial += data
                self._send_204()
            else:
                # send some
                payload = self.server.partial + data[:idx+1]
                self.server.partial = data[idx+1:]
                self._send_200(payload)

    # override
    def log_message(self, fmt, *args):
        if self.server.access_fh is not None:
            self.server.access_fh.write('%s - - [%s]  %s\n' %
                    (self.client_address[0],
                     self.log_date_time_string(),
                     fmt%args))
            self.server.access_fh.flush()

    def do_GET(self):
        host, port = self.client_address

        #print 'log_path: ', self.server.log_path
        #print 'log_offset: ', self.server.log_offset
        #print 'host: ', host
        #print 'whitelist:\n', '\n'.join(self.server.whitelist)

        if self.server.whitelist:
            if host not in self.server.whitelist:
                self._send_401()
                return

        url = urlparse.urlparse(self.path)
        if url.path == '/shard':
            self._send_shard()
        else:
            self._send_404()


class LogServer(BaseHTTPServer.HTTPServer):
    def __init__(self, server_address, log_dir='.', starting_offset=0, whitelist_path=None, access_dir='.', ):
        self.log_dir = log_dir                  # immutable
        self.log_offset = starting_offset       # updated by _refresh_log_path, request handler
        self.whitelist_path = whitelist_path    # immutable
        self.access_dir = access_dir            # immutable

        self.access_path = ''       # updated by _refresh_access_log
        self.access_fh  = None      # updated by _refresh_access_log
        self.log_path = ''          # updated by _refresh_log_path
        self.partial = ''           # updated by _refresh_log_path
        self.whitelist = []         # _updatedby _refresh_whitelist

        self._refresh()

        BaseHTTPServer.HTTPServer.__init__(self, server_address, LogHandler)

    # updates log_offset, log_path, partial on UTC day rollover
    def _refresh_log_path(self):
        st = time.gmtime() 
        log_name = '%d-%02d-%02d.log' % (st.tm_year, st.tm_mon, st.tm_mday)
        log_path = os.path.join(self.log_dir, log_name)
        if log_path != self.log_path:
            if self.log_path:
                # for the first log, we use starting_offset; subsequent logs
                # start at 0
                self.log_offset = 0
            self.log_path = log_path
            self.log_partial = ''

    # updates access_fh, self.access_path on UTC day rollover
    # access log is calldd yyyy-mm-dd.access.log
    def _refresh_access_log(self):
        st = time.gmtime() 
        log_name = '%d-%02d-%02d.access.log' % (st.tm_year, st.tm_mon, st.tm_mday)
        log_path = os.path.join(self.access_dir, log_name)
        if log_path != self.access_path:
            if self.access_fh:
                # close existing handle
                self.access_fh.close()
                self.access_fh = None
            try:
                self.access_fh = open(log_path, 'a')
            except IOError as e:
                sys.stderr.write("could not open access log '%s': %s" %
                        (log_path, str(e)))
                sys.exit(1)
            else:
                self.access_path = log_path

    # updates whitelist
    # currently performed before every request
    def _refresh_whitelist(self):
        if not self.whitelist_path:
            return

        ips = []
        try:
            f = open(self.whitelist_path)
        except IOError as e:
            sys.stderr.write('failed to open whitelist file: %s\n' % str(e)) 
            sys.exit(1)

        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith('#'):
                continue
            else:
                ips.append(line)

        f.close()
        self.whitelist = ips
    
    def _refresh(self):
        self._refresh_log_path()
        self._refresh_access_log()
        self._refresh_whitelist()

    # override of SocketServer
    def handle_request(self):
        self._refresh()
        BaseHTTPServer.HTTPServer.handle_request(self)

def usage(errcode):
    sys.stderr.write('%s\n' % USAGE)
    sys.exit(errcode)

def main(argv):
    shortopts = 'a:hl:o:w:'
    longopts = ['access-log-directory=', 'help', 'log-directory=', 'offset=', 
            'whitelist=']

    try:
        opts, args = getopt.getopt(argv[1:], shortopts, longopts)
    except getopt.GetoptError as err:
        print str(err)
        usage(2)

    log_dir = '.' 
    access_dir = '.'
    whitelist_path = None
    whitelist = None
    offset = 0

    for o, a in opts:
        if o in ('-a', '--access-log-directory'):
            access_dir = a
        elif o in ('-h', '--help'):
            usage(0)
        elif o in ('-l', '--log-directory'):
            log_dir = a
        elif o in ('-o', '--offset'):
            offset = int(a)
        elif o in ('-w', '--whitelist'):
            whitelist_path = a
        else:
            assert False, "unhandled option %s" % o

    if len(args) != 1:
        usage(1)

    port = int(args[0])
    server = LogServer(('0.0.0.0', port), log_dir, offset, whitelist_path,
            access_dir)


    sa = server.socket.getsockname()
    print 'listening on', sa[0], 'port', sa[1], '...'
    while True:
        server.handle_request()

if __name__ == '__main__':
    main(sys.argv)

