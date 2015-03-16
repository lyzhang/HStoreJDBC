#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2009 VoltDB L.L.C.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
import cmd
import socket
from datetime import datetime
from fastserializer import *

class VoltQueryClient(cmd.Cmd):
    TYPES = {"byte": FastSerializer.VOLTTYPE_TINYINT,
             "short": FastSerializer.VOLTTYPE_SMALLINT,
             "int": FastSerializer.VOLTTYPE_INTEGER,
             "long": FastSerializer.VOLTTYPE_BIGINT,
             "float": FastSerializer.VOLTTYPE_FLOAT,
             "string": FastSerializer.VOLTTYPE_STRING,
             "date": FastSerializer.VOLTTYPE_TIMESTAMP}

    TRANSFORMERS = {FastSerializer.VOLTTYPE_TINYINT: eval,
                    FastSerializer.VOLTTYPE_SMALLINT: eval,
                    FastSerializer.VOLTTYPE_INTEGER: eval,
                    FastSerializer.VOLTTYPE_BIGINT: eval,
                    FastSerializer.VOLTTYPE_FLOAT: eval,
                    FastSerializer.VOLTTYPE_STRING: lambda x: x,
                    FastSerializer.VOLTTYPE_TIMESTAMP:
                        lambda x: datetime.fromtimestamp(x)}

    def __init__(self, host, port):
        cmd.Cmd.__init__(self)

        self.__quiet = False
        self.__timeout = None

        self.fs = FastSerializer(host, port)
        self.adhoc = VoltProcedure(self.fs, "@adhoc",
                                   [FastSerializer.VOLTTYPE_STRING])
        self.stat = VoltProcedure(self.fs, "@Statistics",
                                  [FastSerializer.VOLTTYPE_STRING])
        self.shutdown = VoltProcedure(self.fs, "@Shutdown", [])

        self.response = None

    def precmd(self, command):
        return command.decode("utf-8")

    def prepare_params(self, procedure, command):
        params = []
        parsed = command.split()

        if len(parsed) != len(procedure.paramtypes):
            raise SyntaxError("Expecting %d parameters, %d given" %
                              (len(procedure.paramtypes), len(parsed)))

        for i in xrange(len(parsed)):
            transformer = self.__class__.TRANSFORMERS[procedure.paramtypes[i]]
            params.append(transformer(parsed[i]))

        return params

    def safe_print(self, *var):
        if not self.__quiet:
            for i in var:
                print i,
            print

    def set_quiet(self, quiet):
        self.__quiet = quiet

    def set_timeout(self, timeout):
        self.__timeout = timeout

    def do_quit(self, command):
        return True

    def do_exit(self, command):
        return True

    def default(self, command):
        if command == "EOF":
            self.safe_print()
            return True

        self.safe_print("Unknown Command:", command)

    def do_stat(self, command):
        if not command:
            return self.help_stat()

        self.safe_print("Getting statistics")
        self.response = self.stat.call([command])
        self.safe_print(self.response)

    def help_stat(self):
        self.safe_print("Get the statistics:")
        self.safe_print("\tstat procedure")

    def do_adhoc(self, command):
        if not command:
            return self.help_adhoc()

        self.safe_print("Executing adhoc query: %s\n" % (command))
        self.response = self.adhoc.call([command], timeout = self.__timeout)
        self.safe_print(self.response)

    def help_adhoc(self):
        self.safe_print("Execute an adhoc query:")
        self.safe_print("\tadhoc SQL_statement")

    def do_shutdown(self, command):
        self.safe_print("Shutting down the server")
        self.shutdown.call(None, False)

    def help_shutdown(self):
        self.safe_print("Shutdown the server")
        self.safe_print("\tshutdown")

    def do_define(self, command):
        if not command:
            return self.help_define()

        parsed = command.split()
        self.safe_print("Defining stored procedure:", parsed[0])

        if getattr(self.__class__, "do_" + parsed[0], None) != None:
            self.safe_print(parsed[0], "is already defined")

        try:
            method_name = "_".join(["stored", parsed[0]])
            proc_name = "_".join(["procedure", parsed[0]])
            code = """
                def %s(self, command):
                    self.safe_print("Executing stored procedure: %s")
                    try:
                        self.response = self.%s.call(self.prepare_params(self.%s, command))
                        self.safe_print(self.response)
                    except SyntaxError, strerr:
                        self.safe_print(strerr)
                   """ % (method_name, parsed[0], proc_name, proc_name)
            tmp = {}
            exec code.strip() in tmp
            setattr(self.__class__, "do_" + parsed[0], tmp[method_name])

            setattr(self.__class__, proc_name,
                    VoltProcedure(self.fs, parsed[0],
                                  [self.__class__.TYPES[i]
                                   for i in parsed[1:]]))
        except KeyError, strerr:
            self.safe_print("Unsupported type", strerr)
            self.help_define()

    def help_define(self):
        self.safe_print("Define a stored procedure")
        self.safe_print("\tdefine stored_procedure_name param_type_1",
                        "param_type_2...")
        self.safe_print()
        self.safe_print("Supported types", self.__class__.TYPES.keys())

def help(program_name):
    print program_name, "hostname port [command]"

if __name__ == "__main__":
    if len(sys.argv) < 3:
        help(sys.argv[0])
        exit(-1)

    try:
        command = VoltQueryClient(sys.argv[1], int(sys.argv[2]))
    except socket.error:
        sys.stderr.write("Error connecting to the server %s\n" % (sys.argv[1]))
        exit(-1)

    if len(sys.argv) > 3:
        command.onecmd(" ".join(sys.argv[3:]))
    else:
        command.cmdloop("VoltDB Query Client")
