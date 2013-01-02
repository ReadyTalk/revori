#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import

import connection as co
import readline, string, os, sys
import argparse

from pydoc import pager

try:
    input = raw_input
except:
    pass

FNAME = os.path.expanduser('~/.revterm_history')

WARNING = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
FAIL = '\033[91m'
ENDC = '\033[0m'

codes = {
    0: OKGREEN,
    1: OKGREEN,
    2: OKGREEN,
    3: OKGREEN,
    4: WARNING
}

class Prompt(object):
    def __init__(self):
        self.database = '<none>'
        self.count = 0
        self.format = string.Template('$database [$count] $prompt_char ')
        self.prompt_char = '>'
        
    def increment(self):
        self.count += 1
        
    def in_copy(self):
        self.prompt_char = '|'
    
    def out_copy(self):
        self.prompt_char = '>'
    
    def __str__(self):
        return OKBLUE + self.format.safe_substitute(self.__dict__) + ENDC

def present_prompt(p):
    try:
        value = input(str(p))
    except EOFError:
        result = 'k'
        
        while len(result) >= 1 and result[0] != 'y' and result[0] != 'n':
            try:
                result = input("\nDo you really want to exit? ([y]/n) ")
            except EOFError:
                result = 'y'
        
        if len(result) == 0 or result[0] == 'y':
            sys.exit(0)
        else:
            return present_prompt(p)
    except KeyboardInterrupt:
        print('')
        return present_prompt(p)
    return value

def generate_output(value):
    output = ''
                
    if isinstance(value.message, str):
        output += value.message
    else:
        for item in value.message:
            output += '\t' + '\t'.join(item) + '\n'
            
    return output

def send(con, msg):
    try:
        con.send(msg)
        return con.recv()
    except Exception as e:
        print('Client encountered fatal error. Exiting...')
        print(e)
        sys.exit(1)
        
        
def send_direct(con, msg):
    try:
        con.send(msg)
    except Exception as e:
        print('Client encountered fatal error. Exiting...')
        print(e)
        sys.exit(1)

def build_parser():
    parser = argparse.ArgumentParser(description='CLI Interface for Revori')
    
    parser.add_argument('-hs', '--host',  type=str, default='localhost')
    parser.add_argument('-p', '--port', type=int, default=8017)
    parser.add_argument('-n', '--lines', type=int, default=25,
        help='Maximum number of lines to display before paging.')
    
    return parser

def parse_args(args):
    parser = build_parser()
    
    return parser.parse_args(args)

def config_readline():
    with open(FNAME, 'a') as f:
        os.utime(FNAME, None)
    
    readline.set_history_length(500)
    readline.read_history_file(FNAME)

def printout(max_lines, result):
    output = generate_output(result)
    if max_lines > 0 and output.count('\n') < max_lines:
        output = codes.get(result.code, ENDC) + output + ENDC
        print(output)
    else:
        pager(output)

def main():
    args = parse_args(sys.argv[1:])
    
    config_readline()
    
    p = Prompt()
    c = co.RevoriConnection(args.host, args.port)
    
    copying = False
    
    print("Welcome to the Revori SQL client interface." + ENDC)
    print('Type "help" to get started.\n' + ENDC);
    try:
        while True:
            value = present_prompt(p)
            
            if value == 'exit' or value == 'quit':
                sys.exit(0)
        
            value = value.strip()
        
            if len(value) > 0:
                if not copying:
                    p.increment()
                
                    result = send(c, value)
                    
                    printout(args.lines, result)
                
                    if hasattr(result, 'database'):
                        p.database = result.database
                    
                    copying = result.copy_context
                    
                    if copying:
                        p.in_copy()
                else:
                    send_direct(c, value)
                    
                    if value == '\\.':
                        copying = False
                        p.out_copy()
                        
                        result = c.recv()
                        output = generate_output(result)
                        printout(args.lines, result)

                    
                    
                        
                        
    finally:
        c.close()
        print(ENDC)
        
        readline.write_history_file(FNAME)
        
if __name__ == '__main__':
    main()
