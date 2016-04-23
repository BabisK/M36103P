#!/usr/local/bin/python2.7
# encoding: utf-8
'''
Part A -- Calculate out degree sequence of RDF graph

@author:     Charalampos Kaidos, Evgenia Stamati

@copyright:  2016 Charalampos Kaidos, Evgenia Stamati. All rights reserved.

@contact:    kaidosc@aueb.gr
'''

import sys
import os
import matplotlib.pyplot
import numpy
import boto3

from collections import Counter
from datetime import datetime

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from inspect import ArgSpec

__all__ = []
__version__ = 0.1
__date__ = '2016-04-10'
__updated__ = '2016-04-18'

DEBUG = 0
TESTRUN = 0
PROFILE = 0

def plot_figs(out_degree_sequence, verbose):
    """Plots figures of out-degree-sequence

    Creates 2 figures;
    first figure plots the out-degree-sequence
    second figure plots the log values of out-degree-sequence

    Both figures are saved on filesystem with names test.png and testlog.png
    respectively
    """
    filename = 'out-degree-sequence.png'
    filename_log = 'log-out-degree-sequence.png'

    out_degree = out_degree_sequence.keys()
    nodes = out_degree_sequence.values()

    if(verbose):
        print 'Plotting out-degree-sequence to file ' + filename
    matplotlib.pyplot.scatter(out_degree,nodes)
    matplotlib.pyplot.suptitle('out degree sequence scatterplot', fontsize=20)
    matplotlib.pyplot.xlabel('out degree', fontsize=18)
    matplotlib.pyplot.ylabel('out degree sequence', fontsize=16)
    matplotlib.pyplot.grid(True)
    matplotlib.pyplot.savefig(filename)
    matplotlib.pyplot.close()

    x = numpy.log(out_degree)
    y = numpy.log(nodes)

    if(verbose):
        print 'Plotting log-out-degree-sequence to file ' + filename_log
    matplotlib.pyplot.scatter(x,y)
    matplotlib.pyplot.suptitle('log of out degree sequence scatterplot', fontsize=20)
    matplotlib.pyplot.xlabel('log out degree', fontsize=18)
    matplotlib.pyplot.ylabel('log out degree sequence', fontsize=16)
    matplotlib.pyplot.grid(True)
    matplotlib.pyplot.savefig(filename_log)

def save_to_file(out_degree_sequence, verbose):
    """Saves out-degree-sequence to file
    """
    filename = 'out-degree-sequence'
    if(verbose):
        print 'Saving out-degree-sequence to file ' + filename
    with open(filename, 'w') as fout:
        for key,value in out_degree_sequence.iteritems():
            fout.write(str(key) + '\t' + str(value) + '\n')

def compute_local(args):
    path = args.localpath
    verbose = args.verbose
    
    file_list = []
    if os.path.isdir(path):
        if(verbose):
            print 'Given path "' + path + '" is a directory. Will read all files in it.'
        file_list = [os.path.join(path,file) for file in os.listdir(path) if os.path.isfile(os.path.join(path,file))]
    elif os.path.isfile(path):
        if(verbose):
            print 'Given path "' + path + '" is a file.'
        file_list.append(path)
    else:
        print('Given path: "%s" is not a directory nor a file' % path)
        raise os.error
    
    subject_list = Counter()
    
    for fileName in file_list:
        if(verbose):
            print 'Reading file: ' + filename
        count = 0
        with open( fileName, "r" ) as fin:
            for line in fin:
                count += 1
                key = line.split(' ', 1)[0]
                subject_list[key] += 1
        print 'File: ' + filename + ' complete: ' + str(count) + ' records processed'

    if(verbose):
        print 'All files complete: ' + str(len(subject_list)) + ' nodes processed'

    out_degree_sequence = Counter(subject_list.itervalues())

    if(verbose):
        print 'Out-degree-sequence calculated: ' + str(len(out_degree_sequence)) + ' entries'
    
    plot_figs(out_degree_sequence, verbose)
    save_to_file(out_degree_sequence, verbose)

def compute_s3(args):
    verbose = args.verbose

    s3 = boto3.resource('s3')
    
    bucket_name = args.s3bucket
    if(verbose):
        print 'Bucket selected: ' + bucket_name
    bucket = s3.Bucket(bucket_name)

    key_name = args.s3key
    if(verbose):
        print 'Key selected: ' + key_name
    
    subject_list = Counter()
    
    if key_name == '*':
        for object in bucket.objects.all():
            if(verbose):
                print 'Reading object: ' + object.key
            leftover = ''
            data = object.get()
            chunk = data['Body'].read(1024*1024)
            count = 0
            while chunk:
                lines = chunk.splitlines(True)
                for line in lines:
                    if(line[-1] == '\n'):
                        count += 1
                        key = line.split(' ', 1)[0]
                        subject_list[key] += 1
                    else:
                        leftover = line
                chunk = data['Body'].read(1024*1024*100)
                if (chunk == '' and leftover != ''):
                    chunk += '\n'
                chunk = leftover + chunk
                leftover = ''
            print 'Object ' + object.key + ' complete: ' + str(count) + ' records processed'
    else:
        leftover = ''
        object = s3.Object(bucket_name, args.s3key)
        if(verbose):
            print 'Reading object: ' + object.key
        data = object.get()
        chunk = data['Body'].read(1024*1024)
        if(verbose):
            print 'Read chunk'
        count = 0
        while chunk:
            lines = chunk.splitlines(True)
            for line in lines:
                if(line[-1] == '\n'):
                    count += 1
                    key = line.split(' ', 1)[0]
                    subject_list[key] += 1
                else:
                    leftover = line
            chunk = data['Body'].read(1024*1024*100)
            if (chunk == '' and leftover != ''):
                chunk += '\n'
            chunk = leftover + chunk
            leftover = ''
        print 'Object ' + object.key + ' complete: ' + str(count) + ' records processed'

    if(verbose):
        print 'All files complete: ' + str(len(subject_list)) + ' nodes processed'

    out_degree_sequence = Counter(subject_list.itervalues())

    if(verbose):
        print 'Out-degree-sequence calculated: ' + str(len(out_degree_sequence)) + ' entries'

    plot_figs(out_degree_sequence, verbose)
    save_to_file(out_degree_sequence, verbose)

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by user_name on %s.
  Copyright 2016 organization_name. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        
        subparsers = parser.add_subparsers(dest='fs')
        parser_s3 = subparsers.add_parser('s3', help='Calculate on data stored on S3 cloud')
        parser_local = subparsers.add_parser('local', help='Calculate on data stored on a local filesystem')
        
        parser_s3.add_argument(dest="s3bucket", help='paths to folder(s) with source file(s) [default: %(default)s]', metavar='bucket')
        parser_s3.add_argument(dest="s3key", help='paths to folder(s) with source file(s) [default: %(default)s]', metavar='key')
        
        parser_local.add_argument(dest="localpath", help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path")

        # Process arguments
        args = parser.parse_args()
        
        verbose = args.verbose
        if verbose > 0:
            print("Verbose mode on")
        
        startTime = datetime.now()
        if args.fs == 's3':
            compute_s3(args)
        elif args.fs == 'local':
            compute_local(args)
        print (datetime.now() - startTime)

        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception, e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    if DEBUG:
        sys.argv.append("-h")
        sys.argv.append("-v")
        sys.argv.append("-r")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'assignment_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())
