#!/usr/bin/env python

import argparse
import logging
import mailsorter.archive


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--archdir', help='directory in which the archive is kept', required=True)
    parser.add_argument('--build', nargs='+', help='add mboxes in the given files and directories to the archive')
    parser.add_argument('--export', action='store_true', help='export basic message metadata in CSV format')
    args = parser.parse_args()
    if args.build:
        mailsorter.archive.process(args.archdir, *args.build)
    if args.export:
        mailsorter.archive.export(args.archdir)