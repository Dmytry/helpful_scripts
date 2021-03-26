#!/usr/bin/python3
import re
import os
import argparse
import subprocess
import time
import multiprocessing
from pathlib import Path
tool = ''
cpucount = multiprocessing.cpu_count()
parser = argparse.ArgumentParser(
    description='Convert all files with a specified extension within a folder',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    epilog='''The tool will recreate the input directory tree at the output, replacing file extensions and names of folders that are exactly equal to input extension.
    For example, if using this to convert png to jpg, input blah/png/image.png will become blah/jpg/image.jpg ''')
parser.add_argument(
    '--in',
    dest='input',
    metavar='PATH',
    required=True,
    help='Input folder to scan recursively')
parser.add_argument(
    '--in_ext',
    metavar='EXTENSION',
    required=True,
    help='Extension of input files')
parser.add_argument(
    '--out',
    dest='output',
    metavar='PATH',
    required=True,
    help='Output folder where to write results')
parser.add_argument(
    '--out_ext',
    metavar='EXTENSION',
    required=True,
    help='Extension of output files.')
parser.add_argument(
    '--dry_run',
    action='store_true',
    help='Dry run (do not run the tool)')
parser.add_argument(
    '--overwrite',
    action='store_true',
    help='Overwrite output files')
parser.add_argument('--tmp', metavar='Temporary extension',
                    help='File extension to use for temporaries')
parser.add_argument(
    '--jobs',
    metavar="NUMBER",
    type=int,
    default=cpucount,
    help='Max number of parallel executions of the command')
parser.add_argument(
    'command',
    metavar='COMMAND_OR_ARGUMENT',
    nargs=argparse.REMAINDER,
    help='Command line and arguments, where {i} is expanded as input path, {o} is expanded as output path.')
args = parser.parse_args()
in_folder = Path(args.input)
out_folder = Path(args.output)
jobs_pool = [None] * args.jobs


def job_gen(cmd, tmp_name=None, final_name=None):
    p = subprocess.Popen(cmd)
    yield p
    while True:
        r = p.poll()
        if r is None:
            yield None
        else:
            if r == 0:
                if tmp_name is not None and final_name is not None:
                    try:
                        tmp_name.rename(final_name)
                    except BaseException:
                        print(f'Failed to rename {tmp_name} to {final_name}')
                yield True
                break
            else:
                yield False
                break


def job_run(pool, cmd, tmp_name=None, final_name=None):
    while True:
        for i, p in enumerate(pool):
            if p is not None:
                rv = next(p[0])
                if rv is not None:
                    p = None
                    pool[i] = None
            if p is None:
                j = job_gen(cmd, tmp_name, final_name)
                pool[i] = [j, next(j)]
                return
        time.sleep(1E-4)


def wait_for_jobs(pool):
    for p in pool:
        if not (p is None):
            p[1].wait()
            next(p[0])


def make_command(in_name, out_name):
    return [a.format(i=in_name, o=out_name) for a in args.command]


dirs = set()

for in_file in in_folder.glob(f'**/*.{args.in_ext}'):
    rel = in_file.relative_to(in_folder)
    parts = []
    for p in rel.parts:
        if p == args.in_ext:
            p = args.out_ext
        parts.append(p)
    out_rel = Path(*parts)
    out = (out_folder / out_rel).with_suffix("." + args.out_ext)
    if (not args.overwrite) and out.exists():
        print(f'{out} exists, skipped (use --overwrite)')
        continue
    if args.tmp:
        tmp_name = out.with_suffix('.' + args.tmp)
        cmd = make_command(in_file, tmp_name)
    else:
        tmp_name = None
        cmd = make_command(in_file, out)
    if args.dry_run:
        print(f'{cmd}')
        if tmp_name is not None:
            print(f'mv {tmp_name} {out}')
    else:
        p = out.parent
        if p not in dirs:
            p.mkdir(parents=True, exist_ok=True)
            dirs.add(out.parent)
        job_run(jobs_pool, cmd, tmp_name, out)

wait_for_jobs(jobs_pool)
