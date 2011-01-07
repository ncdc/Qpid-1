#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Functions for comparing broker log files, used by cluster_tests.py.

import os, os.path, re, glob
from itertools import izip

def split_log(log):
    """Split a broker log at checkpoints where a member joins.
    Return the set of checkpoints discovered."""
    checkpoint_re = re.compile("Member joined, frameSeq=([0-9]+), queue snapshot:")
    outfile = None
    checkpoints = []
    for l in open(log):
        match = checkpoint_re.search(l)
        if match:
            checkpoint = match.groups()[0]
            checkpoints.append(checkpoint)
            if outfile: outfile.close()
            outfile = open("%s.%s"%(log, checkpoint), 'w')

        if outfile: outfile.write(l)
    if outfile: outfile.close()
    return checkpoints

def filter_log(log):
    """Filter the contents of a log file to remove data that is expected
    to differ between brokers in a cluster. Filtered log contents between
    the same checkpoints should match across the cluster."""
    out = open("%s.filter"%(log), 'w')
    for l in open(log):
        # Lines to skip entirely
        skip = "|".join([
            'local connection',         # Only on local broker
            'UPDATER|UPDATEE|OFFER',    # Ignore update process
            'stall for update|unstall, ignore update|cancelled offer .* unstall',
            'caught up',
            'active for links|Passivating links|Activating links',
            'info Connection.* connected to', # UpdateClient connection
            'warning Broker closed connection: 200, OK',
            'task late',
            'task overran'
            ])
        if re.compile(skip).search(l): continue

        # Regex to match a UUID
        uuid='\w\w\w\w\w\w\w\w-\w\w\w\w-\w\w\w\w-\w\w\w\w-\w\w\w\w\w\w\w\w\w\w\w\w'

        # Regular expression substitutions to remove expected differences
        for pattern,subst in [
            (r'\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d', ''), # Remove timestamp
            (r'cluster\([0-9.: ]*', 'cluster('), # Remove cluster node id
            (r' local\)| shadow\)', ')'), # Remove local/shadow indication
            (r'CATCHUP', 'READY'), # Treat catchup as equivalent to ready.
            # System UUID
            (r'(org.apache.qpid.broker:system[:(])%s(\)?)'%(uuid), r'\1UUID\2'),

            # FIXME aconway 2010-12-20: substitutions to mask known problems
            #(r' len=\d+', ' len=NN'),   # buffer lengths
            #(r' map={.*_object_name:([^,}]*)[,}].*', r' \1'), # V2 map - just keep name
            #(r'\d+-\d+-\d+--\d+', 'X-X-X--X'), # V1 Object IDs
            ]: l = re.sub(pattern,subst,l)
        out.write(l)
    out.close()

def verify_logs(logs):
    """Compare log files from cluster brokers, verify that they correspond correctly."""
    for l in glob.glob("*.log"): filter_log(l)
    checkpoints = set()
    for l in glob.glob("*.filter"): checkpoints = checkpoints.union(set(split_log(l)))
    errors=[]
    for c in checkpoints:
        fragments = glob.glob("*.filter.%s"%(c))
        fragments.sort(reverse=True, key=os.path.getsize)
        while len(fragments) >= 2:
            a = fragments.pop(0)
            b = fragments[0]
            for ab in izip(open(a), open(b)):
                if ab[0] != ab[1]:
                    errors.append("\n    %s %s"%(a, b))
                    break
    if errors:
        raise Exception("Files differ in %s"%(os.getcwd())+"".join(errors))

# Can be run as a script.
if __name__ == "__main__":
    verify_logs(glob.glob("*.log"))
