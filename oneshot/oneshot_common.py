#!/usr/bin/env python3

import sys, os

if sys.version_info[0] < 3:
    raise SystemExit("Python 3 is required.")

sys.path.insert(1, os.path.join(sys.path[0], '..'))
