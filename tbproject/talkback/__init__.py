"""
https://stackoverflow.com/questions/41136359/import-a-python-module-in-multiple-aws-lambdas?rq=1

This is the core module for the talkback project.
The entire talkback python code base has been split into small modules to fit within aws lambda limits.

"""

from .version import __version__

from .TBProject import *
from .StreamingMultipartUpload import *



