"""
Ctrl-Shift-F10 or right-click and choose Run
to test locally without docker or aws api
"""
import os
os.environ["AWS_PROFILE"] = "talkback"
os.environ["STAGE"] = "stage"
from talkback_error import app

app.start()
