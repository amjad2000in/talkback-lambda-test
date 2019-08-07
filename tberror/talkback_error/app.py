import json
from talkback import TBProject, TBCache


class TBError(TBProject):
    def run_job(self, s3_feedback, debug=False):
        pass


tbcache = TBCache()


def lambda_handler(event, context):
    global tbcache

    tberror = TBError(tbcache)
    tberror.set_event(event, context)

    # in case the source did not, or could not, raise an exception
    exception = Exception(json.dumps(event))

    # must raise an exception to halt the pipeline
    raise exception


def start():
    pass
