import os
import sys
import datetime
import json
import time
from enum import Enum, auto
import base64
import traceback

from urllib.request import url2pathname
from urllib.parse import unquote_plus
import requests
from requests.adapters import BaseAdapter
from botocore.exceptions import ClientError
import boto3
from boto3.dynamodb.conditions import Attr

from talkback.StreamingMultipartUpload import StreamingMultipartUpload


class AutoName(Enum):
    """
    https://stackoverflow.com/questions/44781681/how-to-compare-a-string-with-a-python-enum/44785241#44785241
    """

    def _generate_next_value_(name, start, count, last_values):
        return name


class TBState(Enum):
    """
    Each project (aka swivl project) can be in one of these states
    """
    NONE = auto()
    BROWSER = auto()
    SWIVL = auto()
    UPLOADED = auto()
    TRANSCODER_STARTED = auto()
    TRANSCODED = auto()
    TRANSCRIBE_STARTED = auto()
    TRANSCRIBED = auto()
    TRANSCRIPT_SENTENCES = auto()
    TRANSCRIPT_VTT = auto()
    TALKMOVES_CATEGORIZED = auto()
    TALKMOVES_FEEDBACK = auto()
    TALKMOVES_WORDCLOUD = auto()
    COMPLETED = auto()


class TBFileType(AutoName):
    """
    These are the file types in each project.  The names are case-sensitive
    These are not file extensions.
    They are recorded in s3 metadata['tb-file-type']
    """

    # if flac changes then also update is_ingest_pipeline_trigger()
    flac = auto()  # audio output from transcoder, if swivl did not provide one

    mp4 = auto()  # video or audio file extension
    VIDEO = auto()  # original video
    MIXED_AUDIO = auto()  # swivl mixed track -- all markers
    PRIMARY_AUDIO = auto()  # swivl primary marker
    MARKER_AUDIO = auto()  # other swivl audio markers
    AWS_TRANSCRIPT = auto()  # raw json output from aws transcribe
    SENTENCES_TRANSCRIPT = auto()  # formatted json for input into the talkmoves function
    VTT_TRANSCRIPT = auto()  # formatted .vtt file for pairing with video playing for subtitles.
    TALKMOVES = auto()  # output from the talkmoves function
    TALKMOVES_FEEDBACK = auto()  # output from the feedback function
    TALKMOVES_WORDCLOUD = auto()  # wordcloud output


class LocalFileAdapter(requests.adapters.BaseAdapter):
    """
    Protocol Adapter to allow python Requests package to GET file:// URLs

    https://stackoverflow.com/questions/10123929/python-requests-fetch-a-file-from-a-local-url

    Plus added response headers.

    """

    @staticmethod
    def _chkpath(method, path):
        """Return an HTTP status for the given filesystem path."""
        if method.lower() in ('put', 'delete'):
            return 501, "Not Implemented"
        elif method.lower() not in ('get', 'head'):
            return 405, "Method Not Allowed"
        elif os.path.isdir(path):
            return 400, "Path Not A File"
        elif not os.path.isfile(path):
            return 404, "File Not Found"
        elif not os.access(path, os.R_OK):
            return 403, "Access Denied"
        else:
            return 200, "OK"

    def send(self, req, **kwargs):  # pylint: disable=unused-argument
        """Return the file specified by the given request

        @type req: C{PreparedRequest}
        """
        path = os.path.normcase(os.path.normpath(url2pathname(req.path_url)))
        response = requests.Response()

        response.status_code, response.reason = self._chkpath(req.method, path)
        if response.status_code == 200 and req.method.lower() != 'head':
            try:
                response.raw = open(path, 'rb')
            except (OSError, IOError) as err:
                response.status_code = 500
                response.reason = str(err)

        if isinstance(req.url, bytes):
            response.url = req.url.decode('utf-8')
        else:
            response.url = req.url

        response.request = req
        response.connection = self

        response.headers['Content-Length'] = str(os.path.getsize(path))
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified
        last_modified = datetime.datetime.utcfromtimestamp(os.path.getmtime(path))
        response.headers['Last-Modified'] = last_modified.strftime('%a, %d %b %Y %H:%M:%S GMT')

        return response

    def close(self):
        pass


class TBCache:
    """
    A global object cache reused by warm lambda executions
    """
    # Lazy load these aws boto3 clients by using the @property decorator
    _dynamodb_client = None
    _s3_client = None
    _s3_resource = None
    _transcribe_client = None
    _sns_client = None
    _transcoder_client = None
    _cognito_client = None
    _sfn_client = None
    # dynamodb table
    _project_table = None


class TBProject:
    """
    Big class that has all the utility methods for all the lambda functions
    in an aws step functions pipeline.

    Each lambda function extends TBProject() and overrides run_job(), which performs
    a step in the step function.

    A TBProject subclass is instantiated for each lambda handler call.

    Each step in the step functions goes like this:

    tbfunction = TBFunction()

    tbfunction.process_projects(input_state, output state)

    tbfunction.process_project(project)

    tbfunction.process_file(project_file)

    tbfunction.run_job(project_file)

    There are many utility methods to support this process.

    """

    # https://stackoverflow.com/questions/12409714/python-class-members

    # references a global instance of TBCache to speed up warm lambdas.
    tbcache = None

    # aws secrets like swivl cloud login
    secrets = {}

    # the environment stage, e.g. stage or prod
    stage = None
    is_production = None

    # where to send error messages
    sns_error_arn = 'arn:aws:sns:us-east-1:813736767038:talkback-pipeline-error'

    # extensions must be unique, except _STARTED matches what is started
    state_file_extension = {TBState.TRANSCODER_STARTED: '.' + TBFileType.flac.name,
                            TBState.TRANSCODED: '.' + TBFileType.flac.name,
                            TBState.TRANSCRIBE_STARTED: '-aws-transcript.json',
                            TBState.TRANSCRIBED: '-aws-transcript.json',
                            TBState.TRANSCRIPT_SENTENCES: '-sentences.json',
                            TBState.TRANSCRIPT_VTT: '-sentences.vtt',
                            TBState.TALKMOVES_CATEGORIZED: '-talkmoves.json',
                            TBState.TALKMOVES_FEEDBACK: '-feedback.json',
                            TBState.TALKMOVES_WORDCLOUD: '-wordcloud.png',
                            TBState.COMPLETED: '-completed.json'
                            }

    # file type matches on metadata['tb-file-type'] not extension
    state_file_type = {TBState.SWIVL: TBFileType.VIDEO,
                       TBState.BROWSER: TBFileType.VIDEO,
                       TBState.UPLOADED: TBFileType.VIDEO,
                       TBState.TRANSCODER_STARTED: TBFileType.VIDEO,
                       TBState.TRANSCODED: TBFileType.MIXED_AUDIO,
                       TBState.TRANSCRIBE_STARTED: TBFileType.MIXED_AUDIO,
                       TBState.TRANSCRIBED: TBFileType.AWS_TRANSCRIPT,
                       TBState.TRANSCRIPT_SENTENCES: TBFileType.SENTENCES_TRANSCRIPT,
                       TBState.TRANSCRIPT_VTT: TBFileType.VTT_TRANSCRIPT,
                       TBState.TALKMOVES_CATEGORIZED: TBFileType.TALKMOVES,
                       TBState.TALKMOVES_FEEDBACK: TBFileType.TALKMOVES_FEEDBACK,
                       TBState.TALKMOVES_WORDCLOUD: TBFileType.TALKMOVES_WORDCLOUD,
                       TBState.COMPLETED: TBFileType.TALKMOVES
                       }

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
    # Data that is deleted from Standard - IA within 30 days will be charged for a full 30 days.
    # https://www.amazonaws.cn/en/s3/faqs/
    # https://aws.amazon.com/s3/storage-classes/
    # storage_class = 'STANDARD'
    storage_class = 'STANDARD_IA'

    # step functions move a project from input state to output state
    input_state = TBState.NONE
    output_state = TBState.NONE

    # all required_jobs must complete for project to move to next state
    required_jobs = 0
    completed_jobs = 0

    # save the first exception, which is usually the root exception
    first_job_exception_message = None
    # for error reporting, to aid debugging
    last_log_message = ''

    # to tell the next step what projects to work on (e.g. tb-video-id)
    project_ids_to_next_state = []

    # default lambda function response message
    lambda_response_message = 'OK'
    pipeline_nickname = None

    # https://docs.aws.amazon.com/lambda/latest/dg/python-programming-model-handler-types.html?shortFooter=true
    event = None
    context = None
    start_counter = 0

    # the class constructor
    def __init__(self, set_tbcache):

        # first reference point for the counter
        self.start_counter = time.perf_counter()

        self.stage = os.environ.get('STAGE', 'not found').lower()
        if self.stage == 'not found' or self.stage not in ['stage', 'prod']:
            self.usage()

        if self.stage == 'prod':
            # for easier code reading
            self.is_production = True
        else:
            self.is_production = False

        self.tbcache = set_tbcache

        return

    ###############################################
    # begin: decorators for lazy loading aws boto3 clients

    @property
    def dynamodb_client(self):
        if not self.tbcache._dynamodb_client:
            self.tbcache._dynamodb_client = boto3.client(service_name='dynamodb', region_name=self.get_aws_region())
        return self.tbcache._dynamodb_client

    @property
    def s3_client(self):
        if not self.tbcache._s3_client:
            self.tbcache._s3_client = boto3.client(service_name='s3', region_name=self.get_aws_region())
        return self.tbcache._s3_client

    @property
    def s3_resource(self):
        if not self.tbcache._s3_resource:
            self.tbcache._s3_resource = boto3.resource(service_name='s3', region_name=self.get_aws_region())
        return self.tbcache._s3_resource

    @property
    def transcribe_client(self):
        if not self.tbcache._transcribe_client:
            self.tbcache._transcribe_client = boto3.client(service_name='transcribe', region_name=self.get_aws_region())
        return self.tbcache._transcribe_client

    @property
    def sns_client(self):
        if not self.tbcache._sns_client:
            self.tbcache._sns_client = boto3.client(service_name='sns', region_name=self.get_aws_region())
        return self.tbcache._sns_client

    @property
    def transcoder_client(self):
        if not self.tbcache._transcoder_client:
            self.tbcache._transcoder_client = boto3.client(service_name='elastictranscoder',
                                                           region_name=self.get_aws_region())
        return self.tbcache._transcoder_client

    @property
    def cognito_client(self):
        if not self.tbcache._cognito_client:
            self.tbcache._cognito_client = boto3.client(service_name='cognito-idp', region_name=self.get_aws_region())
        return self.tbcache._cognito_client

    @property
    def sfn_client(self):
        if not self.tbcache._sfn_client:
            self.tbcache._sfn_client = boto3.client(service_name='stepfunctions', region_name=self.get_aws_region())
        return self.tbcache._sfn_client

    @property
    def project_table(self):
        return self.get_table()

    ###############################################
    # end: decorators for lazy loading aws boto3 clients

    ###############################################
    # begin: static methods for constant variables

    @staticmethod
    def get_aws_region():
        """
        All Talkback aws systems are in us-east-1
        :return:
        """
        return 'us-east-1'

    @staticmethod
    def get_bucket_name():
        """
        The s3 bucket name for all talkback pipeline processing
        :return: 'talkback-transcribe'
        """
        return 'talkback-transcribe'

    @staticmethod
    def get_google_project_name():
        """
        Use a familiar name for google projects
        :return:
        """
        return TBProject.get_bucket_name()

    @staticmethod
    def get_none():
        """
        Can't leave dynamodb attributes blank, so use this value.
        :return:  'None'
        """
        return 'None'

    @staticmethod
    def get_date_format():
        """
        :return: date string formatted for easy sorting in dynamodb and elsewhere
        """
        return '%Y-%m-%d'

    @staticmethod
    def get_lambda_response_start():
        """
        This value should be in the Step Function StartAt Task Parameters
        :return:  'START'
        """
        return 'START'

    @staticmethod
    def get_lambda_response_ok():
        """
        The usual lambda response message
        :return:  'OK'
        """
        return 'OK'

    @staticmethod
    def get_lambda_response_continue():
        """
        Tell the last function in the pipeline to restart the pipeline
        :return:  'CONTINUE'
        """
        return 'CONTINUE'

    @staticmethod
    def get_lambda_response_error():
        """
        The lambda function probably will raise an exception and not use this method
        :return:  'ERROR'
        """
        return 'ERROR'

    @staticmethod
    def get_cognito_user_pool_id():
        """
        The id for 'tm-user-pool'
        https://console.aws.amazon.com/cognito/users/?region=us-east-1#/pool/us-east-1_kcQOOJrWl/details?_k=cz31ea
        :return:
        """
        return 'us-east-1_kcQOOJrWl'

    @staticmethod
    def get_cognito_default_group_name():
        """
        The default owner of projects uploaded to /stage/uploads or /prod/uploads.
        https://console.aws.amazon.com/cognito/users/?region=us-east-1#/pool/us-east-1_kcQOOJrWl/groups/default-owner?_k=sux6p6

        Ideally a cognito id is associated with uploads.  But not in the case of testing when we just
        copy / past from /test-videos.

        :return: 'default-owner'
        """
        return 'default-owner'

    @staticmethod
    def get_cognito_domain():
        """
        Aws cognito user pool 'tm-user-pool' App Integration Domain name
        https://console.aws.amazon.com/cognito/users/?region=us-east-1#/pool/us-east-1_kcQOOJrWl/app-integration-domain?_k=slp0yb
        :return:
        """
        return 'talkmoves'

    @staticmethod
    def get_next_id_item_key():
        """
        The dynamo db item key holding the value for the next-id
        :return:
        """
        return {'tb_teacher_id': '0', 'tb_video_id': '0'}

    @staticmethod
    def get_async_job_tag_key():
        """
        Use this tag name when tagging s3 objects to hold an asynchronous job name
        :return:
        """
        return 'tb-async-job'

    @staticmethod
    def get_project_ids_to_next_state():
        return "project_ids_to_next_state"

    ###############################################
    # end: static methods for constant variables

    ###############################################
    # begin: static utility methods

    @staticmethod
    def get_s3_valid_characters():
        """
        # https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
        # Use most restrictive validation for use in job names: [0-9a-zA-Z._-]

        Returns
        -------
        array of valid characters

        """
        return ['_', '-', '.', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
                'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
                'B', 'C',
                'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                'Y', 'Z']

        # invalid characters
        # return ['&', '$', '@', '=', ';', ':', '+', ' ', ',', '?', '\\', '{', '^', '}', '%', '`', '[',
        #        ']', '"', '\'', '>', '<', '~', '#', '|', '(', ')']

    @staticmethod
    def get_project_table_key(s3_object):
        """

        Parameters
        ----------
        s3_object -- s3_object.metadata contains the keys to the dynamodb table

        Returns
        -------
        dict of project_key_attributes for dynamodb_table.update_project()

        """

        # Note the dynamodb names have underscores instead of dashes.
        # I got reserved keyword errors when using dashes.

        project_key_attributes = {'tb_teacher_id': s3_object.metadata['tb-teacher-id'],
                                  'tb_video_id': s3_object.metadata['tb-video-id']
                                  }
        return project_key_attributes

    @staticmethod
    def format_exception(exception):
        formatted_message = ''
        if exception is not None and isinstance(exception, Exception):
            if exception.__traceback__ is not None:
                # neatly captures the stack trace in a message
                formatted_message += str(exception)
                formatted_message += '\n\n'
                for line in traceback.format_tb(exception.__traceback__):
                    formatted_message += line
        return formatted_message

    @staticmethod
    def get_tags(get_object_tagging_response):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object_tagging

        Parameters
        ----------
        get_object_tagging() response

        Returns
        -------
        tags - a dictionary of key,value tags

        """
        tags = {}
        if 'TagSet' in get_object_tagging_response:
            for d in get_object_tagging_response['TagSet']:
                key = d['Key']
                value = d['Value']
                tags[key] = value
        return tags

    @staticmethod
    def get_extension_no_period(s3_object):
        # index 0 is the base file name
        # index 1 is the extension and slice 1:
        return os.path.splitext(s3_object.metadata['tb-file-name'])[1][1:]

    @staticmethod
    def reformat_external_s3_url(source_url):
        """
        Can't access s3 via https, so reformat it
        :param source_url:
        :return: { Bucket: 'bucket-name', 'Key', 'key'}
        """
        if isinstance(source_url, str):
            (bucket, key) = source_url[5:].split('/', 1)
            source_url = {'Bucket': bucket, 'Key': key}

        return source_url
    ###############################################
    # end: static utility methods

    ###############################################
    # begin: main process flow
    # Reference for type checking https://realpython.com/python-type-checking/
    def process_projects(self, set_input_state: TBState, set_output_state: TBState, debug=False) -> []:

        self.input_state = set_input_state
        self.output_state = set_output_state
        self.project_ids_to_next_state = []

        projects = []

        try:
            # Three ways to get project ids for run_job().
            if self.is_digest_pipeline():
                projects = self.get_lambda_prior_projects()

            if len(projects) == 0:
                projects = self.get_async_job_project_code_from_trigger()

            if len(projects) == 0:
                projects = self.get_projects_having_state(self.input_state, debug)
        except Exception as e:
            self.log('process_projects() get_projects because {}', e)
            raise e

        try:
            for project_code in projects:
                if self.process_project(project_code, debug):
                    self.project_ids_to_next_state.append(project_code)
        except Exception as e:
            self.log('process_projects() {} {}', projects, e)
            raise e

        self.log('project ids {} moved from {} to {}', self.project_ids_to_next_state, self.input_state,
                 self.output_state)

        # if this ingest function was triggered by an s3 event then start the ingest pipeline,
        # except if this is the first step of the ingest pipeline
        if self.is_ingest_pipeline_event() and not self.is_start_pipeline():
            self.start_ingest_pipeline()

        return self.project_ids_to_next_state

    def process_project(self, project_code, debug=False):
        """
        if self.required_jobs == self.completed_jobs then move project to next state

        Parameters
        ----------
        project_code:
        debug: False

        Returns
        -------
        completed_project:  True | False


        """
        self.log('entering process project: {}', project_code)
        project_path = self.get_project_path(project_code)
        project_file_keys = self.get_s3_object_keys(project_path)

        project_file = None

        self.required_jobs = 0
        self.completed_jobs = 0
        completed_project = False

        self.first_job_exception_message = None

        # accumulate a directory of s3 objects for the dynamodb column tb_files
        tb_files = {}

        for project_file_key in project_file_keys:
            try:
                project_file = self.s3_resource.Object(self.get_bucket_name(), project_file_key)
                tb_files[project_file_key] = project_file.metadata
                tb_files[project_file_key]['tb-bucket-name'] = self.get_bucket_name()
                self.process_file(project_file, debug)
            except ClientError as e:
                if e.response['Error']['Code'] != "404":
                    self.log('Can not get_object {}', project_file_key)
                    self.log(str(e))

        # if all files in a project are completed change the state
        if 0 < self.completed_jobs == self.required_jobs:
            self.update_project_state(project_file, self.output_state, tb_files)
            completed_project = True
            if debug:
                self.log('Completed project {} for input state {}, next state is {}', project_code, self.input_state,
                         self.output_state)
        else:
            if debug:
                self.log('Incomplete project {} for input state {}, only {} out of {} required jobs', project_code,
                         self.input_state,
                         self.completed_jobs, self.required_jobs)

        self.log('leaving process project: {} {}', project_code, completed_project)
        return completed_project

    def process_file(self, project_file, debug=False):
        if debug:
            self.log('entering process file: {}', project_file.metadata['tb-bucket-key'])

        if self.is_file_type(project_file, self.get_state_file_type(self.input_state)):
            self.log('process_file {}', project_file.metadata)
            self.delete_output_if_exists(project_file, self.output_state)
            try:
                self.log('entering run_job for {}', project_file.metadata['tb-bucket-key'])
                self.run_job(project_file, debug)
            except Exception as e:
                self.log("Error run_job() {} {}", project_file.metadata, e)
                # don't raise exception because we want pipeline to continue
                # processing all files and not get hung up on one.
        else:
            if debug:
                self.log('This file type {} does not match {} by this job state: {}',
                         project_file.metadata['tb-file-type'],
                         self.get_state_file_type(self.input_state),
                         self.input_state)

        if debug:
            self.log('leaving process file: {}', project_file.metadata['tb-bucket-key'])
        return

    def run_job(self, s3_object, debug=True):
        """
        Each lambda function has a class that extends TBProject
        and implements run_job.

        :param s3_object: s3_object
        :param debug: True | False
        :return:
        """

        message = self.log('Must override run_job() for project_file {}', s3_object)
        if debug:
            pass

        raise Exception(message)

    def get_lambda_response(self):

        # message and project_ids_to_next_state are used
        # by the function after this one

        # states and first_job_exception are useful for humans
        # using the AWS Console.

        # pipeline_nickname is used to determine if a function
        # is in a pipeline and which one

        return {

            "statusCode": 200,
            "body":
                {"message": self.may_not_be_none(self.get_lambda_response_message()),
                 "pipeline_nickname": self.may_not_be_none(self.pipeline_nickname),
                 "pipeline_function_name": self.may_not_be_none(self.context.function_name),
                 "input_state": self.may_not_be_none(self.input_state.name),
                 "output_state": self.may_not_be_none(self.output_state.name),
                 TBProject.get_project_ids_to_next_state(): self.project_ids_to_next_state,
                 "first_job_exception": self.may_not_be_none(self.first_job_exception_message)
                 }
        }

    def log(self, message="None", *args):
        """
        Including an Exception will send an alert to the sns queue
        :param message: plain string, formatted string, or exception
        :param args: format arguments and / or exception
        :return: the formatted_message, useful for including in a new exception to be raised
        """

        exception = None
        formatted_message = ''

        if isinstance(message, BaseException):
            exception = message
            formatted_message = '{} {} {}'.format(self.stage.upper(), self.__class__.__name__, exception)
        else:
            if len(args):
                for arg in args:
                    if isinstance(arg, Exception):
                        exception = arg
                    formatted_message = '{} {} {}'.format(self.stage.upper(), self.__class__.__name__,
                                                          message.format(*args))
            else:
                formatted_message = '{} {} {}'.format(self.stage.upper(), self.__class__.__name__, message)

        if exception is not None:
            formatted_message += self.format_exception(exception)
            if self.first_job_exception_message is None:
                self.first_job_exception_message = formatted_message
            self.send_alert(formatted_message)

        # log locally
        print(formatted_message)
        self.last_log_message = formatted_message

        return formatted_message

    def send_alert(self, formatted_message):
        try:
            subject = '{} {} alert (a Talkback AWS Lambda function) '.format(self.stage, self.__class__.__name__)
            self.sns_client.publish(TopicArn=self.sns_error_arn, Message='Last log message\n\n' +
                                                                         self.last_log_message +
                                                                         '\n\nAlert\n\n' +
                                                                         formatted_message, Subject=subject)
        except ClientError as e:
            print(str(e))

    ###############################################
    # end: main process flow

    def usage(self):
        """
        Ensure the os environment variables are set correctly
        :return:
        """
        raise ValueError('Environment: Variables: STAGE: [stage|prod] must be set in template.yaml: {}'.
                         format(self.stage))

    def get_table_name(self):
        """
        Dynamodb table name
        :return: str
        """
        if self.is_production:
            return 'projects'
        elif self.stage == 'stage':
            return 'stage-projects'
        else:
            self.usage()

    def get_model_key(self, file):
        """
        get s3 object key for the model directory containing machine learning models
        and other static objects that sometimes need to be downloaded to the lambda
        function container.  Because they're too big to include in the deployment zip file.
        :param file:
        :return:
        """
        if self.is_production:
            return 'prod/model/' + file
        elif self.stage == 'stage':
            return 'stage/model/' + file
        else:
            self.usage()

    def get_ingest_pipeline(self):
        """
        :return: aws step function arn
        """
        if self.is_production:
            return 'arn:aws:states:us-east-1:813736767038:stateMachine:talkback-pipeline'
        elif self.stage == 'stage':
            return 'arn:aws:states:us-east-1:813736767038:stateMachine:stage-talkback-pipeline'
        else:
            self.usage()

    def get_digest_pipeline(self):
        """
        :return: aws step function arn
        """
        if self.is_production:
            return "arn:aws:states:us-east-1:813736767038:stateMachine:prod-talkback-digest"
        elif self.stage == 'stage':
            return 'arn:aws:states:us-east-1:813736767038:stateMachine:stage-talkback-digest'
        else:
            self.usage()

    def get_transcoder_pipeline_id(self):
        """
        https://console.aws.amazon.com/elastictranscoder/home?region=us-east-1#pipeline-details:1556206155126-iiktis
        :return: talkback's elastic transcoder pipeline id
        """
        if self.is_production:
            return '1556206155126-iiktis'
        elif self.stage == 'stage':
            return '1556206155126-iiktis'
        else:
            self.usage()
        return

    def get_bucket_path_prefix(self):
        """
        s3 bucket path prefix for storing projects under
        :return:
        """
        if self.is_production:
            return 'prod/talkback'
        elif self.stage == 'stage':
            return 'stage/talkback'
        else:
            self.usage()
        return

    def get_uploads_path(self):
        """
        s3 bucket path prefix for /uploads folder
        :return:
        """
        if self.is_production:
            return 'prod/uploads/'
        elif self.stage == 'stage':
            return 'stage/uploads/'
        else:
            self.usage()
        return

    def get_async_job_name(self, s3_audio):
        """
        IMPORTANT: the job name must match the s3 trigger prefix and extension
        that will fire off the next step.

        Any changes here may also change get_async_job_project_code_from_trigger()

        This method generates a different name each time.

        For example:

        stage_talkback_[project video file]_[timestamp]-aws-transcript.json

        Aws allows us to assign job name, but must be unique per account
        https://docs.aws.amazon.com/transcribe/latest/dg/API_StartTranscriptionJob.html

        Google Translate assigns the job name to us
        Parameters
        ----------
        s3_audio:  s3 object or s3 key
        :return: str, used by aws transcode and transcribe as s3 key for output
        """

        # cannot use traditional ':' char in job name
        timestamp = '_{:%Y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now())

        # e.g. -aws-transcript = os.path.splitext('-aws-transcript.json')[0]
        # aws transcode & transcribe will append .flac or .json to the job name and use as s3 key
        extension_prefix = os.path.splitext(self.get_extension(self.output_state))[0]

        if isinstance(s3_audio, str):
            return os.path.splitext(s3_audio)[0].replace('/', '_') + timestamp + extension_prefix
        else:
            return os.path.splitext(s3_audio.key)[0].replace('/', '_') + timestamp + extension_prefix

    def get_async_job_project_code_from_trigger(self):
        """
        Try and extract the project code from the s3 trigger key
        :return: []
        """
        s3_trigger_key = self.get_s3_key_from_event()
        if s3_trigger_key is None:
            return []
        else:
            parts = s3_trigger_key.split('_')
            # e.g. stage talkback project_code base_file_name timestamp extension
            return [parts[2]]

    def get_s3_key_from_event(self, debug=False):
        """
        Remove ++ encoding from s3 keys
        https://stackoverflow.com/questions/44779042/aws-how-to-fix-s3-event-replacing-space-with-sign-in-object-key-names-in-js

        :return: None if no key, else a correct key that will work in TBProject.exists_in_s3(key)
        """

        unquoted_key = None

        if self.event is not None:
            if 'Records' in self.event and 's3' in self.event['Records'][0]:
                quoted_key = self.event['Records'][0]['s3']['object']['key']
                unquoted_key = unquote_plus(quoted_key)
                if debug:
                    self.log('before {} and after {}', quoted_key, unquoted_key)

        return unquoted_key

    def set_async_job_tag(self, s3_object, job_name):
        """
        s3 utility function for adding tags to an s3 object

        :param s3_object:
        :param job_name:
        :return:
        """
        try:
            self.s3_client.put_object_tagging(Bucket=self.get_bucket_name(),
                                              Key=s3_object.metadata['tb-bucket-key'],
                                              Tagging={
                                                  'TagSet': [
                                                      {
                                                          'Key': self.get_async_job_tag_key(),
                                                          'Value': job_name
                                                      }
                                                  ]
                                              })

        except ClientError as e:
            self.log('Error setting TagSet Key = {} and Value = {} for {} having metadata {} because {}',
                     self.get_async_job_tag_key(),
                     job_name,
                     s3_object,
                     s3_object.metadata,
                     e)
            raise e

    def get_async_job_tag_value(self, s3_object):
        """
        s3 utility function for getting tags from an s3 object

        :param s3_object:
        :return: str, job name
        """
        try:
            response = self.s3_client.get_object_tagging(Bucket=self.get_bucket_name(),
                                                         Key=s3_object.metadata['tb-bucket-key'])

            tags = self.get_tags(response)
        except ClientError as e:
            self.log('get_object_tagging for {} because {}', s3_object, str(e))
            raise e

        if self.get_async_job_tag_key() in tags:
            self.log('job_name is {}', tags[self.get_async_job_tag_key()])
            return tags[self.get_async_job_tag_key()]
        else:
            return None

    def delete_async_job_tag(self, s3_object):
        """
        s3 utility function to delete a tag set
        :param s3_object:
        :return:
        """
        try:
            # remove job name tag from s3 object
            self.s3_client.delete_object_tagging(Bucket=self.get_bucket_name(),
                                                 Key=s3_object.metadata['tb-bucket-key'])
        except ClientError as e:
            self.log('Error deleting tag on {} because {}', s3_object, e)
            raise e

    def get_extension(self, state: TBState) -> str:
        """
        get the configured file extension for a given state
        :param state: TBState
        :return: str, e.g. -aws-transcript.json
        """
        self.is_valid_state(state)

        if state in self.state_file_extension:
            return self.state_file_extension[state]
        else:
            message = 'No self.state_file_extension configured for state {}'.format(state)
            self.log(message)
            raise ValueError(message)

    def get_state_file_type(self, state: TBState) -> TBFileType:
        """
        get the configured TBFileType for a given TBState
        :param state: TBState
        :return: TBFileType
        """
        self.is_valid_state(state)

        if state in self.state_file_type:
            return self.state_file_type[state]
        else:
            message = 'No entry in self.state_file_type for state {}'.format(state)
            self.log(message)
            raise ValueError(message)

    def get_project_path(self, project_code) -> str:
        """
        :param project_code:  s3_object or str
        :return: str, s3 object prefix for the project directory holding several files including the video
        """
        # https://stackoverflow.com/questions/1303243/how-to-find-out-if-a-python-object-is-a-string
        if not isinstance(project_code, str):
            # it's an s3 object
            project_code = project_code.metadata['tb-video-id']

        project_path = self.get_bucket_path_prefix() + '/' + project_code

        return project_path

    def get_output_s3_key(self, s3_object, state: TBState, debug=False) -> str:
        """
        Calculates the s3 key for the given s3_object and TBState.
        Useful for lambda functions to determine input / output s3 key
        for a prior / later lambda function state.

        :param s3_object:
        :param state:  TBState
        :param debug:  True|False
        :return: str, an s3 object key
        """
        self.is_valid_state(state)

        if state == TBState.UPLOADED:
            # keep the same extension as the source system
            key = s3_object.metadata['tb-bucket-key-base'] + os.path.splitext(s3_object.metadata['tb-bucket-key'])[1]
        else:
            # change the extension to match the new contents
            key = s3_object.metadata['tb-bucket-key-base'] + self.get_extension(state)

        if debug:
            self.log('output s3 key for s3_object {} and state {} is {}', s3_object.metadata, state, key)
        return key

    def get_output_file(self, s3_object, state: TBState, debug=False):
        """
        Fetches the s3_object for a given s3_object and output TBState

        :param s3_object:
        :param state: TBState
        :param debug:
        :return: s3_object
        """
        s3_key = self.get_output_s3_key(s3_object, state, debug)
        self.log('Output state {} yields s3 key {}', state, s3_key)
        try:
            output_file = self.s3_resource.Object(self.get_bucket_name(), s3_key)
            return output_file
        except ClientError as e:
            self.log('Error getting s3_object for state {} from bucket {} key {} because {}, raising exception',
                     state, self.get_bucket_name(), s3_key, e)
            raise e

    def get_base_file_name(self, s3_object) -> str:
        """
        Strips the path and extension off an s3_object's key

        :param s3_object: Having key stage/talkback/1/ptl_test_1.mp3
        :return: str,  ptl_test_1
        """
        try:
            # index 0 is the base file name
            # index 1 is the extension
            with_extension = os.path.split(s3_object.metadata['tb-file-name'])[1]
            return os.path.splitext(with_extension)[0]
        except Exception as e:
            self.log('get_base_file_name() error from input {} because {}, raising exception', s3_object, e)
            raise e

    def delete_s3_object(self, bucket, key, debug=False):
        """

        :param bucket: full s3:// url or  bucket name str
        :param key:   if bucket is a full s3:// url this is ignored
        :param debug:
        :return: True if deleted or does not exist.  Exception raised otherwise
        """
        try:
            if 's3://' in bucket:
                (bucket, key) = bucket[5:].split('/', 1)

            object_summary = self.s3_resource.ObjectSummary(bucket_name=bucket,
                                                            key=key)
            object_summary.load()
            object_summary.delete()
            self.log('Successfully deleted {} {}', bucket, key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                self.log('Error delete_s3_object bucket {} key {} because {}, raising exception', bucket, key, e)
                raise e
        if debug:
            self.log('Bucket key does not exist, return True: {} {}', bucket, key)
        return True

    def delete_output_if_exists(self, s3_object, state: TBState, debug=False):
        """
        If re-running then delete the output from this step

        Parameters
        ----------
        s3_object
        state
        debug

        Returns
        -------
        True if output exists and was deleted. Also True if output does not exist.
        False if output does exist and could not be deleted

        """

        key = self.get_output_s3_key(s3_object, state, debug)

        return self.delete_s3_object(self.get_bucket_name(), key)

    def get_s3_object_keys(self, path, debug=False):
        """
        :param path: an s3 prefix like stage/talkback/1
        :param debug: True | False
        :return: s3_object_keys -- list []
        """
        response = self.s3_client.list_objects_v2(Bucket=self.get_bucket_name(), Prefix=path)

        # limited to 1000 keys, we expect < 10
        object_keys = []
        if 'Contents' in response:
            object_keys = [x['Key'] for x in response['Contents'] if x['Key'] != path]
            if debug:
                self.log('get_s3_objects({}) found {}', path, len(object_keys))
        else:
            self.log('None of the expected s3 keys found for path {}.  '
                     'Perhaps s3 folder was deleted but not dynamodb item?'
                     '  S3 list_objects response is {}.  Please check logs', path, response, ValueError())

        return object_keys

    def get_performance_seconds(self, state=None):
        """
        time.perf_counter() must be called in the constructor to set the starting point.
        :return:  performance_key,performance_value tuple
        """
        if state is None:
            state = self.output_state

        return 'tb_' + state.name.lower() + '_secs', round(time.perf_counter() - self.start_counter)

    def update_project_state(self, project_file, state: TBState, tb_files=None):
        project_key = self.get_project_table_key(project_file)

        performance_key, performance_value = self.get_performance_seconds()
        if tb_files is not None:
            # in one request, the front end can get all the metadata about the project's s3 bucket objects
            tb_files_json = json.dumps(tb_files)
            project_attributes = {performance_key: performance_value,
                                  'tb_project_state': state.name, 'tb_files': tb_files_json}
        else:
            project_attributes = {performance_key: performance_value,
                                  'tb_project_state': state.name}

        self.update_project_table(project_key, project_attributes)

    def save_project_to_table(self, project_attributes):
        """
        Saves project attributes to dynamodb table
        :param project_attributes
        :return: raises an exception if fails
        """
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item

            # The key attributes must be present.  Any additional attributes will be saved as well
            self.project_table.put_item(Item=project_attributes)
        except ClientError as e:
            self.log('save_project_to_table() failed for {} because {}', project_attributes, e)
            raise e

    def update_project_table(self, key, update_attributes, debug=False):
        """
        Dynamodb item update

        :param key:
        :param update_attributes:
        :param debug:
        :return: if fails raises an exception
        """

        update_expression = 'SET '
        count = 0
        attribute_values = {}
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item
            # The key attributes must be present.  Any additional attributes will be saved as well

            for (k, v) in update_attributes.items():
                if k in key:
                    continue
                # Note the dynamodb names have underscores instead of dashes.
                # I got reserved keyword errors when using dashes.
                # Ensure dashes are replaced with underscore
                k1 = k.replace('-', '_')
                k2 = ':{}'.format(count)
                if count > 0:
                    update_expression += ','
                update_expression += k1 + '=' + k2
                k2 = ':{}'.format(count)
                attribute_values[k2] = v
                count += 1

            if debug:
                self.log('update_project_table keys {} UpdateExpression {} ExpressionAttributeValues {}',
                         key, update_expression, attribute_values)

            self.project_table.update_item(Key=key,
                                           UpdateExpression=update_expression,
                                           ExpressionAttributeValues=attribute_values)
        except ClientError as e:
            self.log('update_project_table failed for keys {} expr {} values {} because {}',
                     key, update_expression, attribute_values, e)
            raise e

    def get_table(self):
        """
        Returns cached table object if exists.
        If table does not exist this method creates it.
        And populates the 'tb_next_id' item starting at 0.
        :return: a dynamodb table object
        """

        if self.tbcache._project_table:
            return self.tbcache._project_table

        # Invalid length for parameter ExclusiveStartTableName, value: 1, valid range: 3-inf
        existing_tables = self.dynamodb_client.list_tables(ExclusiveStartTableName=self.get_table_name()[0:4])[
            'TableNames']

        if self.get_table_name() not in existing_tables:
            self.tbcache._project_table = self.create_table()
            # bootstrap table with counter for video ids
            project_attributes = {'tb_next_id': 0}
            self.update_project_table(TBProject.get_next_id_item_key(), project_attributes)
        else:
            self.tbcache._project_table = self.get_existing_table()

        return self.tbcache._project_table

    def get_existing_table(self, debug=False):
        """
        Returns dynamodb table object if exists, else None
        :return: a dynamodb table object or None
        """
        try:

            dynamodb_resource = boto3.resource(service_name='dynamodb', region_name=self.get_aws_region())

            # Instantiate a table resource object without actually
            # creating a DynamoDB table. Note that the attributes of this table
            # are lazy-loaded: a request is not made nor are the attribute
            # values populated until the attributes
            # on the table resource are accessed or its load() method is called.
            existing_db_table = dynamodb_resource.Table(self.get_table_name())

            # Get some data about the table, like existing_db_table.creation_date_time
            # This will cause a request to be made to DynamoDB and its attribute
            # values will be set based on the response.
            if debug:
                self.log('Found table \'{}\' created on {}', self.get_table_name(),
                         existing_db_table.creation_date_time)
        except ClientError as e:
            self.log('get_existing_table(): {}', e)
            return None

        return existing_db_table

    def create_table(self):
        """
        Creates the project's dynamodb table.
        :return: a dynamo db table object or raises an exception
        """
        # Avoid long list of reserved words in AttributeName
        # NB: the attribute name cannot even begin with a reserved word, like project!
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html

        # The AttributeDefinitions contain only key attributes, not all attributes
        # https://stackoverflow.com/questions/30866030/number-of-attributes-in-key-schema-must-match-the-number-of-attributes-defined-i

        """
        create_table(): An error occurred (ValidationException) when calling the CreateTable operation:
        One or more parameter values were invalid: SSEType AES256 is not supported
        create_table(): An error occurred (ValidationException) when calling the CreateTable operation:
        1 validation error detected: Value 'AES-256' at 'sSESpecification.sSEType' failed to satisfy constraint:
         Member must satisfy enum value set: [AES256, KMS]
        Seems we can't create the table here.  Manually creating via console.

        Aha!  DynamoDB encryption is now default.

        Remove
        ,
                SSESpecification={
                    "Enabled": True,
                    "SSEType": 'AES256'
                }
        """

        try:

            self.dynamodb_client.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'tb_teacher_id',
                        'AttributeType': 'S',
                    },
                    {
                        'AttributeName': 'tb_video_id',
                        'AttributeType': 'S',
                    }
                ],
                TableName=self.get_table_name(),
                KeySchema=[
                    {
                        'AttributeName': 'tb_teacher_id',
                        'KeyType': 'HASH',
                    },
                    {
                        'AttributeName': 'tb_video_id',
                        'KeyType': 'RANGE',
                    }
                ],
                BillingMode='PAY_PER_REQUEST'
            )
        except ClientError as e:
            self.log('create_table(): error because {}', e)
            raise e

        # Wait until the table exists.
        waiter = self.dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=self.get_table_name())

        return self.get_existing_table()

    def get_projects_having_state(self, state: TBState, debug=False):
        """
        A simple list [] of tb_video_ids who's projects have the desired state

        :param state: TBState
        :param debug: True | False
        :return: []
        """
        self.is_valid_state(state)

        return self.get_projects_having_attribute_value('tb_project_state', state.name, debug)

    def get_projects_having_attribute_value(self, attribute, value, debug=False):
        """
        A simple list [] of tb_video_ids having the supplied attribute = value

        :param attribute:  e.g. TBState.name
        :param value:  e.g. UPLOADED
        :param debug: True | False
        :return: []
        """
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#ref-dynamodb-conditions

        try:

            json_response = self.project_table.scan(FilterExpression=Attr(attribute).eq(value),
                                                    ProjectionExpression='tb_video_id')
            if debug:
                self.log('project_table.scan {} = {} results: {}', attribute, value, json_response['Items'])
        except ClientError as e:
            self.log('get_projects_having_attribute_value({},{}): {}', attribute, value, e)
            return []

        if json_response['Items']:
            # e.g. 'Items': [{'tb_video_id': '2871824'}]
            project_codes = [x['tb_video_id'] for x in json_response['Items']]
        else:
            project_codes = []

        self.log('get_projects_having_attribute_value({},{}): found {}', attribute, value, len(project_codes))

        return project_codes

    def get_project_attributes(self, s3_object, debug=False):
        """
        Get the dynamodb item corresponding to this s3_object

        :param s3_object:  s3_object or s3_object.metadata (dict)
        :param debug:
        :return: {}
        """
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#ref-dynamodb-conditions
        if isinstance(s3_object, dict):
            key = s3_object
        else:
            key = self.get_project_table_key(s3_object)

        try:
            response = self.project_table.get_item(Key=key,
                                                   ConsistentRead=False,
                                                   ReturnConsumedCapacity='NONE')

            # example query vs get_item
            # json_response = self.project_table.query(
            #    KeyConditionExpression=Key('tb_teacher_id').eq(tb_teacher_id) & Key('tb_video_id').eq(
            #        tb_video_id))
        except ClientError as e:
            self.log('Error project_table.get_item results for key {}: {}', key, e)
            return []

        if response['Item']:
            project_attributes = response['Item']
            if debug:
                self.log('project_table.get_item results for key {}: {}', key, response)
        else:
            self.log('No get_item results for key {} because {}', key, response)
            project_attributes = {}

        return project_attributes

    def get_next_video_id(self, debug=False) -> str:
        """
        Uses DynamoDB Atomic Counter per
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.AtomicCounters

        aws --profile=talkback --region=us-east-1 dynamodb update-item \
        >     --table-name stage-projects \
        >     --key '{"tb_teacher_id":{"S":"0"},"tb_video_id": {"S":"0"}}' \
        >     --update-expression "SET tb_next_id = tb_next_id + :incr" \
        >     --expression-attribute-values '{":incr": {"N":"1"}}' \
        >     --return-values UPDATED_NEW
        {
            "Attributes": {
                "tb_next_id": {
                    "N": "2"
                }
            }
        }

        The boto3 ExpressionAttributeValues is not the same as the command line ExpressionAttributeValues.
        :return:  integer
        """
        try:
            response = self.project_table.update_item(Key=TBProject.get_next_id_item_key(),
                                                      UpdateExpression='SET tb_next_id = tb_next_id + :incr',
                                                      ExpressionAttributeValues={':incr': 1},
                                                      ReturnValues='UPDATED_NEW')

            if debug:
                self.log('get_next_video_id response is {}', response)
            # {'Attributes': {'tb_next_id': Decimal('4')}
            next_id = response['Attributes']['tb_next_id']
        except ClientError as e:
            self.log('Error getting next video id because {}', e)
            raise e

        return str(next_id)

    def is_valid_state(self, state: TBState):
        """
        Validates state to be a TBState
        :param state: TBState
        :return: True | False
        """
        if isinstance(state, TBState):
            return True

        message = 'Invalid state {} not in {}'.format(state, TBState)
        self.log(message)
        raise ValueError(message)

    def is_valid_file_type(self, file_type: TBFileType):
        """
        Validates file_type to be a TBFileType

        :param file_type: TBFileType
        :return: True | False
        """
        if isinstance(file_type, TBFileType):
            return True

        message = 'Invalid file_type {} not in {}'.format(file_type, TBFileType)
        self.log(message)
        raise ValueError(message)

    def is_file_type(self, s3_object, file_types):
        """
        Does the file type of this s3_object, as stored in metadata[tb-file-type],
        match the desired TBFileType type or types.

        :param s3_object:
        :param file_types:  TBFileType or  TBFileType array
        :return: True if there is a match, otherwise False
        """
        if isinstance(file_types, list):
            for file_type in file_types:
                if self.is_valid_file_type(file_type):
                    # https://stackoverflow.com/questions/44781681/how-to-compare-a-string-with-a-python-enum
                    # be lenient and compare using lower case, in case enum changes after metadata created
                    if s3_object.metadata['tb-file-type'].lower() == file_type.name.lower():
                        return True
        else:
            if self.is_valid_file_type(file_types):
                if s3_object.metadata['tb-file-type'].lower() == file_types.name.lower():
                    return True

        return False

    def get_secret(self, secret_name):
        """
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html

        Parameters
        ----------
        secret_name

        Returns
        -------
        A dictionary of secrets

        """
        if secret_name in self.secrets:
            return self.secrets.get(secret_name)

        # not in cache, must go get it

        #   Create a Secrets Manager client
        session = boto3.session.Session()

        client = session.client(service_name='secretsmanager', region_name=self.get_aws_region())

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        except ClientError as e:
            self.log(
                'Can not find secret_name {0} using AWS_PROFILE_NAME {1} in AWS_REGION {2}',
                secret_name,
                os.environ["AWS_PROFILE"],
                self.get_aws_region())
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                return None
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                return None
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                return None
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                return None
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                return None
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret_json = get_secret_value_response['SecretString']
                secret = json.loads(secret_json)

                self.secrets[secret_name] = secret
                return secret
            else:
                decoded_binary_secret_json = base64.b64decode(get_secret_value_response['SecretBinary'])
                secret = json.loads(decoded_binary_secret_json)
                self.secrets[secret_name] = secret
                return secret

    def exists_in_s3(self, s3_key, debug=False):
        """
        Does the s3 object key actually exist in s3?

        :param s3_key: containing full s3 key'
        :param debug:
        :return:  True if exists, False if doesn't exist.  Exception raised otherwise.
        """
        try:
            object_summary = self.s3_resource.ObjectSummary(bucket_name=self.get_bucket_name(),
                                                            key=s3_key)
            object_summary.load()
            if debug:
                self.log('exists_in_s3() object already in bucket, assuming existing object is OK: {}',
                         s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '403':
                self.log('If running locally, ensure aws configure is set for Talkback project: {}', e)
                raise e
            if e.response['Error']['Code'] != '404':
                self.log('Error checking if s3 object exists: {} error {}', s3_key, e)
                raise e
            if debug:
                self.log('exists_in_s3() object not in bucket: {}',
                         s3_key)

        return False

    def convert_https_bucket_link_to_s3(self, source_url):
        """
        Convert external https s3 link to an internal bucket/key link

        :param source_url: example https://s3.amazonaws.com/talkback-transcribe/
        stage_talkback_3_Paul_test_1_2019-07-31_20-00-57-aws-transcript.json

        :return: {'Bucket': 'talkback-transcribe',
        'Key': 'stage_talkback_3_Paul_test_1_2019-07-31_20-00-57-aws-transcript.json'}
        """

        prefix = 'https://s3.amazonaws.com/' + self.get_bucket_name() + '/'
        if prefix in source_url:
            key = source_url[len(prefix):]
            source_url = {'Bucket': self.get_bucket_name(), 'Key': key}

        return source_url

    def upload_output_to_s3(self, s3_input, source_url, source_system, source_version, state=None):
        """
        Several lambda functions produce output in the lambda container which must be uploaded
        to the project's s3 bucket / path.

        :param s3_input: source of metadata to be copied to new uploaded s3 metadata
        :param source_url: the actual source location of the new s3 object to be uploaded

        When the source_url is another s3 bucket:
        This form will work:  source_url = 's3://' + self.get_bucket_name() + '/' + s3_key
        This form is best: source_url = {'Bucket': self.get_bucket_name(), 'Key': s3_key}
        This https form will _not_ work.  It will be parsed and converted to one of the above:
        https://s3.amazonaws.com/talkback-transcribe/stage_talkback_3_Paul_test_1_2019-07-31_20-00-57-aws-transcript.json

        :param source_system: saved in uploaded s3 metadata
        :param source_version: saved in uploaded s3 metadata
        :param state: TBState, defaults to output_state which determines 'tb-file-type' saved in metadata
        :return:
        """

        metadata = []

        source_url = self.convert_https_bucket_link_to_s3(source_url)

        try:
            if state is None:
                state = self.output_state

            # copy tb- metadata from input transcript file
            metadata = {k: v for (k, v)
                        in s3_input.metadata.items() if k.startswith('tb-')}

            # metadata for the new prediction file
            metadata['tb-source-system'] = source_system
            metadata['tb-source-version'] = source_version

            metadata['tb-file-type'] = self.get_state_file_type(state).name

            metadata['tb-bucket-key'] = self.get_output_s3_key(s3_input, state)

            metadata['tb-file-name'] = os.path.split(metadata['tb-bucket-key'])[1]

            http_session = requests.Session()
            s3_object = self.save_url_to_s3(http_session, metadata, source_url)
            http_session.close()

            self.log('uploaded {} to {}', source_url, metadata['tb-bucket-key'])
        except ClientError as e:
            self.log('Error uploading run_job output {} to {} because {}', source_url, metadata, e)
            raise e

        return s3_object

    def save_url_to_s3(self, http_session, metadata, source_url, debug=False):
        """

        Parameters
        ----------
        http_session: -- May be None if source_url is from s3
        metadata
        source_url: http://, https://, file://, s3://, or { 'Bucket': 'bucket', 'Key': 'key' }
        debug

        Returns
        -------
        s3 object - file successfully uploaded or raises an Exception
        """

        if debug:
            self.log('Entering save_url_to_s3(): {}', source_url)

        # An error occurred (MetadataTooLarge) when calling the PutObject operation:
        # Your metadata headers exceed the maximum allowed metadata size
        # The user-defined metadata is limited to 2 KB in size
        # https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

        # metadata limited to 2k and source urls are very long and not needed
        if 'tb-source-url' in metadata:
            del metadata['tb-source-url']

        # validate metadata
        for key, value in metadata.items():
            if value is None:
                message = 's3.metadata[] value for key {} cannot be None.'.format(key)
                raise ValueError(message)

        if isinstance(source_url, dict) or 's3://' in source_url:
            try:
                # destination object
                s3_object = self.s3_resource.Object(self.get_bucket_name(), metadata['tb-bucket-key'])

                source_url = TBProject.reformat_external_s3_url(source_url)

                response = s3_object.copy_from(CopySource=source_url,
                                               Metadata=metadata,
                                               MetadataDirective='REPLACE',
                                               StorageClass=self.storage_class
                                               )

                if debug:
                    self.log('s3 copy_from {}', response)
                    self.log('now wait for s3 to finish copy')

                waiter = self.s3_client.get_waiter('object_exists')
                waiter.wait(Bucket=self.get_bucket_name(),
                            Key=metadata['tb-bucket-key'])

                return s3_object

            except ClientError as e:
                self.log('s3 copy_from {} {}', source_url, e)
                raise e
        else:

            try:
                # see LocalFileAdapter() class at the bottom
                http_session.mount('file://', LocalFileAdapter())

                # Object does not exist, copy from source-url
                http_response = http_session.get(source_url, stream=True)
                if http_response.ok:

                    metadata['tb-content-length'] = http_response.headers['Content-Length']
                    metadata['tb-last-modified'] = http_response.headers['Last-Modified']

                    metadata_limit = 2048
                    metadata_size = sys.getsizeof(metadata)

                    if debug:
                        boto3.set_stream_logger(name="botocore")
                        self.log('origin http_response.headers: {}', http_response.headers)
                        self.log('origin_content_length = {}', http_response.headers['Content-Length'])
                        self.log('origin_last_modified={}', http_response.headers['Last-Modified'])
                        self.log('metadata size={}', metadata_size)
                        self.log('metadata {}', metadata)

                    if metadata_size >= metadata_limit:
                        message = "metadata size {} is => limit {}".format(metadata_size, metadata_limit)
                        self.log(message)
                        raise ValueError(message)

                    megabyte = 1024 * 1024
                    if int(metadata['tb-content-length']) > 128 * megabyte:
                        # stream chunks of data with multipart uploading in constant ram

                        mpu = StreamingMultipartUpload(
                            self.get_bucket_name(),
                            metadata['tb-bucket-key'],
                            http_response,
                            metadata,
                            int(metadata['tb-content-length']),
                            128 * megabyte,
                            self.storage_class,
                            self.get_aws_region(),
                            True)

                        s3_object = mpu.go()

                    else:

                        # download to ram then upload
                        s3_object = self.s3_resource.Object(self.get_bucket_name(), metadata['tb-bucket-key'])
                        put_response = s3_object.put(Body=http_response.content,
                                                     StorageClass=self.storage_class,
                                                     Metadata=metadata,
                                                     ServerSideEncryption='AES256')

                        if debug:
                            self.log('put_response: {}', put_response['ResponseMetadata'])

                        if put_response['ResponseMetadata']['HTTPStatusCode'] != 200:
                            self.log('save_url_to_s3() Error in HTTPStatusCode: {}',
                                     put_response['ResponseMetadata']['HTTPStatusCode'])
                            message = 'put failure uploading {} because {} and {}'.format(
                                metadata,
                                put_response,
                                http_response.headers)
                            raise requests.exceptions.HTTPError(message)

                    # update the attributes
                    s3_object.load()

                    if debug:
                        """destination attributes:
                            content length
                            13180201
                            last modified
                            2019-01-29 04:22:27+00:00
                        """
                        self.log('destination attributes:')
                        self.log('destination_content_length {}', s3_object.content_length)
                        self.log('destination_last_modified {}', s3_object.last_modified)

                    if s3_object.content_length == int(metadata['tb-content-length']):
                        return s3_object
                    else:
                        # back out s3 object
                        message = 'destination length {} != source length {}. deleting destination s3 object {}'.format(
                            metadata['tb-content-length'],
                            s3_object.content_length,
                            metadata
                        )
                        self.log(message)
                        try:
                            s3_object.delete()
                        except ClientError as e:
                            self.log('save_file_to_s3(): {}', e)

                        raise requests.RequestException(message)

                else:
                    message = 'save_file_to_s3() error downloading from source: {} status {} {}'.format(
                        source_url,
                        http_response.status_code,
                        http_response.headers)
                    self.log(message)
                    raise requests.RequestException(message)
            except requests.RequestException as e:
                self.log('save_url_to_s3() Error {} saving {}', e, source_url)
                raise e

    def set_event(self, event, context, debug=False):
        """
        Save the lambda event and context in the class variables
        for later examination.

        :param event:
        :param context:
        :param debug:
        :return:
        """
        self.event = event
        self.context = context

        if self.event is not None:
            self.log('event {}', self.event)

        self.set_pipeline_nickname()
        self.set_lambda_response_message()

        if debug:
            pass

    def set_pipeline_nickname(self):
        """
        Help determine if a function is in a pipeline and which one.
        Be sure and use Parameters in the StartAt Task of a step function definition:

       "Parameters":             {"body":
                {
                    "message": "START",
                    "pipeline_nickname": "ingest"
                }
            }
 ,

 OR

        "Parameters":             {"body":
                {
                    "message": "START",
                    "pipeline_nickname": "digest"
                }
            }
 ,
        :return: CONTINUE OR OK
        """
        if self.event is not None and 'body' in self.event and 'pipeline_nickname' in self.event['body']:
            self.pipeline_nickname = self.event['body']['pipeline_nickname']
        elif self.is_digest_pipeline_event():
            self.pipeline_nickname = 'digest'
        elif self.is_ingest_pipeline_schedule() or self.is_ingest_pipeline_event():
            self.pipeline_nickname = 'ingest'
        else:
            self.pipeline_nickname = None

        self.log('pipeline_nickname is {}', self.pipeline_nickname)

        return self.pipeline_nickname

    def is_digest_pipeline(self):

        result = False
        if self.pipeline_nickname is not None and 'digest' in self.pipeline_nickname:
            result = True

        self.log('is_digest_pipeline is {}', result)
        return result

    def is_ingest_pipeline(self):

        result = False
        if self.pipeline_nickname is not None and 'ingest' in self.pipeline_nickname:
            result = True

        self.log('is_ingest_pipeline is {}', result)
        return result

    def is_digest_pipeline_event(self):
        """
        Determine if this function was started via s3 trigger
        and if that trigger is for the digest pipeline
        :return: True | False
        """
        result = False
        if self.event is not None and 'Records' in self.event:
            s3_key = self.event['Records'][0]['s3']['object']['key']
            if '-transcript.json' in s3_key:
                self.log('triggered by {}', s3_key)
                result = True

        self.log('is_digest_pipeline_event is {}', result)
        return result

    def is_ingest_pipeline_event(self):
        """
        Determine if this function was started via s3 event
        and is that trigger for the ingest pipeline

        Help determine if the function should process one
        file (aka the s3 key in the event).

        :return: True | False
        """
        result = False
        if self.event is not None and 'Records' in self.event:
            s3_key = self.event['Records'][0]['s3']['object']['key']
            if '/uploads/' or '.flac' in s3_key:
                self.log('triggered by {}', s3_key)
                result = True

        self.log('is_ingest_pipeline_event is {}', result)
        return result

    def is_ingest_pipeline_schedule(self):
        """
        Help determine if the function should process all projects
        in the current input state.

        :return: True | False
        """

        result = False
        if self.event is not None and 'detail-type' in self.event and 'Scheduled Event' in self.event['detail-type']:
            self.log('triggered by Scheduled Event')
            result = True

        self.log('is_ingest_pipeline_schedule is {}', result)
        return result

    def get_lambda_response_message(self):
        """
        :return: OK, CONTINUE, or ERROR
        """
        if self.first_job_exception_message is not None:
            # change the message to ERROR
            return self.get_lambda_response_error()
        elif self.lambda_response_message is None or self.is_start_pipeline():
            # change the message to OK
            return self.get_lambda_response_ok()
        else:
            # pass along the prior function's message
            return self.lambda_response_message

    def set_lambda_response_message(self):
        """
        set the message passed by the prior function
        """
        if self.event is not None and 'body' in self.event and "message" in self.event['body']:
            self.lambda_response_message = self.event['body']['message']
        else:
            self.lambda_response_message = None

    def may_not_be_none(self, string_object):
        if string_object is None:
            return self.get_none()
        else:
            return string_object

    def get_lambda_prior_projects(self):
        """
        Must use the project list if sent by prior function
        :return: []
        """
        if self.event is not None and 'body' in self.event \
                and TBProject.get_project_ids_to_next_state() in self.event['body']:
            return self.event['body'][TBProject.get_project_ids_to_next_state()]
        else:
            return []

    def is_continue_pipeline(self):
        """
        Can the end of the pipeline restart the pipeline?
        :return: True | False
        """
        result = False
        if self.lambda_response_message is not None and \
                self.lambda_response_message == self.get_lambda_response_continue():
            result = True

        self.log('is_continue_pipeline is {}', result)
        return result

    def is_start_pipeline(self):
        """
        Is this the first function in the pipeline?
        :return: True | False
        """
        result = False
        if self.lambda_response_message is not None and \
                self.lambda_response_message == self.get_lambda_response_start():
            result = True

        self.log('is_start_pipeline is {}', result)
        return result

    def is_function_in_pipeline(self):
        """
        Is this function being run as a step in the pipeline?
        :return: True | False
        """
        result = False
        if self.pipeline_nickname is not None:
            result = True

        self.log('is_function_in_pipeline is {}', result)
        return result

    def download_resources(self, resources, debug=False):
        """
        Download resources too big for the zip deployment file into the lambda container's /tmp folder.
        :param resources: str, simple name, not a path of the desired resource
        :param debug:
        :return:
        """
        library_directory = '/tmp'
        sys.path.append(library_directory)

        if debug:
            self.log("sys.path should have {} at the end: {}", library_directory, sys.path)

        resource_paths = {}

        for library_name in resources:
            library_full_path = library_directory + '/' + library_name
            resource_paths[library_name] = library_full_path
            library_s3_key = self.get_model_key(library_name)  # remember, no leading slash!

            # only download if resource not there
            exists = os.path.isfile(library_full_path)
            if not exists:
                try:
                    self.s3_resource.Bucket(self.get_bucket_name()).download_file(library_s3_key,
                                                                                  library_full_path)
                except ClientError as e:
                    self.log("Error downloading {} to {}: {}", library_s3_key, library_full_path, e)
                    raise e

        return resource_paths

    def describe_user_pool(self):
        """
        Test function to see what the responses are like for each of these cognito methods.
        :return:
        """
        try:
            response = self.cognito_client.describe_user_pool_domain(Domain=TBProject.get_cognito_domain())
            self.log('Cognito User Pool Domain {} is {}', TBProject.get_cognito_domain(), response)

            response = self.cognito_client.describe_user_pool(UserPoolId=TBProject.get_cognito_user_pool_id())
            self.log('UserPool {} is {}', TBProject.get_cognito_user_pool_id(), response)

            response = self.cognito_client.get_group(GroupName=TBProject.get_cognito_default_group_name(),
                                                     UserPoolId=TBProject.get_cognito_user_pool_id())
            self.log('Group {} is {}', TBProject.get_cognito_default_group_name(), response)

            response = self.cognito_client.list_identity_providers(UserPoolId=TBProject.get_cognito_user_pool_id(),
                                                                   MaxResults=60,
                                                                   NextToken='12345678900987654321')
            self.log('IdentifyProviders for UserPool {} are {}', TBProject.get_cognito_user_pool_id(), response)

            response = self.cognito_client.list_resource_servers(UserPoolId=TBProject.get_cognito_user_pool_id(),
                                                                 MaxResults=50,
                                                                 NextToken='12345678900987654321')
            self.log('Resource Servers for UserPool {} are {}', TBProject.get_cognito_user_pool_id(), response)

            response = self.cognito_client.list_user_pool_clients(UserPoolId=TBProject.get_cognito_user_pool_id(),
                                                                  MaxResults=50,
                                                                  NextToken='12345678900987654321')
            self.log('User Pool Clients for UserPool {} are {}', TBProject.get_cognito_user_pool_id(), response)

            response = self.cognito_client.list_users(UserPoolId=TBProject.get_cognito_user_pool_id(),
                                                      AttributesToGet=['email'],
                                                      Limit=3,
                                                      Filter='email = "plittle@ptlittle.com"')
            self.log('Users in UserPool {} are {}', TBProject.get_cognito_user_pool_id(), response)

            response = self.cognito_client.list_users_in_group(UserPoolId=TBProject.get_cognito_user_pool_id(),
                                                               GroupName=TBProject.get_cognito_default_group_name(),
                                                               Limit=1)
            self.log('Users in Group {} are {}\nincluding {}', TBProject.get_cognito_default_group_name(),
                     response,
                     response['Users'][0]['Username'])

        except ClientError as e:
            self.log('Error describe_identity_pool {}', TBProject.get_cognito_user_pool_id(), e)
            raise e

    def get_cognito_username(self, search_attribute, search_key, debug=False):
        """
        Search the cognito user pool for the given attribute and key.

        :param search_attribute:
        :param search_key:
        :param debug:
        :return:
        """

        search_filter = '{} = "{}"'.format(search_attribute, search_key)
        self.log('entering get_cognito_username using filter {}', search_filter)
        cognito_username = None
        cognito_email = None

        # look up first matching cognito user pool username
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cognito-idp.html#CognitoIdentityProvider.Client.list_users
            response = self.cognito_client.list_users(UserPoolId=TBProject.get_cognito_user_pool_id(),
                                                      AttributesToGet=['email'],
                                                      Limit=10,
                                                      Filter=search_filter)

            cognito_username, cognito_email = self.get_cognito_email(response)
            if cognito_username is None:
                # send email alert to fix linkage between swivl and cognito
                self.log('Could not find cognito login for email {}', search_key,
                         ValueError('Could not find cognito login'))

        except ClientError as e:
            self.log('Error searching filter {} in cognito user pool {}', search_filter,
                     TBProject.get_cognito_user_pool_id(), e)

        if cognito_username is None:
            try:
                # TODO sign them up if searchAttribute is `email`
                pass
            except ClientError as e:
                self.log('Error signing up swivl email {} to cognito.',
                         search_key, e)

        if cognito_username is None:
            try:
                response = self.cognito_client.list_users_in_group(UserPoolId=TBProject.get_cognito_user_pool_id(),
                                                                   GroupName=TBProject.get_cognito_default_group_name(),
                                                                   Limit=10)
                if debug:
                    self.log('list_users_in_group response {}', response)

                cognito_username, cognito_email = self.get_cognito_email(response, debug)
                self.log('{} not found, defaulting to cognito username {} email {}',
                         search_key, cognito_username, cognito_email)
            except ClientError as e:
                self.log('Error getting default cognito username from group {} ',
                         TBProject.get_cognito_default_group_name(), e)

        if cognito_username is None:
            # last resort :(
            cognito_username = 'fd080c56-b917-4cfd-97cc-e83ee6c8d823'
            cognito_email = 'plittle@evolutionhosting.com'
            self.log('using last resort cognito username {}', cognito_username)

        return cognito_username, cognito_email

    def get_cognito_email(self, list_users_response, debug=False):
        """

        :param list_users_response:
        :param debug:
        :return: cognito_username, cognito_email
        """
        cognito_username = None
        cognito_email = None

        if 'Users' not in list_users_response:
            self.log('no Users found in response {}', list_users_response)
            return cognito_username, cognito_email

        for cognito_user in list_users_response['Users']:
            if debug:
                self.log('Searching cognito user pool entry: {}', cognito_user)
            if cognito_user['Enabled'] \
                    and cognito_user['UserStatus'] in ['CONFIRMED', 'RESET_REQUIRED',
                                                       'FORCE_CHANGE_PASSWORD']:
                cognito_username = cognito_user['Username']
                for attribute in cognito_user['Attributes']:
                    if attribute['Name'] == 'email':
                        cognito_email = attribute['Value']
                        break
                break
        return cognito_username, cognito_email

    def start_ingest_pipeline(self, force=False, debug=False):
        """

        :param force: True = the last step of the pipeline may force another pipeline to start
        :param debug: True | False
        :return: the response of the aws api for list_executions or start_execution
        """
        state_machine_arn = self.get_ingest_pipeline()
        # The name of the execution must be unique, pretty much
        # https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html
        execution_name = self.stage + self.__class__.__name__ + str(time.time())
        self.log('about to start ingest step function {} with name {}', state_machine_arn, execution_name)

        # The string that contains the JSON input data for the execution
        step_function_input = {"body": {"message": "START", "pipeline_nickname": "ingest"}}

        try:
            response = self.sfn_client.list_executions(stateMachineArn=state_machine_arn,
                                                       statusFilter='RUNNING',
                                                       maxResults=100)

            if debug:
                self.log('list_executions returns {}', response)

            # Only one ingest pipeline running at a time, please.
            # The 'repeat' step of the ingest pipeline may start it over using 'force'
            if len(response['executions']) == 0 or force:
                response = self.sfn_client.start_execution(
                    stateMachineArn=state_machine_arn,
                    name=execution_name,
                    input=json.dumps(step_function_input)
                )
                self.log('started ingest pipeline {}', execution_name)
            else:
                self.log('ingest pipeline already running {}', state_machine_arn)

            return response
        except ClientError as e:
            self.log('Error regarding ingest step function {} because {}.', state_machine_arn, e)
            raise e

    def start_digest_pipeline(self, debug=False):
        """
        Starts the digest pipeline passing the s3 event trigger, if any. through as task input
        :param debug: True | False
        :return: the response of the aws api for start_execution
        """
        state_machine_arn = self.get_digest_pipeline()
        # The name of the execution must be unique, pretty much
        # https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html
        execution_name = self.stage + self.__class__.__name__ + str(time.time())

        if self.is_ingest_pipeline():
            # start digest in batch to pick up any stranded projects
            step_function_input = {"body": {"message": "START", "pipeline_nickname": "digest"}}
        else:
            # pass through s3 event
            step_function_input = self.event

        try:
            response = self.sfn_client.start_execution(
                stateMachineArn=state_machine_arn,
                name=execution_name,
                input=json.dumps(step_function_input)
            )
            if debug:
                self.log('start execution response is {}', response)

            self.log('started digest pipeline {} for event {}', execution_name, self.event)

            return response
        except ClientError as e:
            print(e)
            raise e
