"""
Ctrl-Shift-F10 or right-click and choose Run
to test locally without docker or aws api
"""
import os
os.environ["AWS_PROFILE"] = "talkback"
os.environ["STAGE"] = "stage"
from talkback import TBCache, TBProject

tbcache = TBCache()

tbproject = TBProject(tbcache)

# tbproject.get_next_video_id()

one = 'stage/uploads/VID_0045.mov - Mixed track.mp4'
two = 'stage/uploads/VID_0045.mov+-+Mixed+track.mp4'

if tbproject.exists_in_s3(one):
    print('one exists')
else:
    print('one does not exist')

if tbproject.exists_in_s3(two):
    print('two exists')
else:
    print('two does not exist')

external_s3_link = 'https://s3.amazonaws.com/talkback-transcribe/stage_talkback_3_Paul_test_1_2019-07-31_20-00-57-aws-transcript.json'

source_url = tbproject.convert_https_bucket_link_to_s3(external_s3_link)
print('source url is {} '.format(source_url))
source_url2 = tbproject.convert_https_bucket_link_to_s3(source_url)
print('source url2 is {} '.format(source_url2))

