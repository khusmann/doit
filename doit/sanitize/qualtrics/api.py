import os
from pathlib import Path

from . import impl

#    r = re.compile('^SV_.*')
#    m = r.match(surveyId)
#    if not m:
#       print ("survey Id must match ^SV_.*")

api_impl = impl.QualtricsApiImpl(
    api_key=os.environ['QUALTRICS_API_KEY'],
    data_center=os.environ['QUALTRICS_DATA_CENTER'],
)

def list_surveys():
   return impl.list_surveys(api_impl)

def download_survey(id: str, save_filename: str):
    return impl.download_survey(api_impl, id, save_filename)
