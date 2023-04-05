#
# Copyright 2015-2021, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import logging
from logging import INFO, ERROR

#import settings

logging.basicConfig()

rootlogger = logging.getLogger('root')
rootlogger.setLevel(INFO)

progresslogger = logging.getLogger('root.progress')
progresslogger.setLevel(INFO)
for hdlr in progresslogger.handlers[:]:
    progresslogger.removeHandler(hdlr)
#success_fh = logging.FileHandler('./logs/'+mv_image_files.img_s3_bucket+'_progress.log')
#progresslogger.addHandler(success_fh)


errlogger = logging.getLogger('root.err')
errlogger.setLevel(ERROR)
for hdlr in errlogger.handlers[:]:
    errlogger.removeHandler(hdlr)
#err_fh = logging.FileHandler('{}/error.log'.format(settings.LOG_DIR))
#err_fh = logging.FileHandler('./logs/'+mv_image_files.img_s3_bucket+'_error.log')
errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
#errlogger.addHandler(err_fh)
#err_fh.setFormatter(errformatter)