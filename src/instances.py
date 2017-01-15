import logging
from falcon_cors import CORS
from lib.api import CustomAPI, ApiRequest
from lib.config import Config

logging.basicConfig(level='DEBUG',
                    format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
logger = logging.getLogger(__name__)

config = Config()

api = CustomAPI(request_type=ApiRequest, middleware=[
    CORS(
        allow_all_origins=True,
        allow_all_headers=True,
        allow_all_methods=True,
        allow_credentials_all_origins=True
    ).middleware
])
