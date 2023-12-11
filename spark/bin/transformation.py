import os
from dotenv import load_dotenv
import logging

# Load the Logging Configuration File
logging.config.fileConfig(fname='./spark/utils/logging_to_file.conf')
logger = logging.getLogger(__name__)
