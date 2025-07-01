# ------------------------------- logger demo ------------------------------- #

# from src.logger import logger

# logger.debug("This is debug log")
# logger.info("This is information log")
# logger.warning("This is warning log")
# logger.error("This is error log")
# logger.critical("This is critical log")

# ------------------------------- exception demo ------------------------------- #

from src.logger import logger
from src.exception import CustomException
import sys

try:
    15/0
except Exception as e:
    logger.info(e)
    raise CustomException(str(e), sys) from e
