import sys
import logging

class CustomException(Exception):
    def __init__(self, error_message: str, error_detail: sys):
        super().__init__(error_message)
        self.error_message = self.get_detailed_error_message(error_message, error_detail)

        # Log the error
        logging.error(self.error_message)

    def get_detailed_error_message(self, error_message, error_detail: sys):
        """
        Extracts file name and line number where exception occurred, and formats the error message.
        """
        _, _, exc_tb = error_detail.exc_info()
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_number = exc_tb.tb_lineno
        return f"File: [{file_name}]: [{line_number}]: {error_message}"

    def __str__(self):
        return self.error_message
