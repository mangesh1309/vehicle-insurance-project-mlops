import json, os, sys

import pandas as pd

from src.exception import CustomException
from src.logger import logger
from src.utils.main_utils import read_yaml_file

from src.entity.config_entity import DataValidationConfig
from src.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from src.constants import SCHEMA_FILE_PATH

class DataValidation:
    def __init__(self, data_validation_config: DataValidationConfig, data_ingestion_artifact: DataIngestionArtifact):
        try:
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise CustomException(e, sys)

    def validate_number_of_cols(self, df: pd.DataFrame) -> bool:
        try:
            validation = len(df.columns) == len(self.schema_config["columns"])
            logger.info(f"Requried number of columns present?: {validation}")
            return validation
        except Exception as e:
            raise CustomException(e, sys)

    def does_column_exists(self, df: pd.DataFrame) -> bool:
        try:
            given_columns = df.columns
            required_numerical_columns = self.schema_config["numerical_columns"]
            missing_numerical_columns = []

            for column in required_numerical_columns:
                if column not in given_columns:
                    missing_numerical_columns.append(column)
            
            if len(missing_numerical_columns)>0:
                logger.info(f"Missing numerical columns: {missing_numerical_columns}")

            required_categorical_columns = self.schema_config["categorical_columns"]
            missing_categorical_columns = []

            for column in required_categorical_columns:
                if column not in given_columns:
                    missing_categorical_columns.append(column)
            
            if len(missing_categorical_columns)>0:
                logger.info(f"Missing categorical columns: {missing_categorical_columns}")
            
            return False if len(missing_categorical_columns)>0 or len(missing_numerical_columns)>0 else True
        except Exception as e:
            raise CustomException(e, sys)

    @staticmethod
    def read_data(path) -> pd.DataFrame:
        try:
            df = pd.read_csv(path)
            return df
        except Exception as e:
            raise CustomException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            validation_error_message = ""
            logger.info("Starting data validation")

            train_df, test_df = (DataValidation.read_data(self.data_ingestion_artifact.trained_file_path),
                                 DataValidation.read_data(self.data_ingestion_artifact.test_file_path))


            # check cols len of df for train/test
            status = self.validate_number_of_cols(train_df)
            if not status:
                validation_error_message += "Columns are missing in training dataframe. "
            else:
                logger.info(f"All required columns present in training dataframe: {status}")

            status = self.validate_number_of_cols(test_df)
            if not status:
                validation_error_message += "Columns are missing in testing dataframe. "
            else:
                logger.info(f"All required columns present in testing dataframe: {status}")

            
            # Validating col dtype for train/test df
            status = self.does_column_exists(df=train_df)
            if not status:
                validation_error_message += f"Columns are missing in training dataframe. "
            else:
                logger.info(f"All categorical/int columns present in training dataframe: {status}")

            status = self.does_column_exists(df=test_df)
            if not status:
                validation_error_message += f"Columns are missing in test dataframe."
            else:
                logger.info(f"All categorical/int columns present in testing dataframe: {status}")

            validation_status = len(validation_error_message)==0

            data_validation_artifact = DataValidationArtifact(
                validation_status = validation_status,
                message = validation_error_message,
                validation_report_file_path = self.data_validation_config.validation_report_file_path
            )

            # Ensure the directory for validation_report_file_path exists
            report_dir = os.path.dirname(self.data_validation_config.validation_report_file_path)
            os.makedirs(report_dir, exist_ok=True)


            validation_report = {
                "validation_status": validation_status,
                "message": validation_error_message.strip()
            }

            with open (self.data_validation_config.validation_report_file_path, "w") as file:
                json.dump(validation_report, file)

            logger.info("Data validation artifact saved.")
            logger.info(f"Data validation artifact: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise CustomException(e, sys)
        
        