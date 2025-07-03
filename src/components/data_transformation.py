import pandas as pd, numpy as np
import sys

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.compose import ColumnTransformer
from imblearn.combine import SMOTEENN

from src.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact, DataTransformationArtifact
from src.entity.config_entity import DataTransformationConfig

from src.constants import *

from src.logger import logger
from src.exception import CustomException
from src.utils.main_utils import read_yaml_file, save_numpy_array_data, save_object

class DataTransformation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                       data_transformation_config: DataTransformationConfig,
                       data_validation_artifact: DataValidationArtifact):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_transformation_config = data_transformation_config
            self.data_validation_artifact = data_validation_artifact
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise CustomException(e, sys)
    
    @staticmethod
    def read_data(path) -> pd.DataFrame:
        try:
            return pd.read_csv(path)
        except Exception as e:
            raise CustomException(e, sys)
        
    def get_data_transformer_object(self) -> Pipeline:
        """
        Creates and returns a data transformer object for the data, 
        including gender mapping, dummy variable creation, column renaming,
        feature scaling, and type adjustments.
        """
        logger.info("Entered get_data_transformer_object method of DataTransformation class")

        try:
            standard_scalar = StandardScaler()
            minmax_scalar = MinMaxScaler()

            num_features =  self.schema_config["num_features"]
            mm_columns = self.schema_config["mm_columns"]
            logger.info("Columns loaded from config yaml")

            preprocessor = ColumnTransformer(
                transformers = [
                    ("StandardScaler", standard_scalar, num_features),
                    ("MinMaxScaler", minmax_scalar, mm_columns)
                ],
                remainder = "passthrough"
            )

            final_pipeline = Pipeline(steps=[("Preprocessor", preprocessor)])
            logger.info("Final Pipeline ready")
            logger.info("Exited get_data_transformer_object method of DataTransformation class")

            return final_pipeline
        except Exception as e:
            raise CustomException(e, sys)
    
    def _map_gender_column(self, df):
        """Map Gender column to 0 for Female and 1 for Male."""
        logger.info("Mapping 'Gender' column to binary values")
        df['Gender'] = df['Gender'].map({'Female': 0, 'Male': 1}).astype(int)
        return df
    
    def _rename_columns(self, df):
        """Rename specific columns and ensure integer types for dummy columns."""
        logger.info("Renaming specific columns and casting to int")
        df = df.rename(columns={
            "Vehicle_Age_< 1 Year": "Vehicle_Age_lt_1_Year",
            "Vehicle_Age_> 2 Years": "Vehicle_Age_gt_2_Years"
        })
        for col in ["Vehicle_Age_lt_1_Year", "Vehicle_Age_gt_2_Years", "Vehicle_Damage_Yes"]:
            if col in df.columns:
                df[col] = df[col].astype('int')
        return df
    
    def _drop_id_column(self, df):
        """Drop the 'id' column if it exists."""
        logger.info("Dropping 'id' column")
        drop_col = self.schema_config['drop_columns']
        if drop_col in df.columns:
            df = df.drop(drop_col, axis=1)
        return df
    
    def _create_dummy_columns(self, df):
        """Create dummy variables for categorical features."""
        logger.info("Creating dummy variables for categorical features")
        df = pd.get_dummies(df, drop_first=True)
        return df
    
    def initiate_data_transformation(self) -> DataTransformationArtifact:
        """
        Initiates the data transformation component for the pipeline.
        """
        try:
            logger.info("Data Transformation Started !!!")
            if not self.data_validation_artifact.validation_status:
                raise Exception(self.data_validation_artifact.message)

            # Load train and test data
            train_df = self.read_data(path=self.data_ingestion_artifact.trained_file_path)
            test_df = self.read_data(path=self.data_ingestion_artifact.test_file_path)
            logger.info("Train-Test data loaded")

            input_feature_train_df = train_df.drop(columns=[TARGET_COLUMN], axis=1)
            target_feature_train_df = train_df[TARGET_COLUMN]

            input_feature_test_df = test_df.drop(columns=[TARGET_COLUMN], axis=1)
            target_feature_test_df = test_df[TARGET_COLUMN]
            logger.info("Input and Target cols defined for both train and test df.")

            # Apply custom transformations in specified sequence
            input_feature_train_df = self._map_gender_column(input_feature_train_df)
            input_feature_train_df = self._drop_id_column(input_feature_train_df)
            input_feature_train_df = self._create_dummy_columns(input_feature_train_df)
            input_feature_train_df = self._rename_columns(input_feature_train_df)

            input_feature_test_df = self._map_gender_column(input_feature_test_df)
            input_feature_test_df = self._drop_id_column(input_feature_test_df)
            input_feature_test_df = self._create_dummy_columns(input_feature_test_df)
            input_feature_test_df = self._rename_columns(input_feature_test_df)
            logger.info("Custom transformations applied to train and test data")

            logger.info("Starting data transformation")
            preprocessor = self.get_data_transformer_object()
            logger.info("Got the preprocessor object")

            logger.info("Initializing transformation for Training-data")
            input_feature_train_arr = preprocessor.fit_transform(input_feature_train_df)
            logger.info("Initializing transformation for Testing-data")
            input_feature_test_arr = preprocessor.transform(input_feature_test_df)
            logger.info("Transformation done end to end to train-test df.")

            logger.info("Applying SMOTEENN for handling imbalanced dataset.")
            smt = SMOTEENN(sampling_strategy="minority")
            input_feature_train_final, target_feature_train_final = smt.fit_resample(input_feature_train_arr, target_feature_train_df)
            input_feature_test_final, target_feature_test_final = smt.fit_resample(input_feature_test_arr, target_feature_test_df)
            logger.info("SMOTEENN applied to train-test df.")

            train_arr = np.c_[input_feature_train_final, np.array(target_feature_train_final)]
            test_arr = np.c_[input_feature_test_final, np.array(target_feature_test_final)]
            logger.info("feature-target concatenation done for train-test df.")

            save_object(self.data_transformation_config.transformed_object_file_path, preprocessor)
            save_numpy_array_data(self.data_transformation_config.transformed_train_file_path, array=train_arr)
            save_numpy_array_data(self.data_transformation_config.transformed_test_file_path, array=test_arr)
            logger.info("Saving transformation object and transformed files.")

            data_transformation_artifact = DataTransformationConfig(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path
            )

            logger.info("Data transformation completed successfully")
            return data_transformation_artifact

        except Exception as e:
            raise CustomException(e, sys) from e