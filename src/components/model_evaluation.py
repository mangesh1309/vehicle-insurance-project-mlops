import pandas as pd
import os, sys
from dataclasses import dataclass
from typing import Optional
from src.exception import CustomException
from src.logger import logger

from src.constants import TARGET_COLUMN
from src.utils.main_utils import *
from sklearn.metrics import f1_score

from src.entity.artifact_entity import DataIngestionArtifact, ModelTrainerArtifact, ModelEvaluationArtifact
from src.entity.config_entity import ModelEvaluationConfig
from src.entity.s3_estimator import Proj1Estimator

@dataclass
class ModelEvaluationResponse:
    trained_model_f1_score: float
    best_model_f1_score: float
    is_model_accepted: bool
    difference: float

class ModelEvaluation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, model_evaluation_config: ModelEvaluationConfig, model_trainer_artifact: ModelTrainerArtifact):
        self.data_ingestion_artifact = data_ingestion_artifact
        self.model_evaluation_config = model_evaluation_config
        self.model_trainer_artifact = model_trainer_artifact

    def get_best_model(self) -> Optional[Proj1Estimator]:
        """
        Method Name :   get_best_model
        Description :   This function is used to get model from production stage.
        
        Output      :   Returns model object if available in s3 storage
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            bucket_name = self.model_evaluation_config.bucket_name
            model_path = self.model_evaluation_config.s3_model_key_path

            proj1_estimator = Proj1Estimator(bucket_name, model_path)

            if proj1_estimator.is_model_present(model_path):
                return proj1_estimator
            return None
        except Exception as e:
            raise CustomException(e, sys)

    def _map_gender_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map Gender column to 0 for Female and 1 for Male."""
        logger.info("Mapping 'Gender' column to binary values")
        df['Gender'] = df['Gender'].map({'Female': 0, 'Male': 1}).astype(int)
        return df
    
    def _create_dummy_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create dummy variables for categorical features."""
        logger.info("Creating dummy variables for categorical features")
        df = pd.get_dummies(df, drop_first=True)
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
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
    
    def _drop_id_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop the 'id' column if it exists."""
        logger.info("Dropping 'id' column")
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)
        return df
    
    def evaluate_model(self) -> ModelEvaluationResponse:
        """
        Method Name :   evaluate_model
        Description :   This function is used to evaluate trained model 
                        with production model and choose best model 
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
            X, y = test_df.drop(TARGET_COLUMN, axis = 1), test_df[TARGET_COLUMN]

            logger.info("Test data loaded and now transforming it for prediction...")

            X = self._map_gender_column(X)
            X = self._drop_id_column(X)
            X = self._create_dummy_columns(X)
            X = self._rename_columns(X)

            trained_model = load_object(self.model_trainer_artifact.trained_model_file_path)
            logger.info("Trained model loaded")

            trained_model_f1_score = self.model_trainer_artifact.metric_artifact.f1_score
            logger.info(f"F1 Score of the loaded trained model: {trained_model_f1_score}")

            best_model_f1_score=None
            best_model = self.get_best_model()
            if best_model is not None:
                logger.info(f"Computing F1_Score for production model..")
                y_hat_best_model = best_model.predict(X)
                best_model_f1_score = f1_score(y, y_hat_best_model)
                logger.info(f"F1_Score-Production Model: {best_model_f1_score}, F1_Score-New Trained Model: {trained_model_f1_score}")
            
            tmp_best_model_score = 0 if best_model_f1_score is None else best_model_f1_score
            result = ModelEvaluationResponse(trained_model_f1_score = trained_model_f1_score,
                                           best_model_f1_score = best_model_f1_score,
                                           is_model_accepted=trained_model_f1_score > tmp_best_model_score,
                                           difference = trained_model_f1_score - tmp_best_model_score
                                           )
            logger.info(f"Result: {result}")
            return result

        except Exception as e:
            raise CustomException(e, sys)

    def initiate_model_evaluation(self) -> ModelEvaluationArtifact:
        try:
            logger.info("Starting model evaluation phase.")
            evaluate_model_response = self.evaluate_model()
            s3_model_path = self.model_evaluation_config.s3_model_key_path

            model_evaluation_artifact = ModelEvaluationArtifact(
                                is_model_accepted = evaluate_model_response.is_model_accepted,
                                s3_model_path = s3_model_path,
                                trained_model_path = self.model_trainer_artifact.trained_model_file_path,
                                changed_accuracy = evaluate_model_response.difference)

            logger.info(f"Model evaluation artifact: {model_evaluation_artifact}")
            return model_evaluation_artifact
        except Exception as e:
            raise CustomException(e, sys)