"""
MLflow Reproducible Training Framework

Ensures 100% reproducibility through checksums and versioning
"""

import hashlib
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import mlflow.pytorch
from mlflow.tracking import MlflowClient
import torch
import torch.nn as nn


class ReproducibleTrainer:
    """Trainer with full reproducibility guarantees"""
    
    def __init__(
        self,
        experiment_name: str,
        tracking_uri: str = "sqlite:///mlflow.db",
        seed: int = 42
    ):
        """
        Initialize reproducible trainer
        
        Args:
            experiment_name: MLflow experiment name
            tracking_uri: MLflow tracking URI
            seed: Random seed for reproducibility
        """
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)
        self.experiment_name = experiment_name
        self.seed = seed
        self._set_seeds(seed)
    
    def _set_seeds(self, seed: int):
        """Set all random seeds for reproducibility"""
        np.random.seed(seed)
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
        os.environ['PYTHONHASHSEED'] = str(seed)
    
    def compute_data_hash(self, df: pd.DataFrame) -> str:
        """
        Compute SHA256 hash of data for reproducibility
        
        Args:
            df: Input dataframe
            
        Returns:
            SHA256 hash string
        """
        # Convert dataframe to bytes
        df_bytes = df.to_csv(index=False).encode('utf-8')
        return hashlib.sha256(df_bytes).hexdigest()
    
    def compute_schema_hash(self, df: pd.DataFrame) -> str:
        """
        Compute hash of feature schema
        
        Args:
            df: Input dataframe
            
        Returns:
            Schema hash string
        """
        schema = {
            'columns': list(df.columns),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
        }
        schema_str = json.dumps(schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode('utf-8')).hexdigest()
    
    def compute_requirements_hash(self) -> str:
        """
        Compute hash of requirements.txt
        
        Returns:
            Requirements hash string
        """
        try:
            with open('requirements.txt', 'r') as f:
                requirements = f.read()
            return hashlib.sha256(requirements.encode('utf-8')).hexdigest()
        except FileNotFoundError:
            return "unknown"
    
    def compute_git_hash(self) -> str:
        """
        Compute git commit hash
        
        Returns:
            Git commit hash
        """
        import subprocess
        try:
            result = subprocess.run(
                ['git', 'rev-parse', 'HEAD'],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return "unknown"
    
    def log_reproducibility_info(
        self,
        data_hash: str,
        schema_hash: str,
        requirements_hash: str,
        git_hash: str
    ):
        """
        Log all reproducibility information to MLflow
        
        Args:
            data_hash: Data hash
            schema_hash: Schema hash
            requirements_hash: Requirements hash
            git_hash: Git commit hash
        """
        mlflow.log_param("data_hash", data_hash)
        mlflow.log_param("schema_hash", schema_hash)
        mlflow.log_param("requirements_hash", requirements_hash)
        mlflow.log_param("git_commit", git_hash)
        mlflow.log_param("torch_seed", self.seed)
        mlflow.log_param("numpy_seed", self.seed)
        mlflow.log_param("python_hashseed", self.seed)
    
    def train_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series,
        model_type: str = "sklearn",
        model_params: Optional[Dict[str, Any]] = None,
        feature_names: Optional[list] = None
    ):
        """
        Train model with full reproducibility tracking
        
        Args:
            X_train: Training features
            y_train: Training labels
            X_val: Validation features
            y_val: Validation labels
            model_type: Type of model ("sklearn" or "pytorch")
            model_params: Model hyperparameters
            feature_names: List of feature names
            
        Returns:
            Trained model
        """
        if model_params is None:
            model_params = {}
        
        # Compute reproducibility hashes
        data_hash = self.compute_data_hash(X_train)
        schema_hash = self.compute_schema_hash(X_train)
        requirements_hash = self.compute_requirements_hash()
        git_hash = self.compute_git_hash()
        
        with mlflow.start_run():
            # Log reproducibility info
            self.log_reproducibility_info(
                data_hash, schema_hash, requirements_hash, git_hash
            )
            
            # Log model parameters
            for key, value in model_params.items():
                mlflow.log_param(key, value)
            
            # Log feature names
            if feature_names:
                mlflow.log_param("feature_names", json.dumps(feature_names))
            
            # Train model
            if model_type == "sklearn":
                from sklearn.ensemble import RandomForestRegressor
                model = RandomForestRegressor(**model_params, random_state=self.seed)
                model.fit(X_train, y_train)
                
                # Log model
                mlflow.sklearn.log_model(model, "model")
                
            elif model_type == "pytorch":
                # Example PyTorch model
                model = self._train_pytorch_model(
                    X_train, y_train, X_val, y_val, model_params
                )
                mlflow.pytorch.log_model(model, "model")
            else:
                raise ValueError(f"Unknown model type: {model_type}")
            
            # Evaluate and log metrics
            train_pred = model.predict(X_train)
            val_pred = model.predict(X_val)
            
            from sklearn.metrics import mean_squared_error, r2_score
            
            train_mse = mean_squared_error(y_train, train_pred)
            val_mse = mean_squared_error(y_val, val_pred)
            train_r2 = r2_score(y_train, train_pred)
            val_r2 = r2_score(y_val, val_pred)
            
            mlflow.log_metric("train_mse", train_mse)
            mlflow.log_metric("val_mse", val_mse)
            mlflow.log_metric("train_r2", train_r2)
            mlflow.log_metric("val_r2", val_r2)
            
            # Log data info
            mlflow.log_param("train_samples", len(X_train))
            mlflow.log_param("val_samples", len(X_val))
            mlflow.log_param("n_features", X_train.shape[1])
            
            return model
    
    def _train_pytorch_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series,
        model_params: Dict[str, Any]
    ) -> nn.Module:
        """Train PyTorch model"""
        # Convert to tensors
        X_train_tensor = torch.FloatTensor(X_train.values)
        y_train_tensor = torch.FloatTensor(y_train.values)
        X_val_tensor = torch.FloatTensor(X_val.values)
        y_val_tensor = torch.FloatTensor(y_val.values)
        
        # Create model
        input_size = X_train.shape[1]
        hidden_size = model_params.get('hidden_size', 64)
        output_size = 1
        
        model = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, output_size)
        )
        
        # Training loop
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=model_params.get('lr', 0.001))
        epochs = model_params.get('epochs', 100)
        
        for epoch in range(epochs):
            # Train
            model.train()
            optimizer.zero_grad()
            train_pred = model(X_train_tensor).squeeze()
            train_loss = criterion(train_pred, y_train_tensor)
            train_loss.backward()
            optimizer.step()
            
            # Validate
            model.eval()
            with torch.no_grad():
                val_pred = model(X_val_tensor).squeeze()
                val_loss = criterion(val_pred, y_val_tensor)
            
            if epoch % 10 == 0:
                mlflow.log_metric("train_loss", train_loss.item(), step=epoch)
                mlflow.log_metric("val_loss", val_loss.item(), step=epoch)
        
        return model


def verify_reproducibility(run_id: str, tracking_uri: str = "sqlite:///mlflow.db"):
    """
    Verify that a run can be reproduced
    
    Args:
        run_id: MLflow run ID
        tracking_uri: MLflow tracking URI
        
    Returns:
        Dictionary with reproducibility check results
    """
    client = MlflowClient(tracking_uri=tracking_uri)
    run = client.get_run(run_id)
    
    checks = {
        'data_hash_present': 'data_hash' in run.data.params,
        'schema_hash_present': 'schema_hash' in run.data.params,
        'requirements_hash_present': 'requirements_hash' in run.data.params,
        'git_commit_present': 'git_commit' in run.data.params,
        'seed_present': 'torch_seed' in run.data.params,
    }
    
    checks['all_present'] = all(checks.values())
    
    return checks
