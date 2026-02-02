"""
Triton Inference Server with Canary + Shadow Deployment

Model serving with canary deployment and shadow testing
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import tritonclient.http as httpclient
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import random
from pydantic import BaseModel
import asyncio
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PredictionRequest(BaseModel):
    """Prediction request model"""
    symbol: str
    features: Dict[str, float]
    timestamp: Optional[datetime] = None


class PredictionResponse(BaseModel):
    """Prediction response model"""
    symbol: str
    prediction: float
    model_version: str
    latency_ms: float
    timestamp: datetime


class ShadowMetrics:
    """Track shadow deployment metrics"""
    
    def __init__(self):
        self.baseline_predictions = []
        self.canary_predictions = []
        self.baseline_latencies = []
        self.canary_latencies = []
        self.start_time = datetime.now()
    
    def record_baseline(self, prediction: float, latency_ms: float):
        """Record baseline model prediction"""
        self.baseline_predictions.append(prediction)
        self.baseline_latencies.append(latency_ms)
    
    def record_canary(self, prediction: float, latency_ms: float):
        """Record canary model prediction"""
        self.canary_predictions.append(prediction)
        self.canary_latencies.append(latency_ms)
    
    def calculate_pnl_diff(self, prices: List[float]) -> float:
        """
        Calculate PnL difference between baseline and canary
        
        Simulates trading based on predictions
        """
        if len(self.baseline_predictions) != len(self.canary_predictions):
            return 0.0
        
        baseline_pnl = self._simulate_trades(self.baseline_predictions, prices)
        canary_pnl = self._simulate_trades(self.canary_predictions, prices)
        
        if abs(baseline_pnl) < 0.01:
            return 0.0
        
        pnl_diff_pct = 100 * (canary_pnl - baseline_pnl) / abs(baseline_pnl)
        return pnl_diff_pct
    
    def _simulate_trades(self, predictions: List[float], prices: List[float]) -> float:
        """Simulate trading based on predictions"""
        pnl = 0.0
        position = 0
        
        for i in range(len(predictions) - 1):
            if predictions[i] > 0:  # Buy signal
                position += 1
                pnl -= prices[i]
            elif predictions[i] < 0:  # Sell signal
                if position > 0:
                    pnl += prices[i]
                    position -= 1
        
        # Close position
        if position > 0 and len(prices) > 0:
            pnl += prices[-1] * position
        
        return pnl
    
    def get_metrics(self) -> Dict:
        """Get shadow metrics"""
        baseline_latency_p95 = np.percentile(self.baseline_latencies, 95) if self.baseline_latencies else 0
        canary_latency_p95 = np.percentile(self.canary_latencies, 95) if self.canary_latencies else 0
        
        return {
            "baseline_count": len(self.baseline_predictions),
            "canary_count": len(self.canary_predictions),
            "baseline_latency_p95_ms": baseline_latency_p95,
            "canary_latency_p95_ms": canary_latency_p95,
            "latency_increase_pct": 100 * (canary_latency_p95 - baseline_latency_p95) / baseline_latency_p95 if baseline_latency_p95 > 0 else 0
        }


class TritonModelServer:
    """Triton Inference Server client wrapper"""
    
    def __init__(self, url: str = "localhost:8000"):
        self.url = url
        self.client = None
        self._connect()
    
    def _connect(self):
        """Connect to Triton server"""
        try:
            self.client = httpclient.InferenceServerClient(url=self.url, verbose=False)
            logger.info(f"Connected to Triton server at {self.url}")
        except Exception as e:
            logger.error(f"Failed to connect to Triton: {e}")
            raise
    
    def predict(self, model_name: str, inputs: Dict[str, np.ndarray]) -> np.ndarray:
        """
        Make prediction using Triton
        
        Args:
            model_name: Name of the model
            inputs: Dictionary of input arrays
            
        Returns:
            Prediction array
        """
        # Prepare inputs
        triton_inputs = []
        for name, data in inputs.items():
            triton_inputs.append(
                httpclient.InferInput(name, data.shape, "FP32")
            )
            triton_inputs[-1].set_data_from_numpy(data)
        
        # Make inference
        result = self.client.infer(model_name, triton_inputs)
        
        # Get output (assuming single output)
        output_name = result.get_response()['outputs'][0]['name']
        output = result.as_numpy(output_name)
        
        return output


class CanaryDeployment:
    """Canary deployment manager"""
    
    def __init__(
        self,
        baseline_model: str,
        canary_model: str,
        canary_traffic_percent: float = 10.0
    ):
        self.baseline_model = baseline_model
        self.canary_model = canary_model
        self.canary_traffic_percent = canary_traffic_percent
        self.shadow_metrics = ShadowMetrics()
        self.deployment_phase = "shadow"  # shadow -> canary -> ramp -> prod
        self.phase_start_time = datetime.now()
    
    def should_use_canary(self) -> bool:
        """Determine if request should go to canary"""
        if self.deployment_phase == "shadow":
            return False  # Shadow: 100% baseline, 10% copied to canary
        elif self.deployment_phase == "canary":
            return random.random() < (self.canary_traffic_percent / 100.0)
        elif self.deployment_phase == "ramp":
            return random.random() < 0.5  # 50% traffic
        else:  # prod
            return True  # 100% canary
    
    def route_request(self, request: PredictionRequest) -> tuple:
        """
        Route request to appropriate model
        
        Returns:
            (model_name, is_shadow)
        """
        use_canary = self.should_use_canary()
        
        if self.deployment_phase == "shadow":
            # Shadow: always baseline, but also send to canary for comparison
            return (self.baseline_model, True)
        else:
            return (self.canary_model if use_canary else self.baseline_model, False)
    
    def check_deployment_health(self) -> Dict:
        """
        Check deployment health and determine if can proceed to next phase
        
        Returns:
            Health status and recommendation
        """
        metrics = self.shadow_metrics.get_metrics()
        
        # Check latency increase
        latency_increase = metrics.get("latency_increase_pct", 0)
        if latency_increase > 20:
            return {
                "status": "unhealthy",
                "reason": f"Latency increased by {latency_increase}%",
                "recommendation": "rollback"
            }
        
        # Check PnL difference (would need prices)
        # pnl_diff = self.shadow_metrics.calculate_pnl_diff(prices)
        # if abs(pnl_diff) > 10:
        #     return {
        #         "status": "unhealthy",
        #         "reason": f"PnL difference: {pnl_diff}%",
        #         "recommendation": "rollback"
        #     }
        
        # Check phase duration
        phase_duration = (datetime.now() - self.phase_start_time).total_seconds() / 3600
        
        if self.deployment_phase == "shadow" and phase_duration >= 24:
            return {
                "status": "healthy",
                "recommendation": "proceed_to_canary"
            }
        elif self.deployment_phase == "canary" and phase_duration >= 48:
            return {
                "status": "healthy",
                "recommendation": "proceed_to_ramp"
            }
        elif self.deployment_phase == "ramp" and phase_duration >= 24:
            return {
                "status": "healthy",
                "recommendation": "proceed_to_prod"
            }
        
        return {
            "status": "healthy",
            "recommendation": "continue"
        }


# FastAPI app
app = FastAPI(title="Financial Timeseries Model Serving")

# Initialize Triton client
triton_client = TritonModelServer()

# Initialize canary deployment
canary = CanaryDeployment(
    baseline_model="regime_baseline",
    canary_model="regime_canary_v2",
    canary_traffic_percent=10.0
)


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """
    Make prediction with canary deployment
    
    Routes request to baseline or canary based on deployment phase
    """
    start_time = datetime.now()
    
    # Route request
    model_name, is_shadow = canary.route_request(request)
    
    # Prepare inputs for Triton
    feature_array = np.array([[request.features.get(f"feature_{i}", 0.0) for i in range(11)]])
    inputs = {"input": feature_array.astype(np.float32)}
    
    # Make prediction
    try:
        prediction = triton_client.predict(model_name, inputs)
        prediction_value = float(prediction[0][0])
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    latency_ms = (datetime.now() - start_time).total_seconds() * 1000
    
    # Record shadow metrics
    if is_shadow:
        canary.shadow_metrics.record_baseline(prediction_value, latency_ms)
        # Also get canary prediction for comparison
        try:
            canary_prediction = triton_client.predict(canary.canary_model, inputs)
            canary.shadow_metrics.record_canary(float(canary_prediction[0][0]), latency_ms)
        except:
            pass
    else:
        if model_name == canary.baseline_model:
            canary.shadow_metrics.record_baseline(prediction_value, latency_ms)
        else:
            canary.shadow_metrics.record_canary(prediction_value, latency_ms)
    
    return PredictionResponse(
        symbol=request.symbol,
        prediction=prediction_value,
        model_version=model_name,
        latency_ms=latency_ms,
        timestamp=datetime.now()
    )


@app.get("/deployment/health")
async def get_deployment_health():
    """Get deployment health status"""
    health = canary.check_deployment_health()
    metrics = canary.shadow_metrics.get_metrics()
    
    return {
        "phase": canary.deployment_phase,
        "health": health,
        "metrics": metrics
    }


@app.post("/deployment/rollback")
async def rollback_deployment():
    """Rollback to baseline model"""
    canary.deployment_phase = "shadow"
    canary.phase_start_time = datetime.now()
    canary.shadow_metrics = ShadowMetrics()
    
    return {"status": "rolled_back", "phase": "shadow"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
