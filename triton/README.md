# Triton Model Repository

This directory contains Triton Inference Server model configurations.

## Structure

```
triton/
├── regime_prediction_model/
│   ├── config.pbtxt          # Model configuration
│   └── 1/
│       └── model.pt           # PyTorch model file
└── README.md
```

## Model Configuration

See `regime_prediction_model/config.pbtxt` for the model configuration.

## Deployment

Models are deployed via Kubernetes PersistentVolumeClaim and mounted to Triton server.

### Upload Model

```bash
# Copy model to PVC
kubectl cp regime_prediction_model/ triton-inference-server-0:/models/ -n financial-timeseries
```

### Verify Model

```bash
# Check model status
curl http://triton-inference-server:8000/v2/models/regime_prediction_model
```

## Canary Deployment

Models are deployed with canary strategy:
- Baseline model: `regime_baseline`
- Canary model: `regime_canary_v2`

See `src/serving/triton_canary.py` for deployment logic.

## Model Serving

```python
import tritonclient.http as httpclient

client = httpclient.InferenceServerClient(url="http://triton:8000")

inputs = [httpclient.InferInput("features", [1, 11], "FP32")]
inputs[0].set_data_from_numpy(features_array.astype(np.float32))

result = client.infer("regime_prediction_model", inputs)
prediction = result.as_numpy("regime_prediction")
```

## References

- [Triton Inference Server Documentation](https://github.com/triton-inference-server/server)
- [Triton Model Configuration](https://github.com/triton-inference-server/server/blob/main/docs/model_configuration.md)
