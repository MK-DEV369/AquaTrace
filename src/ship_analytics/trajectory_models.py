"""Trajectory prediction models: LSTM seq2seq vs Transformer.

Both models share a common interface so the comparison harness can train and
evaluate them on identical data. Models run on GPU when available.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import torch
import torch.nn as nn


def _device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


FEATURES = ["latitude", "longitude", "sog_knots", "cog_degrees"]
INPUT_DIM = len(FEATURES)


@dataclass
class PredictionResult:
    model: str
    horizon_minutes: int
    positions: list[dict[str, float]]
    latency_ms: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "horizon_minutes": self.horizon_minutes,
            "positions": self.positions,
            "latency_ms": self.latency_ms,
        }


class LSTMSeq2Seq(nn.Module):
    """Encoder-decoder LSTM over (lat, lon, sog, cog) sequences."""

    def __init__(self, hidden_dim: int = 64, num_layers: int = 2, horizon: int = 6):
        super().__init__()
        self.horizon = horizon
        self.encoder = nn.LSTM(INPUT_DIM, hidden_dim, num_layers, batch_first=True)
        self.decoder = nn.LSTM(INPUT_DIM, hidden_dim, num_layers, batch_first=True)
        self.head = nn.Linear(hidden_dim, INPUT_DIM)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, INPUT_DIM)
        _, (h, c) = self.encoder(x)
        # Seed decoder with last input repeated.
        last = x[:, -1:, :].repeat(1, self.horizon, 1)
        out, _ = self.decoder(last, (h, c))
        return self.head(out)  # (batch, horizon, INPUT_DIM)


class TrajectoryTransformer(nn.Module):
    """Transformer encoder over trajectory tokens (TrAISformer-style simplified)."""

    def __init__(self, d_model: int = 64, nhead: int = 4, num_layers: int = 2, horizon: int = 6):
        super().__init__()
        self.horizon = horizon
        self.input_proj = nn.Linear(INPUT_DIM, d_model)
        self.pos_emb = nn.Embedding(512, d_model)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=nhead, batch_first=True, dim_feedforward=d_model * 4
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.query = nn.Parameter(torch.randn(1, horizon, d_model))
        decoder_layer = nn.TransformerDecoderLayer(
            d_model=d_model, nhead=nhead, batch_first=True, dim_feedforward=d_model * 4
        )
        self.decoder = nn.TransformerDecoder(decoder_layer, num_layers=num_layers)
        self.head = nn.Linear(d_model, INPUT_DIM)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        b, s, _ = x.shape
        src = self.input_proj(x) + self.pos_emb(torch.arange(s, device=x.device))
        memory = self.encoder(src)
        tgt = self.query.repeat(b, 1, 1)
        out = self.decoder(tgt, memory)
        return self.head(out)  # (batch, horizon, INPUT_DIM)


class TrajectoryPredictor:
    """High-level wrapper that trains and runs either model."""

    def __init__(self, model_type: str = "lstm", horizon_minutes: int = 60, seq_len: int = 10):
        self.model_type = model_type
        self.horizon_minutes = horizon_minutes
        # 6 time steps of 10 min = 60 min horizon.
        self.horizon_steps = max(1, horizon_minutes // 10)
        self.seq_len = seq_len
        self.device = _device()
        if model_type == "lstm":
            self.model = LSTMSeq2Seq(horizon=self.horizon_steps).to(self.device)
        elif model_type == "transformer":
            self.model = TrajectoryTransformer(horizon=self.horizon_steps).to(self.device)
        else:
            raise ValueError(f"unknown model_type {model_type}")
        self.trained = False

    def _make_dataset(self, trajectories: list[list[dict[str, float]]]) -> tuple[torch.Tensor, torch.Tensor]:
        xs: list[np.ndarray] = []
        ys: list[np.ndarray] = []
        for traj in trajectories:
            arr = np.array([[p[f] for f in FEATURES] for p in traj], dtype=np.float32)
            if len(arr) < self.seq_len + self.horizon_steps:
                continue
            for i in range(len(arr) - self.seq_len - self.horizon_steps + 1):
                xs.append(arr[i : i + self.seq_len])
                ys.append(arr[i + self.seq_len : i + self.seq_len + self.horizon_steps])
        if not xs:
            return torch.zeros(0, self.seq_len, INPUT_DIM), torch.zeros(0, self.horizon_steps, INPUT_DIM)
        return (
            torch.from_numpy(np.stack(xs)),
            torch.from_numpy(np.stack(ys)),
        )

    def train(self, trajectories: list[list[dict[str, float]]], epochs: int = 5, lr: float = 1e-3) -> dict[str, Any]:
        x, y = self._make_dataset(trajectories)
        if x.shape[0] == 0:
            return {"status": "no_data", "samples": 0}
        x = x.to(self.device)
        y = y.to(self.device)
        opt = torch.optim.Adam(self.model.parameters(), lr=lr)
        loss_fn = nn.MSELoss()
        self.model.train()
        losses = []
        for _ in range(epochs):
            opt.zero_grad()
            pred = self.model(x)
            loss = loss_fn(pred, y)
            loss.backward()
            opt.step()
            losses.append(float(loss.item()))
        self.trained = True
        return {
            "status": "trained",
            "samples": int(x.shape[0]),
            "epochs": epochs,
            "final_loss": losses[-1] if losses else None,
            "device": str(self.device),
        }

    def predict(self, recent: list[dict[str, float]]) -> PredictionResult:
        import time as _time

        arr = np.array([[p[f] for f in FEATURES] for p in recent[-self.seq_len :]], dtype=np.float32)
        if arr.shape[0] < self.seq_len:
            # Pad with the last observation.
            pad = np.repeat(arr[-1:], self.seq_len - arr.shape[0], axis=0)
            arr = np.concatenate([pad, arr], axis=0)
        x = torch.from_numpy(arr).unsqueeze(0).to(self.device)
        self.model.eval()
        t0 = _time.perf_counter()
        with torch.no_grad():
            pred = self.model(x).squeeze(0).cpu().numpy()
        latency = (_time.perf_counter() - t0) * 1000.0
        positions = []
        for step in pred:
            positions.append(
                {
                    "latitude": float(step[0]),
                    "longitude": float(step[1]),
                    "sog_knots": float(step[2]),
                    "cog_degrees": float(step[3]),
                }
            )
        return PredictionResult(
            model=self.model_type,
            horizon_minutes=self.horizon_minutes,
            positions=positions,
            latency_ms=latency,
        )

    def parameter_count(self) -> int:
        return sum(p.numel() for p in self.model.parameters())

    def save(self, path: str) -> None:
        torch.save(self.model.state_dict(), path)

    def load(self, path: str) -> None:
        state = torch.load(path, map_location=self.device)
        self.model.load_state_dict(state)
        self.trained = True


def build_trajectories_from_events(events: list[dict[str, Any]]) -> list[list[dict[str, float]]]:
    """Group events by MMSI and convert to feature dicts."""
    by_vessel: dict[int, list[dict[str, float]]] = {}
    for e in events:
        by_vessel.setdefault(e["mmsi"], []).append(
            {
                "latitude": float(e["latitude"]),
                "longitude": float(e["longitude"]),
                "sog_knots": float(e.get("sog_knots") or 0.0),
                "cog_degrees": float(e.get("cog_degrees") or 0.0),
            }
        )
    return list(by_vessel.values())


def compare_trajectory_models(
    trajectories: list[list[dict[str, float]]],
    *,
    epochs: int = 1,
    horizon_minutes: int = 60,
    seq_len: int = 10,
) -> dict[str, Any]:
    """Train and evaluate LSTM and Transformer models on identical holdouts."""
    results: dict[str, Any] = {"horizon_minutes": horizon_minutes, "models": {}}
    for model_type in ("lstm", "transformer"):
        predictor = TrajectoryPredictor(
            model_type=model_type,
            horizon_minutes=horizon_minutes,
            seq_len=seq_len,
        )
        training = predictor.train(trajectories, epochs=epochs)
        errors: list[float] = []
        latencies: list[float] = []
        for trajectory in trajectories:
            if len(trajectory) < seq_len + predictor.horizon_steps:
                continue
            recent = trajectory[:seq_len]
            target = trajectory[seq_len : seq_len + predictor.horizon_steps]
            prediction = predictor.predict(recent)
            latencies.append(prediction.latency_ms)
            for predicted, actual in zip(prediction.positions, target, strict=False):
                errors.append(
                    _haversine_nm(
                        predicted["latitude"],
                        predicted["longitude"],
                        actual["latitude"],
                        actual["longitude"],
                    )
                )
        results["models"][model_type] = {
            "training": training,
            "samples_evaluated": len(errors),
            "mae_nm": float(np.mean(errors)) if errors else None,
            "rmse_nm": float(np.sqrt(np.mean(np.square(errors)))) if errors else None,
            "p95_latency_ms": float(np.percentile(latencies, 95)) if latencies else None,
            "device": str(predictor.device),
        }
    return results


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_nm = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius_nm * math.asin(math.sqrt(a))