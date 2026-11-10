"""Evaluation metrics for trajectory prediction."""

from __future__ import annotations

import math


def haversine_nm(lat1, lon1, lat2, lon2):
    """Great-circle distance in nautical miles."""
    r_nm = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * r_nm * math.asin(math.sqrt(a))


def mean_error_nm(preds, targets):
    """Mean position error in nautical miles."""
    errs = []
    for p, t in zip(preds, targets, strict=False):
        errs.append(haversine_nm(p[0], p[1], t[0], t[1]))
    return sum(errs) / len(errs) if errs else 0.0


def rmse_nm(preds, targets):
    errs = [haversine_nm(p[0], p[1], t[0], t[1]) for p, t in zip(preds, targets, strict=False)]
    return math.sqrt(sum(e * e for e in errs) / len(errs)) if errs else 0.0


def mae_knots(pred_speeds, target_speeds):
    diffs = [abs(p - t) for p, t in zip(pred_speeds, target_speeds, strict=False)]
    return sum(diffs) / len(diffs) if diffs else 0.0


def accuracy_at_threshold_nm(preds, targets, threshold_nm=1.0):
    hits = sum(1 for p, t in zip(preds, targets, strict=False) if haversine_nm(p[0], p[1], t[0], t[1]) <= threshold_nm)
    return hits / len(preds) if preds else 0.0
