"""Enhanced DBSCAN implementation with parameter optimization and spatial features.

This module provides an advanced DBSCAN clustering implementation specifically
designed for maritime vessel tracking data with enhanced spatial features,
parameter optimization, and streaming capabilities.
"""

import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, davies_bouldin_score
from scipy.spatial.distance import squareform

from ship_analytics.schemas import AisEvent


@dataclass
class ClusteringParameters:
    """Dataclass for DBSCAN clustering parameters."""
    
    eps: float = 0.5
    min_samples: int = 5
    algorithm: str = 'auto'
    leaf_size: int = 30
    metric: str = 'euclidean'
    metric_params: Optional[Dict] = None
    p: Optional[float] = None
    n_jobs: Optional[int] = None
    min_cluster_size: float = 0.01
    cluster_selection_method: str = 'eomatic'


@dataclass
class ClusteringMetrics:
    """Dataclass for clustering performance metrics."""
    
    silhouette_score: float = 0.0
    davies_bouldin_score: float = 0.0
    calinski_harabasz_score: float = 0.0
    dunn_index: float = 0.0
    elbow_score: float = 0.0
    knee_score: float = 0.0
    n_clusters: int = 0
    n_noise_points: int = 0
    cluster_sizes: List[int] = None
    
    def __post_init__(self):
        if self.cluster_sizes is None:
            self.cluster_sizes = []


class ParameterOptimizer:
    """Advanced DBSCAN parameter optimization using multiple algorithms."""
    
    def __init__(self, feature_matrix: np.ndarray, max_eps: float = 5.0, 
                 eps_steps: int = 50, min_samples_range: Tuple[int, int] = (3, 20)):
        self.feature_matrix = feature_matrix
        self.max_eps = max_eps
        self.eps_steps = eps_steps
        self.min_samples_range = min_samples_range
        self.silhouette_optimizer = SilhouetteOptimizer()
        self.elbow_optimizer = ElbowOptimizer()
        self.knee_optimizer = KneeOptimizer()
    
    def optimize_parameters(self) -> Dict[str, Any]:
        """Run all optimization algorithms and return consensus parameters."""
        # Generate parameter grid
        eps_values = np.linspace(0.1, self.max_eps, self.eps_steps)
        min_samples_values = list(range(self.min_samples_range[0], self.min_samples_range[1] + 1))
        
        # Grid search for baseline optimization
        grid_results = self._grid_search_optimization(eps_values, min_samples_values)
        
        # Silhouette score optimization
        silhouette_results = self.silhouette_optimizer.optimize(self.feature_matrix)
        
        # Elbow method optimization
        elbow_results = self.elbow_optimizer.optimize(self.feature_matrix)
        
        # Knee point detection
        knee_results = self.knee_optimizer.optimize(self.feature_matrix)
        
        # Combine results using ensemble method
        consensus_params = self._ensemble_parameter_selection(
            grid_results, silhouette_results, elbow_results, knee_results
        )
        
        return {
            'optimized_parameters': consensus_params,
            'grid_search_results': grid_results,
            'silhouette_results': silhouette_results,
            'elbow_results': elbow_results,
            'knee_results': knee_results,
            'optimization_method': 'ensemble',
        }
    
    def _grid_search_optimization(self, eps_values: np.ndarray, 
                                 min_samples_values: List[int]) -> Dict:
        """Perform grid search optimization over parameter space."""
        results = []
        
        for eps in eps_values:
            for min_samples in min_samples_values:
                # Apply DBSCAN
                clustering = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
                labels = clustering.fit_predict(self.feature_matrix)
                
                # Calculate metrics
                metrics = self._calculate_clustering_metrics(self.feature_matrix, labels)
                
                if metrics['n_clusters'] > 1:  # Only consider valid clusters
                    results.append({
                        'eps': eps,
                        'min_samples': min_samples,
                        'metrics': metrics,
                        'validity_scores': self._calculate_validity_scores(metrics),
                    })
        
        # Find best parameters based on silhouette score
        if results:
            best_result = max(results, key=lambda x: x['metrics']['silhouette_score'])
            return best_result
        else:
            return {'eps': 0.5, 'min_samples': 5, 'metrics': {}, 'validity_scores': {}}
    
    def _calculate_clustering_metrics(self, X: np.ndarray, labels: np.ndarray) -> Dict:
        """Calculate comprehensive clustering metrics."""
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise_points = sum(1 for label in labels if label == -1)
        
        metrics = {
            'n_clusters': n_clusters,
            'n_noise_points': n_noise_points,
            'silhouette_score': 0.0,
            'davies_bouldin_score': 0.0,
            'calinski_harabasz_score': 0.0,
            'dunn_index': 0.0,
            'cluster_sizes': [],
        }
        
        if n_clusters > 1 and n_noise_points < len(labels):
            # Calculate cluster validity metrics
            try:
                metrics['silhouette_score'] = silhouette_score(X, labels)
                metrics['davies_bouldin_score'] = davies_bouldin_score(X, labels)
                metrics['calinski_harabasz_score'] = self._calculate_calinski_harabasz(X, labels)
                metrics['dunn_index'] = self._calculate_dunn_index(X, labels)
            except Exception:
                pass
        
        # Calculate cluster sizes
        if n_clusters > 0:
            unique_labels = [l for l in set(labels) if l != -1]
            for label in unique_labels:
                cluster_size = np.sum(labels == label)
                metrics['cluster_sizes'].append(cluster_size)
        
        return metrics
    
    def _calculate_validity_scores(self, metrics: Dict) -> Dict:
        """Calculate enhanced validity scores for parameter selection."""
        scores = {}
        
        # Normalized silhouette score
        if metrics['silhouette_score'] > 0:
            scores['silhouette_normalized'] = metrics['silhouette_score']
        else:
            scores['silhouette_normalized'] = 0.0
        
        # Inverse Davies-Bouldin score (lower is better)
        if metrics['davies_bouldin_score'] > 0:
            scores['davies_bouldin_normalized'] = 1.0 / metrics['davies_bouldin_score']
        else:
            scores['davies_bouldin_normalized'] = 0.0
        
        # Cluster balance score (avoid very small or large clusters)
        if metrics['cluster_sizes']:
            sizes = np.array(metrics['cluster_sizes'])
            balance_score = 1.0 - np.std(sizes) / np.mean(sizes)
            scores['balance_score'] = max(0.0, min(1.0, balance_score))
        else:
            scores['balance_score'] = 0.0
        
        return scores
    
    def _calculate_calinski_harabasz(self, X: np.ndarray, labels: np.ndarray) -> float:
        """Calculate Calinski-Harabasz index."""
        if len(set(labels)) < 2:
            return 0.0
        
        # Between-cluster dispersion
        k = len(set(labels))
        overall_mean = np.mean(X, axis=0)
        
        between_disp = 0.0
        within_disp = 0.0
        
        for label in set(labels):
            if label == -1:  # Skip noise
                continue
            
            cluster_points = X[labels == label]
            cluster_mean = np.mean(cluster_points, axis=0)
            
            between_disp += np.sum((cluster_mean - overall_mean) ** 2) * len(cluster_points)
            within_disp += np.sum((cluster_points - cluster_mean) ** 2)
        
        if within_disp == 0:
            return 0.0
        
        return (between_disp / (k - 1)) / (within_disp / (len(X) - k))
    
    def _calculate_dunn_index(self, X: np.ndarray, labels: np.ndarray) -> float:
        """Calculate Dunn index."""
        if len(set(labels)) < 2:
            return 0.0
        
        # Find all cluster pairs
        unique_labels = [l for l in set(labels) if l != -1]
        if len(unique_labels) < 2:
            return 0.0
        
        min_distance = float('inf')
        
        for i in range(len(unique_labels)):
            for j in range(i + 1, len(unique_labels)):
                label_i = unique_labels[i]
                label_j = unique_labels[j]
                
                points_i = X[labels == label_i]
                points_j = X[labels == label_j]
                
                # Calculate distances between all pairs
                distances = []
                for point_i in points_i:
                    for point_j in points_j:
                        dist = np.linalg.norm(point_i - point_j)
                        distances.append(dist)
                
                if distances:
                    cluster_separation = min(distances)
                    min_diameter = min(
                        np.max(squareform(pdist(points_i))) if len(points_i) > 1 else 0,
                        np.max(squareform(pdist(points_j))) if len(points_j) > 1 else 0
                    )
                    
                    dunn_value = cluster_separation / max(min_diameter, 1e-10)
                    min_distance = min(min_distance, dunn_value)
        
        return min_distance if min_distance != float('inf') else 0.0
    
    def _ensemble_parameter_selection(self, grid_result: Dict, silhouette_result: Dict,
                                    elbow_result: Dict, knee_result: Dict) -> Dict:
        """Select parameters using ensemble method combining all optimization results."""
        candidates = []
        
        # Add grid search winner
        if grid_result:
            candidates.append(grid_result)
        
        # Add silhouette winner
        if silhouette_result:
            candidates.append(silhouette_result)
        
        # Add elbow winner
        if elbow_result:
            candidates.append(elbow_result)
        
        # Add knee winner
        if knee_result:
            candidates.append(knee_result)
        
        if not candidates:
            return {'eps': 0.5, 'min_samples': 5, 'method': 'fallback'}
        
        # Calculate weighted scores for each candidate
        candidate_scores = []
        for candidate in candidates:
            score = self._calculate_candidate_score(candidate)
            candidate_scores.append((score, candidate))
        
        # Select best candidate
        best_score, best_candidate = max(candidate_scores, key=lambda x: x[0])
        
        return {
            'eps': best_candidate.get('eps', 0.5),
            'min_samples': best_candidate.get('min_samples', 5),
            'method': 'ensemble_optimization',
            'confidence_score': best_score,
        }
    
    def _calculate_candidate_score(self, candidate: Dict) -> float:
        """Calculate overall score for a candidate parameter set."""
        scores = []
        
        if 'silhouette_score' in candidate.get('metrics', {}):
            silhouette = candidate['metrics']['silhouette_score']
            # Normalize silhouette to 0-1 scale (typical range: -1 to 1)
            scores.append(max(0, (silhouette + 1) / 2))
        
        if 'davies_bouldin_score' in candidate.get('metrics', {}):
            db_score = candidate['metrics']['davies_bouldin_score']
            if db_score > 0:
                # Lower DB score is better, invert to 0-1 scale
                scores.append(max(0, min(1, 1 - db_score / 2)))
        
        if 'cluster_sizes' in candidate.get('metrics', {}):
            sizes = candidate['metrics']['cluster_sizes']
            if sizes:
                balance = 1.0 - np.std(sizes) / np.mean(sizes)
                scores.append(max(0, min(1, balance)))
        
        if 'validity_scores' in candidate:
            validity = candidate['validity_scores']
            for score_name, score in validity.items():
                if isinstance(score, (int, float)):
                    scores.append(score)
        
        return np.mean(scores) if scores else 0.0


class SilhouetteOptimizer:
    """Silhouette score based parameter optimization."""
    
    def optimize(self, X: np.ndarray, eps_values: Optional[List[float]] = None) -> Dict:
        """Optimize parameters using silhouette score maximization."""
        if eps_values is None:
            eps_values = np.linspace(0.1, 5.0, 30)
        
        best_score = -1
        best_params = {'eps': 0.5, 'min_samples': 5}
        
        for eps in eps_values:
            clustering = DBSCAN(eps=eps, min_samples=5, metric='euclidean')
            labels = clustering.fit_predict(X)
            
            if len(set(labels)) - (1 if -1 in labels else 0) > 1:
                try:
                    score = silhouette_score(X, labels)
                    if score > best_score:
                        best_score = score
                        best_params = {'eps': eps, 'min_samples': 5}
                except Exception:
                    pass
        
        return {
            'eps': best_params['eps'],
            'min_samples': best_params['min_samples'],
            'silhouette_score': best_score,
            'method': 'silhouette_optimization',
        }


class ElbowOptimizer:
    """Elbow method based parameter optimization."""
    
    def optimize(self, X: np.ndarray) -> Dict:
        """Optimize parameters using elbow method."""
        eps_values = np.linspace(0.1, 5.0, 40)
        silhouette_scores = []
        
        for eps in eps_values:
            clustering = DBSCAN(eps=eps, min_samples=5, metric='euclidean')
            labels = clustering.fit_predict(X)
            
            if len(set(labels)) - (1 if -1 in labels else 0) > 1:
                try:
                    score = silhouette_score(X, labels)
                    silhouette_scores.append(score)
                except Exception:
                    silhouette_scores.append(0)
            else:
                silhouette_scores.append(0)
        
        # Find elbow point
        if len(silhouette_scores) > 5:
            scores_array = np.array(silhouette_scores)
            elbow_idx = self._find_elbow_point(scores_array)
            
            return {
                'eps': eps_values[elbow_idx],
                'min_samples': 5,
                'silhouette_score': silhouette_scores[elbow_idx],
                'elbow_index': elbow_idx,
                'method': 'elbow_optimization',
            }
        
        return {'eps': 0.5, 'min_samples': 5, 'silhouette_score': 0.0, 'method': 'elbow_optimization'}
    
    def _find_elbow_point(self, scores: np.ndarray) -> int:
        """Find elbow point using Kneedle algorithm."""
        if len(scores) < 3:
            return len(scores) - 1
        
        # Normalize scores
        normalized = (scores - scores.min()) / (scores.max() - scores.min())
        
        # Calculate distance from ideal point
        ideal_point = np.array([0, 1])  # Start high, end low
        distances = []
        
        for i, score in enumerate(normalized):
            point = np.array([i / (len(normalized) - 1), score])
            distances.append(np.linalg.norm(point - ideal_point))
        
        # Find minimum distance (elbow)
        return np.argmin(distances)


class KneeDetector:
    """Knee point detection for DBSCAN parameter optimization."""
    
    def optimize(self, X: np.ndarray) -> Dict:
        """Optimize parameters using knee point detection."""
        eps_values = np.linspace(0.1, 5.0, 40)
        silhouette_scores = []
        
        for eps in eps_values:
            clustering = DBSCAN(eps=eps, min_samples=5, metric='euclidean')
            labels = clustering.fit_predict(X)
            
            if len(set(labels)) - (1 if -1 in labels else 0) > 1:
                try:
                    score = silhouette_score(X, labels)
                    silhouette_scores.append(score)
                except Exception:
                    silhouette_scores.append(0)
            else:
                silhouette_scores.append(0)
        
        # Detect knee point using curvature method
        if len(silhouette_scores) > 5:
            knee_idx = self._detect_knee_point(silhouette_scores)
            
            return {
                'eps': eps_values[knee_idx],
                'min_samples': 5,
                'silhouette_score': silhouette_scores[knee_idx],
                'knee_index': knee_idx,
                'method': 'knee_detection',
            }
        
        return {'eps': 0.5, 'min_samples': 5, 'silhouette_score': 0.0, 'method': 'knee_detection'}
    
    def _detect_knee_point(self, scores: List[float]) -> int:
        """Detect knee point using curvature method."""
        if len(scores) < 3:
            return len(scores) - 1
        
        # Convert to numpy array
        scores_array = np.array(scores)
        
        # Calculate first and second derivatives
        first_derivative = np.gradient(scores_array)
        second_derivative = np.gradient(first_derivative)
        
        # Calculate curvature
        curvature = np.abs(second_derivative) / (1 + first_derivative ** 2) ** 1.5
        
        # Find point with maximum curvature (knee point)
        knee_idx = np.argmax(curvature)
        
        return knee_idx


class SpatialFeatureExtractor:
    """Enhanced spatial feature extraction for maritime vessel tracking."""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.geometric_transformer = GeometricTransformer()
    
    def extract_features(self, event: AisEvent, vessel_history: List[AisEvent]) -> np.ndarray:
        """Extract comprehensive spatial features for a vessel event."""
        # Basic spatial features
        basic_features = self._extract_basic_spatial_features(event, vessel_history)
        
        # Velocity and course features
        velocity_features = self._extract_velocity_features(event, vessel_history)
        
        # Trajectory features
        trajectory_features = self._extract_trajectory_features(event, vessel_history)
        
        # Statistical features
        statistical_features = self._extract_statistical_features(event, vessel_history)
        
        # Geometric features
        geometric_features = self.geometric_transformer.transform(event, vessel_history)
        
        # Combine all features
        all_features = np.concatenate([
            basic_features,
            velocity_features,
            trajectory_features,
            statistical_features,
            geometric_features,
        ])
        
        return all_features
    
    def _extract_basic_spatial_features(self, event: AisEvent, 
                                       vessel_history: List[AisEvent]) -> np.ndarray:
        """Extract basic spatial position features."""
        features = [
            event.latitude,
            event.longitude,
            event.sog_knots or 0.0,
            event.cog_degrees or 0.0,
            event.heading_degrees or 0.0,
            event.event_time.hour,
            event.event_time.weekday(),
            event.event_time.day,
        ]
        
        # Add recent position differences
        if len(vessel_history) >= 2:
            last_event = vessel_history[-1]
            features.extend([
                event.latitude - last_event.latitude,
                event.longitude - last_event.longitude,
                (event.sog_knots or 0.0) - (last_event.sog_knots or 0.0),
                (event.cog_degrees or 0.0) - (last_event.cog_degrees or 0.0),
            ])
        
        return np.array(features)
    
    def _extract_velocity_features(self, event: AisEvent, 
                                   vessel_history: List[AisEvent]) -> np.ndarray:
        """Extract velocity and course consistency features."""
        if len(vessel_history) < 3:
            return np.zeros(4)
        
        # Get recent events
        recent_events = vessel_history[-5:] if len(vessel_history) >= 5 else vessel_history
        
        # Speed features
        speeds = [e.sog_knots or 0.0 for e in recent_events]
        speed_changes = np.diff(speeds) if len(speeds) > 1 else np.array([0.0])
        
        # Course features
        courses = [e.cog_degrees or 0.0 for e in recent_events if e.cog_degrees is not None]
        course_changes = np.diff(courses) if len(courses) > 1 else np.array([0.0])
        
        # Calculate statistics
        features = [
            np.std(speeds) if len(speeds) > 1 else 0.0,
            np.mean(speeds) if speeds else 0.0,
            np.max(speed_changes) if len(speed_changes) > 0 else 0.0,
            np.min(speed_changes) if len(speed_changes) > 0 else 0.0,
            np.std(courses) if len(courses) > 1 else 0.0,
            np.mean(courses) if courses else 0.0,
            np.max(course_changes) if len(course_changes) > 0 else 0.0,
            np.min(course_changes) if len(course_changes) > 0 else 0.0,
        ]
        
        return np.array(features)
    
    def _extract_trajectory_features(self, event: AisEvent, 
                                     vessel_history: List[AisEvent]) -> np.ndarray:
        """Extract trajectory curvature and pattern features."""
        if len(vessel_history) < 3:
            return np.zeros(6)
        
        # Get trajectory points
        trajectory = [(e.latitude, e.longitude) for e in vessel_history[-10:]]
        
        if len(trajectory) < 3:
            return np.zeros(6)
        
        # Calculate curvature
        curvatures = []
        for i in range(1, len(trajectory) - 1):
            p1, p2, p3 = trajectory[i-1], trajectory[i], trajectory[i+1]
            
            # Calculate curvature using three-point formula
            x1, y1 = p1
            x2, y2 = p2
            x3, y3 = p3
            
            # Cross product of vectors
            dx1 = x2 - x1
            dy1 = y2 - y1
            dx2 = x3 - x2
            dy2 = y3 - y2
            
            cross_product = dx1 * dy2 - dy1 * dx2
            magnitude1 = np.sqrt(dx1**2 + dy1**2)
            magnitude2 = np.sqrt(dx2**2 + dy2**2)
            
            if magnitude1 > 0 and magnitude2 > 0:
                curvature = abs(cross_product) / (magnitude1 * magnitude2)
                curvatures.append(curvature)
        
        # Calculate trajectory statistics
        features = [
            np.mean(curvatures) if curvatures else 0.0,
            np.std(curvatures) if curvatures else 0.0,
            max(curvatures) if curvatures else 0.0,
            np.mean(np.diff([p[0] for p in trajectory])) if len(trajectory) > 1 else 0.0,
            np.mean(np.diff([p[1] for p in trajectory])) if len(trajectory) > 1 else 0.0,
            len(trajectory),
        ]
        
        return np.array(features)
    
    def _extract_statistical_features(self, event: AisEvent, 
                                      vessel_history: List[AisEvent]) -> np.ndarray:
        """Extract statistical features from vessel history."""
        if not vessel_history:
            return np.zeros(8)
        
        # Extract all relevant data
        lats = [e.latitude for e in vessel_history]
        lons = [e.longitude for e in vessel_history]
        speeds = [e.sog_knots or 0.0 for e in vessel_history]
        courses = [e.cog_degrees or 0.0 for e in vessel_history if e.cog_degrees is not None]
        
        # Calculate statistics
        features = [
            np.mean(lats) if lats else 0.0,
            np.std(lats) if len(lats) > 1 else 0.0,
            np.max(lats) - np.min(lats) if lats else 0.0,  # Latitude range
            np.mean(lons) if lons else 0.0,
            np.std(lons) if len(lons) > 1 else 0.0,
            np.max(lons) - np.min(lons) if lons else 0.0,  # Longitude range
            np.mean(speeds) if speeds else 0.0,
            np.std(speeds) if len(speeds) > 1 else 0.0,
            np.mean(courses) if courses else 0.0,
            np.std(courses) if len(courses) > 1 else 0.0,
        ]
        
        return np.array(features)
    
    def fit(self, X: np.ndarray):
        """Fit the scaler to the feature matrix."""
        self.scaler.fit(X)
        return self
    
    def transform(self, X: np.ndarray) -> np.ndarray:
        """Transform features using the fitted scaler."""
        return self.scaler.transform(X)


class GeometricTransformer:
    """Geometric transformations for vessel trajectory analysis."""
    
    def transform(self, event: AisEvent, vessel_history: List[AisEvent]) -> np.ndarray:
        """Transform geometric features from vessel event."""
        features = []
        
        if len(vessel_history) < 2:
            return np.zeros(5)
        
        # Get recent trajectory
        trajectory = [(e.latitude, e.longitude, e.sog_knots or 0.0, e.cog_degrees or 0.0)
                     for e in vessel_history[-5:]]
        
        # Calculate geometric properties
        features.extend([
            self._calculate_trajectory_length(trajectory),
            self._calculate_direction_change(trajectory),
            self._calculate_speed_variation(trajectory),
            self._calculate_course_consistency(trajectory),
            self._calculate_spatial_density(trajectory),
        ])
        
        return np.array(features)
    
    def _calculate_trajectory_length(self, trajectory: List[tuple]) -> float:
        """Calculate total trajectory length."""
        if len(trajectory) < 2:
            return 0.0
        
        total_distance = 0.0
        for i in range(1, len(trajectory)):
            lat1, lon1, _, _ = trajectory[i-1]
            lat2, lon2, _, _ = trajectory[i]
            distance = self._haversine_distance(lat1, lon1, lat2, lon2)
            total_distance += distance
        
        return total_distance
    
    def _calculate_direction_change(self, trajectory: List[tuple]) -> float:
        """Calculate total direction change in trajectory."""
        if len(trajectory) < 3:
            return 0.0
        
        direction_changes = []
        for i in range(1, len(trajectory) - 1):
            _, _, _, cog1 = trajectory[i-1]
            _, _, _, cog2 = trajectory[i]
            _, _, _, cog3 = trajectory[i+1]
            
            change = abs((cog3 - cog1 + 180) % 360 - 180)
            direction_changes.append(change)
        
        return np.mean(direction_changes) if direction_changes else 0.0
    
    def _calculate_speed_variation(self, trajectory: List[tuple]) -> float:
        """Calculate speed variation in trajectory."""
        if len(trajectory) < 2:
            return 0.0
        
        speeds = [t[2] for t in trajectory]
        speed_changes = np.diff(speeds)
        
        return np.std(speed_changes) if len(speed_changes) > 1 else 0.0
    
    def _calculate_course_consistency(self, trajectory: List[tuple]) -> float:
        """Calculate course consistency in trajectory."""
        if len(trajectory) < 3:
            return 0.0
        
        courses = [t[3] for t in trajectory if t[3] is not None]
        if len(courses) < 2:
            return 0.0
        
        # Calculate circular variance
        cos_values = np.cos(np.radians(courses))
        sin_values = np.sin(np.radians(courses))
        
        mean_cos = np.mean(cos_values)
        mean_sin = np.mean(sin_values)
        
        circular_variance = 1 - np.sqrt(mean_cos**2 + mean_sin**2)
        return circular_variance
    
    def _calculate_spatial_density(self, trajectory: List[tuple]) -> float:
        """Calculate spatial density of trajectory points."""
        if len(trajectory) < 3:
            return 0.0
        
        # Convert to numpy array for distance calculations
        points = np.array([(t[0], t[1]) for t in trajectory])
        
        # Calculate average distance between consecutive points
        distances = []
        for i in range(1, len(points)):
            dist = np.linalg.norm(points[i] - points[i-1])
            distances.append(dist)
        
        return np.mean(distances) if distances else 0.0
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate great-circle distance between two points in kilometers."""
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371  # Earth radius in km
        
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        return R * c


class AdvancedDBSCAN:
    """Advanced DBSCAN implementation with parameter optimization and feature enhancement."""
    
    def __init__(self, spatial_feature_extractor: Optional[SpatialFeatureExtractor] = None,
                 parameter_optimizer: Optional[ParameterOptimizer] = None):
        self.spatial_feature_extractor = spatial_feature_extractor or SpatialFeatureExtractor()
        self.parameter_optimizer = parameter_optimizer
        self.is_fitted = False
        self.optimal_parameters = None
        self.clustering_metrics = None
        self.scaler = StandardScaler()
    
    def fit(self, vessel_events: List[AisEvent]) -> 'AdvancedDBSCAN':
        """Train DBSCAN clustering with optimized parameters."""
        if len(vessel_events) < 2:
            raise ValueError("Need at least 2 events for clustering")
        
        # Extract features for all events
        feature_matrix = []
        for event in vessel_events:
            features = self.spatial_feature_extractor.extract_features(event, vessel_events)
            feature_matrix.append(features)
        
        X = np.array(feature_matrix)
        
        # Optimize parameters if optimizer is provided
        if self.parameter_optimizer:
            optimization_result = self.parameter_optimizer.optimize_parameters(X)
            self.optimal_parameters = optimization_result['optimized_parameters']
            eps = self.optimal_parameters['eps']
            min_samples = self.optimal_parameters['min_samples']
            optimization_method = self.optimal_parameters.get('method', 'unknown')
        else:
            # Fallback to default parameters
            eps = 0.5
            min_samples = 5
            optimization_method = 'default'
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Apply DBSCAN
        self.dbscan = DBSCAN(
            eps=eps,
            min_samples=min_samples,
            metric='euclidean',
            algorithm='auto',
            leaf_size=30,
        )
        
        self.labels_ = self.dbscan.fit_predict(X_scaled)
        
        # Calculate clustering metrics
        self.clustering_metrics = self._calculate_clustering_metrics(X_scaled, self.labels_)
        
        # Store original number of events
        self.n_events_ = len(vessel_events)
        
        self.is_fitted = True
        
        return self
    
    def predict(self, event: AisEvent, vessel_events: List[AisEvent]) -> int:
        """Predict cluster for a single event."""
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        # Extract features for the event
        features = self.spatial_feature_extractor.extract_features(event, vessel_events)
        features_scaled = self.scaler.transform(features.reshape(1, -1))
        
        # Predict cluster
        cluster_label = self.dbscan.predict(features_scaled)
        
        return int(cluster_label)
    
    def fit_predict(self, vessel_events: List[AisEvent]) -> np.ndarray:
        """Fit clustering and predict cluster labels for all events."""
        self.fit(vessel_events)
        return self.labels_
    
    def get_clusters(self, vessel_events: List[AisEvent]) -> Dict[str, Any]:
        """Get detailed cluster information."""
        if not self.is_fitted:
            raise ValueError("Model must be fitted before getting clusters")
        
        clusters = {}
        
        for label in np.unique(self.labels_):
            if label == -1:  # Skip noise
                continue
            
            cluster_indices = np.where(self.labels_ == label)[0]
            cluster_events = [vessel_events[i] for i in cluster_indices]
            
            clusters[f"cluster_{label}"] = {
                "size": len(cluster_events),
                "events": cluster_events,
                "event_ids": [event.event_id for event in cluster_events],
                "mmsi": cluster_events[0].mmsi if cluster_events else None,
                "start_time": min(event.event_time for event in cluster_events),
                "end_time": max(event.event_time for event in cluster_events),
                "total_distance": sum(self._calculate_event_distance(cluster_events[i], cluster_events[i+1])
                                     for i in range(len(cluster_events)-1)),
                "average_speed": np.mean([event.sog_knots or 0 for event in cluster_events]),
                "average_course": np.mean([event.cog_degrees or 0 for event in cluster_events if event.cog_degrees is not None]),
            }
        
        # Add noise points
        noise_indices = np.where(self.labels_ == -1)[0]
        if len(noise_indices) > 0:
            noise_events = [vessel_events[i] for i in noise_indices]
            clusters["noise"] = {
                "size": len(noise_events),
                "events": noise_events,
                "event_ids": [event.event_id for event in noise_events],
                "description": "Outlier points not assigned to any cluster",
            }
        
        return clusters
    
    def get_clustering_summary(self) -> Dict[str, Any]:
        """Get comprehensive clustering summary."""
        if not self.is_fitted:
            raise ValueError("Model must be fitted before getting summary")
        
        return {
            "n_clusters": len(set(self.labels_)) - (1 if -1 in self.labels_ else 0),
            "n_noise_points": sum(1 for label in self.labels_ if label == -1),
            "silhouette_score": self.clustering_metrics.silhouette_score if self.clustering_metrics else 0.0,
            "davies_bouldin_score": self.clustering_metrics.davies_bouldin_score if self.clustering_metrics else 0.0,
            "calinski_harabasz_score": self.clustering_metrics.calinski_harabasz_score if self.clustering_metrics else 0.0,
            "dunn_index": self.clustering_metrics.dunn_index if self.clustering_metrics else 0.0,
            "cluster_sizes": self.clustering_metrics.cluster_sizes if self.clustering_metrics else [],
            "eps": self.optimal_parameters['eps'] if self.optimal_parameters else 0.5,
            "min_samples": self.optimal_parameters['min_samples'] if self.optimal_parameters else 5,
            "optimization_method": self.optimal_parameters['method'] if self.optimal_parameters else 'default',
        }
    
    def _calculate_clustering_metrics(self, X: np.ndarray, labels: np.ndarray) -> ClusteringMetrics:
        """Calculate comprehensive clustering metrics."""
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise_points = sum(1 for label in labels if label == -1)
        
        metrics = ClusteringMetrics(
            n_clusters=n_clusters,
            n_noise_points=n_noise_points,
        )
        
        if n_clusters > 1 and n_noise_points < len(labels):
            # Calculate validity metrics
            try:
                metrics.silhouette_score = silhouette_score(X, labels)
                metrics.davies_bouldin_score = davies_bouldin_score(X, labels)
                metrics.calinski_harabasz_score = self._calculate_calinski_harabasz(X, labels)
                metrics.dunn_index = self._calculate_dunn_index(X, labels)
            except Exception:
                pass
        
        return metrics
    
    def _calculate_calinski_harabasz(self, X: np.ndarray, labels: np.ndarray) -> float:
        """Calculate Calinski-Harabasz index."""
        if len(set(labels)) < 2:
            return 0.0
        
        k = len(set(labels))
        overall_mean = np.mean(X, axis=0)
        
        between_disp = 0.0
        within_disp = 0.0
        
        for label in set(labels):
            if label == -1:
                continue
            
            cluster_points = X[labels == label]
            cluster_mean = np.mean(cluster_points, axis=0)
            
            between_disp += np.sum((cluster_mean - overall_mean) ** 2) * len(cluster_points)
            within_disp += np.sum((cluster_points - cluster_mean) ** 2)
        
        if within_disp == 0:
            return 0.0
        
        return (between_disp / (k - 1)) / (within_disp / (len(X) - k))
    
    def _calculate_dunn_index(self, X: np.ndarray, labels: np.ndarray) -> float:
        """Calculate Dunn index."""
        if len(set(labels)) < 2:
            return 0.0
        
        unique_labels = [l for l in set(labels) if l != -1]
        if len(unique_labels) < 2:
            return 0.0
        
        min_distance = float('inf')
        
        for i in range(len(unique_labels)):
            for j in range(i + 1, len(unique_labels)):
                label_i = unique_labels[i]
                label_j = unique_labels[j]
                
                points_i = X[labels == label_i]
                points_j = X[labels == label_j]
                
                distances = []
                for point_i in points_i:
                    for point_j in points_j:
                        dist = np.linalg.norm(point_i - point_j)
                        distances.append(dist)
                
                if distances:
                    cluster_separation = min(distances)
                    min_diameter = min(
                        np.max(squareform(pdist(points_i))) if len(points_i) > 1 else 0,
                        np.max(squareform(pdist(points_j))) if len(points_j) > 1 else 0
                    )
                    
                    dunn_value = cluster_separation / max(min_diameter, 1e-10)
                    min_distance = min(min_distance, dunn_value)
        
        return min_distance if min_distance != float('inf') else 0.0
    
    def _calculate_event_distance(self, event1: AisEvent, event2: AisEvent) -> float:
        """Calculate distance between two AIS events."""
        return self._haversine_distance(event1.latitude, event1.longitude, event2.latitude, event2.longitude)
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate great-circle distance between two points in kilometers."""
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371  # Earth radius in km
        
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        return R * c

    def __repr__(self) -> str:
        return f"AdvancedDBSCAN(eps={self.optimal_parameters['eps'] if self.optimal_parameters else 0.5}, " \
               f"min_samples={self.optimal_parameters['min_samples'] if self.optimal_parameters else 5}, " \
               f"clusters={self.clustering_metrics.n_clusters if self.clustering_metrics else 0})"


# Convenience function for quick clustering
async def discover_routes_with_advanced_dbscan(
    vessel_events: List[AisEvent],
    eps: float = 0.5,
    min_samples: int = 5,
    use_optimization: bool = False,
) -> Dict[str, Any]:
    """Quick function to discover routes using advanced DBSCAN."""
    spatial_extractor = SpatialFeatureExtractor()
    
    # Extract features
    feature_matrix = []
    for event in vessel_events:
        features = spatial_extractor.extract_features(event, vessel_events)
        feature_matrix.append(features)
    
    X = np.array(feature_matrix)
    
    # Optimize parameters if requested
    if use_optimization:
        optimizer = ParameterOptimizer(X)
        optimization_result = optimizer.optimize_parameters()
        eps = optimization_result['optimized_parameters']['eps']
        min_samples = optimization_result['optimized_parameters']['min_samples']
    
    # Apply DBSCAN
    clustering = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')
    labels = clustering.fit_predict(X)
    
    # Calculate metrics
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise_points = sum(1 for label in labels if label == -1)
    
    return {
        "clusters": {
            f"cluster_{label}": {
                "size": int(sum(labels == label)),
                "events": [vessel_events[i] for i in range(len(vessel_events)) if labels[i] == label],
                "event_ids": [vessel_events[i].event_id for i in range(len(vessel_events)) if labels[i] == label],
                "mmsi": vessel_events[0].mmsi if vessel_events else None,
            }
            for label in set(labels) - {-1}
        },
        "noise_count": n_noise_points,
        "n_clusters": n_clusters,
        "parameters_used": {
            "eps": eps,
            "min_samples": min_samples,
            "optimization_used": use_optimization,
        },
    }