"""
Performance Benchmark Script

Benchmarks key operations in the pipeline
"""

import os
import time
import statistics
from typing import List, Dict
from datetime import datetime, timedelta
from src.features.smartdb_contract import SmartDBContract
from src.config.settings import config
from sqlalchemy import create_engine
from feast import FeatureStore


class PerformanceBenchmark:
    """Benchmarks pipeline performance"""
    
    def __init__(self):
        self.db_engine = create_engine(config.database.url)
        self.contract = SmartDBContract(self.db_engine)
        self.feast_store = FeatureStore(repo_path=os.getenv('FEAST_REPO_PATH', './feast_repo'))
    
    def benchmark_pit_snapshot(self, num_iterations: int = 100) -> Dict:
        """Benchmark PIT snapshot query"""
        times: List[float] = []
        symbol = 'AAPL'
        query_time = datetime.now() - timedelta(hours=1)
        
        for _ in range(num_iterations):
            start = time.time()
            snapshot = self.contract.get_pit_snapshot(symbol, query_time)
            elapsed = time.time() - start
            times.append(elapsed)
        
        return {
            'operation': 'pit_snapshot',
            'iterations': num_iterations,
            'mean_ms': statistics.mean(times) * 1000,
            'median_ms': statistics.median(times) * 1000,
            'p95_ms': statistics.quantiles(times, n=20)[18] * 1000,
            'p99_ms': statistics.quantiles(times, n=100)[98] * 1000,
            'min_ms': min(times) * 1000,
            'max_ms': max(times) * 1000
        }
    
    def benchmark_feast_online(self, num_iterations: int = 100) -> Dict:
        """Benchmark Feast online feature retrieval"""
        times: List[float] = []
        
        for _ in range(num_iterations):
            start = time.time()
            features = self.feast_store.get_online_features(
                features=['technical_indicators:sma_20'],
                entity_rows=[{'symbol': 'AAPL'}]
            )
            elapsed = time.time() - start
            times.append(elapsed)
        
        return {
            'operation': 'feast_online',
            'iterations': num_iterations,
            'mean_ms': statistics.mean(times) * 1000,
            'median_ms': statistics.median(times) * 1000,
            'p95_ms': statistics.quantiles(times, n=20)[18] * 1000,
            'p99_ms': statistics.quantiles(times, n=100)[98] * 1000,
            'min_ms': min(times) * 1000,
            'max_ms': max(times) * 1000
        }
    
    def benchmark_feast_historical(self, num_iterations: int = 10) -> Dict:
        """Benchmark Feast historical feature retrieval"""
        import pandas as pd
        
        times: List[float] = []
        entity_df = pd.DataFrame({
            'symbol': ['AAPL'] * 100,
            'event_timestamp': [datetime.now() - timedelta(hours=i) for i in range(100)]
        })
        
        for _ in range(num_iterations):
            start = time.time()
            features = self.feast_store.get_historical_features(
                entity_df=entity_df,
                features=['technical_indicators:sma_20']
            )
            elapsed = time.time() - start
            times.append(elapsed)
        
        return {
            'operation': 'feast_historical_100_timestamps',
            'iterations': num_iterations,
            'mean_ms': statistics.mean(times) * 1000,
            'median_ms': statistics.median(times) * 1000,
            'p95_ms': statistics.quantiles(times, n=20)[18] * 1000,
            'p99_ms': statistics.quantiles(times, n=100)[98] * 1000,
            'min_ms': min(times) * 1000,
            'max_ms': max(times) * 1000
        }
    
    def run_all_benchmarks(self) -> List[Dict]:
        """Run all benchmarks"""
        print("🚀 Running performance benchmarks...")
        print()
        
        results = []
        
        print("1. Benchmarking PIT snapshot...")
        results.append(self.benchmark_pit_snapshot())
        
        print("2. Benchmarking Feast online features...")
        results.append(self.benchmark_feast_online())
        
        print("3. Benchmarking Feast historical features...")
        results.append(self.benchmark_feast_historical())
        
        return results
    
    def print_results(self, results: List[Dict]):
        """Print benchmark results"""
        print("\n" + "="*80)
        print("Performance Benchmark Results")
        print("="*80 + "\n")
        
        for result in results:
            print(f"Operation: {result['operation']}")
            print(f"  Iterations: {result['iterations']}")
            print(f"  Mean: {result['mean_ms']:.2f} ms")
            print(f"  Median: {result['median_ms']:.2f} ms")
            print(f"  P95: {result['p95_ms']:.2f} ms")
            print(f"  P99: {result['p99_ms']:.2f} ms")
            print(f"  Min: {result['min_ms']:.2f} ms")
            print(f"  Max: {result['max_ms']:.2f} ms")
            print()
        
        # Check SLA compliance
        print("SLA Compliance Check:")
        print("-" * 80)
        
        pit_result = next(r for r in results if r['operation'] == 'pit_snapshot')
        if pit_result['p95_ms'] < 100:
            print("✅ PIT Snapshot P95 < 100ms (SLA: instant)")
        else:
            print(f"⚠️  PIT Snapshot P95: {pit_result['p95_ms']:.2f}ms (target: <100ms)")
        
        feast_online = next(r for r in results if r['operation'] == 'feast_online')
        if feast_online['p95_ms'] < 100:
            print("✅ Feast Online P95 < 100ms (SLA: <100ms)")
        else:
            print(f"⚠️  Feast Online P95: {feast_online['p95_ms']:.2f}ms (target: <100ms)")


def main():
    """Main entry point"""
    benchmark = PerformanceBenchmark()
    results = benchmark.run_all_benchmarks()
    benchmark.print_results(results)


if __name__ == "__main__":
    main()
