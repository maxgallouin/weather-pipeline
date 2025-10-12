"""
Data quality checks for ML-ready weather data.

Validates transformed data in weather_observations_by_postal_code and
weather_forecasts_by_postal_code tables to ensure they meet ML requirements.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

try:
    from src.database import fetch_all, fetch_one
except ImportError:
    from database import fetch_all, fetch_one

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Performs data quality checks on ML-ready weather tables."""

    def __init__(self):
        self.checks_passed = 0
        self.checks_failed = 0
        self.warnings = []
        self.errors = []

    def check_data_freshness(
        self, table: str, max_age_hours: int = 2
    ) -> Dict[str, any]:
        """
        Check if data is fresh (not stale).

        Args:
            table: Table name (observations or forecasts)
            max_age_hours: Maximum acceptable age in hours

        Returns:
            Check result dictionary
        """
        query = f"""
            SELECT
                MAX(processed_at) as latest_processed,
                COUNT(*) as total_records
            FROM {table}
            WHERE processed_at >= NOW() - INTERVAL '{max_age_hours * 2} hours'
        """

        result = fetch_one(query)

        if not result or not result["latest_processed"]:
            self.errors.append(f"❌ {table}: No recent data found")
            self.checks_failed += 1
            return {"passed": False, "table": table, "error": "No data"}

        age_hours = (datetime.utcnow() - result["latest_processed"]).total_seconds() / 3600

        if age_hours > max_age_hours:
            self.warnings.append(
                f"⚠️ {table}: Data is {age_hours:.1f} hours old (threshold: {max_age_hours}h)"
            )
            self.checks_failed += 1
            return {
                "passed": False,
                "table": table,
                "age_hours": age_hours,
                "threshold": max_age_hours
            }

        logger.info(f"✓ {table}: Data freshness OK ({age_hours:.1f}h old)")
        self.checks_passed += 1
        return {
            "passed": True,
            "table": table,
            "age_hours": age_hours,
            "total_records": result["total_records"]
        }

    def check_temporal_continuity(
        self, table: str, lookback_hours: int = 24, max_gap_hours: int = 2
    ) -> Dict[str, any]:
        """
        Check for gaps in time series data (missing hours).

        Args:
            table: Table name
            lookback_hours: How far back to check
            max_gap_hours: Maximum acceptable gap

        Returns:
            Check result dictionary
        """
        query = f"""
            WITH hourly_data AS (
                SELECT
                    postal_code,
                    timestamp,
                    LAG(timestamp) OVER (PARTITION BY postal_code ORDER BY timestamp) as prev_timestamp
                FROM {table}
                WHERE timestamp >= NOW() - INTERVAL '{lookback_hours} hours'
            ),
            gaps AS (
                SELECT
                    postal_code,
                    timestamp,
                    prev_timestamp,
                    EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600 as gap_hours
                FROM hourly_data
                WHERE prev_timestamp IS NOT NULL
                  AND EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600 > {max_gap_hours}
            )
            SELECT
                COUNT(*) as gap_count,
                AVG(gap_hours) as avg_gap_hours,
                MAX(gap_hours) as max_gap_hours
            FROM gaps
        """

        result = fetch_one(query)

        if result["gap_count"] > 0:
            self.warnings.append(
                f"⚠️ {table}: Found {result['gap_count']} temporal gaps "
                f"(avg: {result['avg_gap_hours']:.1f}h, max: {result['max_gap_hours']:.1f}h)"
            )
            self.checks_failed += 1
            return {
                "passed": False,
                "table": table,
                "gap_count": result["gap_count"],
                "avg_gap_hours": result["avg_gap_hours"],
                "max_gap_hours": result["max_gap_hours"]
            }

        logger.info(f"✓ {table}: No temporal gaps detected")
        self.checks_passed += 1
        return {"passed": True, "table": table, "gap_count": 0}

    def check_data_completeness(
        self, table: str, min_completeness: float = 0.8, core_fields: Optional[List[str]] = None
    ) -> Dict[str, any]:
        """
        Check that core fields have sufficient non-null data.

        Args:
            table: Table name
            min_completeness: Minimum acceptable completeness ratio
            core_fields: List of fields to check (defaults to observations fields)

        Returns:
            Check result dictionary
        """
        # Core fields required for ML (default for observations)
        if core_fields is None:
            core_fields = [
                "temperature",
                "precipitation",
                "pressure_msl",
                "relative_humidity",
                "wind_speed"
            ]

        query = f"""
            SELECT
                COUNT(*) as total_records,
                {', '.join([f"COUNT({field}) as {field}_count" for field in core_fields])}
            FROM {table}
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """

        result = fetch_one(query)

        if not result or result["total_records"] == 0:
            self.errors.append(f"❌ {table}: No data in last 24 hours")
            self.checks_failed += 1
            return {"passed": False, "table": table, "error": "No data"}

        total = result["total_records"]
        incomplete_fields = []

        for field in core_fields:
            count = result[f"{field}_count"]
            completeness = count / total
            if completeness < min_completeness:
                incomplete_fields.append(
                    f"{field}: {completeness:.1%}"
                )

        if incomplete_fields:
            self.warnings.append(
                f"⚠️ {table}: Low completeness for {', '.join(incomplete_fields)}"
            )
            self.checks_failed += 1
            return {
                "passed": False,
                "table": table,
                "incomplete_fields": incomplete_fields,
                "total_records": total
            }

        logger.info(f"✓ {table}: Data completeness OK (all fields ≥ {min_completeness:.0%})")
        self.checks_passed += 1
        return {"passed": True, "table": table, "total_records": total}

    def check_quality_scores(
        self, table: str, min_avg_quality: float = 0.7, max_low_quality_pct: float = 0.2
    ) -> Dict[str, any]:
        """
        Check overall quality scores are acceptable.

        Args:
            table: Table name
            min_avg_quality: Minimum acceptable average quality
            max_low_quality_pct: Maximum acceptable % of low quality records

        Returns:
            Check result dictionary
        """
        query = f"""
            SELECT
                AVG(quality_score) as avg_quality,
                MIN(quality_score) as min_quality,
                MAX(quality_score) as max_quality,
                COUNT(*) FILTER (WHERE quality_score < 0.5) as low_quality_count,
                COUNT(*) as total_count
            FROM {table}
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """

        result = fetch_one(query)

        if not result or result["total_count"] == 0:
            self.errors.append(f"❌ {table}: No data for quality check")
            self.checks_failed += 1
            return {"passed": False, "table": table, "error": "No data"}

        avg_quality = result["avg_quality"]
        low_quality_pct = result["low_quality_count"] / result["total_count"]

        issues = []
        if avg_quality < min_avg_quality:
            issues.append(f"avg quality: {avg_quality:.2f} (threshold: {min_avg_quality})")

        if low_quality_pct > max_low_quality_pct:
            issues.append(
                f"low quality records: {low_quality_pct:.1%} (threshold: {max_low_quality_pct:.0%})"
            )

        if issues:
            self.warnings.append(f"⚠️ {table}: Quality issues - {', '.join(issues)}")
            self.checks_failed += 1
            return {
                "passed": False,
                "table": table,
                "avg_quality": avg_quality,
                "low_quality_pct": low_quality_pct,
                "issues": issues
            }

        logger.info(
            f"✓ {table}: Quality scores OK "
            f"(avg: {avg_quality:.2f}, low: {low_quality_pct:.1%})"
        )
        self.checks_passed += 1
        return {
            "passed": True,
            "table": table,
            "avg_quality": avg_quality,
            "low_quality_pct": low_quality_pct
        }

    def check_spatial_coverage(
        self, table: str, min_postal_codes: int = 10
    ) -> Dict[str, any]:
        """
        Check that we have data for sufficient postal codes.

        Args:
            table: Table name
            min_postal_codes: Minimum number of postal codes expected

        Returns:
            Check result dictionary
        """
        query = f"""
            SELECT
                COUNT(DISTINCT postal_code) as postal_code_count,
                AVG(num_stations_used) as avg_stations_per_postal
            FROM {table}
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """

        result = fetch_one(query)

        if not result or result["postal_code_count"] < min_postal_codes:
            actual = result["postal_code_count"] if result else 0
            self.errors.append(
                f"❌ {table}: Insufficient postal codes ({actual} < {min_postal_codes})"
            )
            self.checks_failed += 1
            return {
                "passed": False,
                "table": table,
                "postal_code_count": actual,
                "threshold": min_postal_codes
            }

        logger.info(
            f"✓ {table}: Spatial coverage OK "
            f"({result['postal_code_count']} postal codes, "
            f"avg {result['avg_stations_per_postal']:.1f} stations/postal)"
        )
        self.checks_passed += 1
        return {
            "passed": True,
            "table": table,
            "postal_code_count": result["postal_code_count"],
            "avg_stations_per_postal": result["avg_stations_per_postal"]
        }

    def check_anomalies(
        self, table: str, lookback_hours: int = 24
    ) -> Dict[str, any]:
        """
        Check for statistical anomalies in temperature and precipitation.

        Uses IQR method to detect extreme values that passed physical constraints
        but may still be anomalous for the dataset.

        Args:
            table: Table name
            lookback_hours: Hours to analyze

        Returns:
            Check result dictionary
        """
        query = f"""
            WITH stats AS (
                SELECT
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY temperature) as temp_q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY temperature) as temp_q3,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY precipitation) as precip_q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY precipitation) as precip_q3
                FROM {table}
                WHERE timestamp >= NOW() - INTERVAL '{lookback_hours} hours'
                  AND temperature IS NOT NULL
                  AND precipitation IS NOT NULL
            ),
            anomalies AS (
                SELECT
                    postal_code,
                    timestamp,
                    temperature,
                    precipitation,
                    CASE
                        WHEN temperature < (stats.temp_q1 - 3 * (stats.temp_q3 - stats.temp_q1))
                          OR temperature > (stats.temp_q3 + 3 * (stats.temp_q3 - stats.temp_q1))
                        THEN true ELSE false
                    END as temp_anomaly,
                    CASE
                        WHEN precipitation > (stats.precip_q3 + 3 * (stats.precip_q3 - stats.precip_q1))
                        THEN true ELSE false
                    END as precip_anomaly
                FROM {table}, stats
                WHERE timestamp >= NOW() - INTERVAL '{lookback_hours} hours'
            )
            SELECT
                COUNT(*) FILTER (WHERE temp_anomaly) as temp_anomaly_count,
                COUNT(*) FILTER (WHERE precip_anomaly) as precip_anomaly_count,
                COUNT(*) as total_count
            FROM anomalies
        """

        result = fetch_one(query)

        if not result or result["total_count"] == 0:
            self.warnings.append(f"⚠️ {table}: No data for anomaly detection")
            return {"passed": True, "table": table, "note": "No data to check"}

        temp_anomaly_pct = result["temp_anomaly_count"] / result["total_count"]
        precip_anomaly_pct = result["precip_anomaly_count"] / result["total_count"]

        # More than 5% anomalies suggests data quality issues
        if temp_anomaly_pct > 0.05 or precip_anomaly_pct > 0.05:
            self.warnings.append(
                f"⚠️ {table}: Statistical anomalies detected - "
                f"temp: {temp_anomaly_pct:.1%}, precip: {precip_anomaly_pct:.1%}"
            )
            return {
                "passed": False,
                "table": table,
                "temp_anomaly_pct": temp_anomaly_pct,
                "precip_anomaly_pct": precip_anomaly_pct
            }

        logger.info(
            f"✓ {table}: No significant anomalies "
            f"(temp: {temp_anomaly_pct:.1%}, precip: {precip_anomaly_pct:.1%})"
        )
        self.checks_passed += 1
        return {
            "passed": True,
            "table": table,
            "temp_anomaly_pct": temp_anomaly_pct,
            "precip_anomaly_pct": precip_anomaly_pct
        }

    def run_all_checks_observations(self) -> Dict[str, any]:
        """
        Run all quality checks for observations table.

        Returns:
            Summary of all checks
        """
        logger.info("🔍 Running data quality checks for observations...")

        table = "weather_observations_by_postal_code"

        results = {
            "freshness": self.check_data_freshness(table, max_age_hours=1),
            "temporal_continuity": self.check_temporal_continuity(table, lookback_hours=24),
            "completeness": self.check_data_completeness(table, min_completeness=0.8),
            "quality_scores": self.check_quality_scores(table),
            "spatial_coverage": self.check_spatial_coverage(table, min_postal_codes=5),
            "anomalies": self.check_anomalies(table, lookback_hours=24),
        }

        return self._summarize_results("observations", results)

    def run_all_checks_forecasts(self) -> Dict[str, any]:
        """
        Run all quality checks for forecasts table.

        Returns:
            Summary of all checks
        """
        logger.info("🔍 Running data quality checks for forecasts...")

        table = "weather_forecasts_by_postal_code"

        # Forecast-specific fields (exclude relative_humidity - not provided by DWD forecasts)
        forecast_core_fields = [
            "temperature",
            "precipitation",
            "pressure_msl",
            "wind_speed"
        ]

        results = {
            "freshness": self.check_data_freshness(table, max_age_hours=2),
            "temporal_continuity": self.check_temporal_continuity(table, lookback_hours=48),
            "completeness": self.check_data_completeness(table, min_completeness=0.8, core_fields=forecast_core_fields),
            "quality_scores": self.check_quality_scores(table),
            "spatial_coverage": self.check_spatial_coverage(table, min_postal_codes=5),
            "anomalies": self.check_anomalies(table, lookback_hours=48),
        }

        return self._summarize_results("forecasts", results)

    def _summarize_results(self, data_type: str, results: Dict) -> Dict[str, any]:
        """Summarize check results."""
        summary = {
            "data_type": data_type,
            "checks_passed": self.checks_passed,
            "checks_failed": self.checks_failed,
            "total_checks": self.checks_passed + self.checks_failed,
            "success_rate": self.checks_passed / max(self.checks_passed + self.checks_failed, 1),
            "warnings": self.warnings,
            "errors": self.errors,
            "detailed_results": results,
            "timestamp": datetime.utcnow().isoformat()
        }

        logger.info(
            f"✓ Data quality checks completed for {data_type}: "
            f"{self.checks_passed}/{self.checks_passed + self.checks_failed} passed "
            f"({summary['success_rate']:.0%})"
        )

        if self.warnings:
            logger.warning(f"Warnings:\n  " + "\n  ".join(self.warnings))

        if self.errors:
            logger.error(f"Errors:\n  " + "\n  ".join(self.errors))

        return summary


def main() -> None:
    """Run all data quality checks."""
    logger.info("Starting data quality checks")

    try:
        checker = DataQualityChecker()

        # Check observations
        obs_results = checker.run_all_checks_observations()

        # Reset counters for forecasts
        checker.checks_passed = 0
        checker.checks_failed = 0
        checker.warnings = []
        checker.errors = []

        # Check forecasts
        fcst_results = checker.run_all_checks_forecasts()

        logger.info("✓ All data quality checks completed")

    except Exception as e:
        logger.error(f"✗ Data quality checks failed: {e}")
        raise


if __name__ == "__main__":
    main()
