"""Unit tests for weather transformation logic."""

import pytest

from src.transform_weather import WeatherTransformer


class TestWeatherTransformer:
    """Test weather transformation functions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = WeatherTransformer()

    def test_validate_temperature_normal(self):
        """Test temperature validation with normal value."""
        is_valid, is_outlier = self.transformer.validate_value("temperature", 20.0)
        assert is_valid is True
        assert is_outlier is False

    def test_validate_temperature_outlier_high(self):
        """Test temperature validation with extreme high value."""
        is_valid, is_outlier = self.transformer.validate_value("temperature", 100.0)
        assert is_valid is True
        assert is_outlier is True

    def test_validate_temperature_outlier_low(self):
        """Test temperature validation with extreme low value."""
        is_valid, is_outlier = self.transformer.validate_value("temperature", -100.0)
        assert is_valid is True
        assert is_outlier is True

    def test_validate_null_value(self):
        """Test validation with null value."""
        is_valid, is_outlier = self.transformer.validate_value("temperature", None)
        assert is_valid is True
        assert is_outlier is False

    def test_calculate_completeness_full(self):
        """Test completeness calculation with all fields."""
        record = {
            "temperature": 20.0,
            "precipitation": 0.0,
            "pressure_msl": 1013.0,
            "relative_humidity": 65.0,
            "wind_speed": 10.0,
            "cloud_cover": 50,
        }
        completeness = self.transformer.calculate_data_completeness(record)
        assert completeness == 1.0

    def test_calculate_completeness_partial(self):
        """Test completeness calculation with some missing fields."""
        record = {
            "temperature": 20.0,
            "precipitation": 0.0,
            "pressure_msl": None,
            "relative_humidity": None,
            "wind_speed": 10.0,
            "cloud_cover": 50,
        }
        completeness = self.transformer.calculate_data_completeness(record)
        assert completeness == 4 / 6  # 4 out of 6 fields

    def test_calculate_completeness_empty(self):
        """Test completeness calculation with no data."""
        record = {}
        completeness = self.transformer.calculate_data_completeness(record)
        assert completeness == 0.0

    def test_quality_score_perfect(self):
        """Test quality score with perfect data."""
        score = self.transformer.calculate_quality_score(
            completeness=1.0, has_outliers=False, num_stations=3
        )
        assert score >= 1.0

    def test_quality_score_with_outliers(self):
        """Test quality score with outliers."""
        score = self.transformer.calculate_quality_score(
            completeness=1.0, has_outliers=True, num_stations=1
        )
        assert score == 0.7  # 30% penalty

    def test_quality_score_incomplete(self):
        """Test quality score with incomplete data."""
        score = self.transformer.calculate_quality_score(
            completeness=0.5, has_outliers=False, num_stations=1
        )
        assert score == 0.5

    def test_feels_like_temperature_normal(self):
        """Test feels-like temperature for normal conditions."""
        feels_like = self.transformer.calculate_feels_like_temperature(
            temp=20.0, humidity=50.0, wind_speed=5.0
        )
        assert feels_like == 20.0  # Should return actual temp

    def test_feels_like_temperature_cold_windy(self):
        """Test feels-like temperature for cold windy conditions."""
        feels_like = self.transformer.calculate_feels_like_temperature(
            temp=5.0, humidity=50.0, wind_speed=30.0
        )
        assert feels_like < 5.0  # Wind chill should make it feel colder

    def test_feels_like_temperature_hot_humid(self):
        """Test feels-like temperature for hot humid conditions."""
        feels_like = self.transformer.calculate_feels_like_temperature(
            temp=30.0, humidity=80.0, wind_speed=5.0
        )
        assert feels_like > 30.0  # Heat index should make it feel hotter

    def test_feels_like_temperature_null(self):
        """Test feels-like temperature with null input."""
        feels_like = self.transformer.calculate_feels_like_temperature(
            temp=None, humidity=50.0, wind_speed=5.0
        )
        assert feels_like is None

    def test_classify_precipitation_none(self):
        """Test precipitation classification for no rain."""
        intensity = self.transformer.classify_precipitation_intensity(0.0)
        assert intensity == "none"

    def test_classify_precipitation_light(self):
        """Test precipitation classification for light rain."""
        intensity = self.transformer.classify_precipitation_intensity(1.0)
        assert intensity == "light"

    def test_classify_precipitation_moderate(self):
        """Test precipitation classification for moderate rain."""
        intensity = self.transformer.classify_precipitation_intensity(5.0)
        assert intensity == "moderate"

    def test_classify_precipitation_heavy(self):
        """Test precipitation classification for heavy rain."""
        intensity = self.transformer.classify_precipitation_intensity(15.0)
        assert intensity == "heavy"

    def test_classify_precipitation_null(self):
        """Test precipitation classification with null."""
        intensity = self.transformer.classify_precipitation_intensity(None)
        assert intensity == "none"

    def test_detect_outliers_clean_record(self):
        """Test outlier detection with clean record."""
        record = {
            "temperature": 20.0,
            "precipitation": 5.0,
            "wind_speed": 30.0,
        }
        has_outliers, outlier_fields = self.transformer.detect_outliers_in_record(record)
        assert has_outliers is False
        assert len(outlier_fields) == 0

    def test_detect_outliers_with_outliers(self):
        """Test outlier detection with outliers present."""
        record = {
            "temperature": 100.0,  # Outlier
            "precipitation": 5.0,
            "wind_speed": 30.0,
        }
        has_outliers, outlier_fields = self.transformer.detect_outliers_in_record(record)
        assert has_outliers is True
        assert "temperature" in outlier_fields


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
