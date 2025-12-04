"""
Tests for the data-fetch framework.
Unit tests for robots logic, inspectors, extractors, and scrapers.
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

import pandas as pd

# Add parent directory to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestRobotsModule:
    """Tests for robots.txt parsing and compliance."""
    
    def test_robots_parser_allows_all(self):
        """Test that empty robots.txt allows all."""
        from src.utils.robots import RobotsParser
        
        parser = RobotsParser("", "*")
        assert parser.is_allowed("/any/path") is True
    
    def test_robots_parser_disallows_path(self):
        """Test that Disallow directive blocks paths."""
        from src.utils.robots import RobotsParser
        
        robots_txt = """
User-agent: *
Disallow: /private/
"""
        parser = RobotsParser(robots_txt, "*")
        
        assert parser.is_allowed("/public/page") is True
        assert parser.is_allowed("/private/secret") is False
    
    def test_robots_parser_allow_overrides_disallow(self):
        """Test that Allow directive overrides Disallow."""
        from src.utils.robots import RobotsParser
        
        robots_txt = """
User-agent: *
Disallow: /api/
Allow: /api/public/
"""
        parser = RobotsParser(robots_txt, "*")
        
        assert parser.is_allowed("/api/private") is False
        assert parser.is_allowed("/api/public/data") is True
    
    def test_robots_decision_status(self):
        """Test RobotsDecision status properties."""
        from src.utils.robots import RobotsDecision, RobotsStatus
        
        allowed = RobotsDecision(status=RobotsStatus.ALLOWED, reason="test")
        assert allowed.is_allowed is True
        assert allowed.is_disallowed is False
        assert allowed.is_unknown is False
        
        disallowed = RobotsDecision(status=RobotsStatus.DISALLOWED, reason="test")
        assert disallowed.is_allowed is False
        assert disallowed.is_disallowed is True
        
        unknown = RobotsDecision(status=RobotsStatus.UNKNOWN, reason="test")
        assert unknown.is_unknown is True
    
    @patch('src.utils.robots.requests.get')
    def test_check_robots_permission_success(self, mock_get):
        """Test successful robots.txt check."""
        from src.utils.robots import check_robots_permission, RobotsStatus
        
        mock_get.return_value = Mock(
            status_code=200,
            text="User-agent: *\nAllow: /",
        )
        
        result = check_robots_permission("https://example.com/data")
        
        assert result.status == RobotsStatus.ALLOWED
        assert mock_get.called
    
    @patch('src.utils.robots.requests.get')
    def test_check_robots_permission_not_found(self, mock_get):
        """Test robots.txt 404 (allows all)."""
        from src.utils.robots import check_robots_permission, RobotsStatus
        
        mock_get.return_value = Mock(status_code=404)
        
        result = check_robots_permission("https://example.com/data")
        
        assert result.status == RobotsStatus.ALLOWED


class TestNetworkInspector:
    """Tests for network request inspection."""
    
    def test_analyze_requests_filters_non_data(self):
        """Test that non-data requests are filtered out."""
        from src.detector.network_inspector import NetworkInspector
        from src.utils.browser import NetworkRequest
        
        inspector = NetworkInspector()
        
        requests = [
            NetworkRequest(
                url="https://example.com/api/data",
                method="GET",
                resource_type="xhr",
                status=200,
                content_type="application/json",
            ),
            NetworkRequest(
                url="https://example.com/tracking.js",
                method="GET",
                resource_type="script",
                status=200,
                content_type="application/javascript",
            ),
        ]
        
        candidates = inspector.analyze_requests(requests)
        
        # Should only find the JSON endpoint
        assert len(candidates) == 1
        assert "api/data" in candidates[0].url
    
    def test_calculate_score_data_keywords(self):
        """Test that data keywords increase score."""
        from src.detector.network_inspector import NetworkInspector
        from src.utils.browser import NetworkRequest
        
        inspector = NetworkInspector()
        
        data_request = NetworkRequest(
            url="https://example.com/api/charts/data",
            method="GET",
            resource_type="xhr",
            status=200,
            content_type="application/json",
        )
        
        plain_request = NetworkRequest(
            url="https://example.com/page",
            method="GET",
            resource_type="xhr",
            status=200,
            content_type="application/json",
        )
        
        score_data = inspector._calculate_score(data_request, None)
        score_plain = inspector._calculate_score(plain_request, None)
        
        # Data keywords should boost score
        assert score_data > score_plain


class TestTableExtractor:
    """Tests for HTML table extraction."""
    
    def test_extract_simple_table(self):
        """Test extraction of a simple HTML table."""
        from src.extractor.table_extractor import TableExtractor
        
        html = """
        <html>
        <body>
            <table>
                <tr><th>Date</th><th>Value</th></tr>
                <tr><td>2024-01-01</td><td>100</td></tr>
                <tr><td>2024-01-02</td><td>200</td></tr>
            </table>
        </body>
        </html>
        """
        
        extractor = TableExtractor()
        df = extractor.extract_table(html)
        
        assert len(df) == 2
        assert "Date" in df.columns
        assert "Value" in df.columns
    
    def test_find_tables(self):
        """Test finding multiple tables."""
        from src.extractor.table_extractor import TableExtractor
        
        html = """
        <html>
        <body>
            <table id="table1"><tr><th>A</th></tr><tr><td>1</td></tr></table>
            <table id="table2"><tr><th>B</th></tr><tr><td>2</td></tr></table>
        </body>
        </html>
        """
        
        extractor = TableExtractor()
        tables = extractor.find_tables(html)
        
        assert len(tables) == 2


class TestJsonExtractor:
    """Tests for JSON data extraction."""
    
    def test_extract_array_of_objects(self):
        """Test extraction of array of objects."""
        from src.extractor.json_extractor import JsonExtractor
        
        data = [
            {"date": "2024-01-01", "value": 100},
            {"date": "2024-01-02", "value": 200},
        ]
        
        extractor = JsonExtractor()
        df = extractor.extract(data)
        
        assert len(df) == 2
        assert "date" in df.columns
        assert "value" in df.columns
    
    def test_extract_nested_data(self):
        """Test extraction with data_path."""
        from src.extractor.json_extractor import JsonExtractor
        
        data = {
            "status": "ok",
            "data": {
                "series": [
                    {"date": "2024-01-01", "value": 100},
                    {"date": "2024-01-02", "value": 200},
                ]
            }
        }
        
        extractor = JsonExtractor()
        df = extractor.extract(data, data_path="data.series")
        
        assert len(df) == 2
    
    def test_detect_structure(self):
        """Test structure detection."""
        from src.extractor.json_extractor import JsonExtractor
        
        data = [
            {"date": "2024-01-01", "value": 100},
            {"date": "2024-01-02", "value": 200},
        ]
        
        extractor = JsonExtractor()
        structure = extractor.detect_structure(data)
        
        assert structure["type"] == "list"
        assert structure["is_timeseries"] is True
        assert structure["row_count"] == 2
        assert "date" in structure["field_names"]


class TestValidators:
    """Tests for data validation."""
    
    def test_validate_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        from src.pipeline.validators import DataValidator
        
        validator = DataValidator()
        df = pd.DataFrame()
        
        result = validator.validate(df)
        
        assert result.is_valid is False
        assert "empty" in result.errors[0].lower()
    
    def test_validate_duplicates(self):
        """Test detection of duplicate rows."""
        from src.pipeline.validators import DataValidator
        
        validator = DataValidator()
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-01", "2024-01-02"],
            "value": [100, 100, 200],
        })
        
        result = validator.validate(df)
        
        assert any("duplicate" in w.lower() for w in result.warnings)
    
    def test_validate_null_values(self):
        """Test detection of null values."""
        from src.pipeline.validators import DataValidator
        
        validator = DataValidator()
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [100, None, 200],
        })
        
        result = validator.validate(df)
        
        assert any("null" in w.lower() or "nan" in w.lower() for w in result.warnings)


class TestSchema:
    """Tests for data schema utilities."""
    
    def test_to_long_format(self):
        """Test conversion to long format."""
        from src.pipeline.schema import to_long_format
        
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "btc_volume": [100, 200],
            "eth_volume": [50, 75],
        })
        
        long_df = to_long_format(df, date_column="date")
        
        assert len(long_df) == 4  # 2 dates × 2 assets
        assert "asset" in long_df.columns
        assert "metric" in long_df.columns
        assert "value" in long_df.columns
    
    def test_normalize_dataframe(self):
        """Test DataFrame normalization."""
        from src.pipeline.schema import normalize_dataframe
        
        df = pd.DataFrame({
            "date": ["2024-01-02", "2024-01-01"],
            "value": ["100", "200"],
        })
        
        normalized = normalize_dataframe(df, numeric_columns=["value"])
        
        # Should be sorted by date
        assert normalized["date"].iloc[0] < normalized["date"].iloc[1]
        # Values should be numeric
        assert pd.api.types.is_numeric_dtype(normalized["value"])


class TestConfigManager:
    """Tests for configuration management."""
    
    def test_validate_config(self):
        """Test configuration validation."""
        from src.utils.config_manager import ConfigManager, SiteConfig, DataSource
        
        manager = ConfigManager()
        
        # Valid config
        valid_config = SiteConfig(
            id="test_site",
            name="Test Site",
            base_url="https://example.com",
            page_url="https://example.com/data",
            extraction_strategy="api_json",
            data_source=DataSource(type="api", endpoint="https://example.com/api"),
            field_mappings={},
        )
        
        errors = manager.validate_config(valid_config)
        assert len(errors) == 0
        
        # Invalid config (missing URL)
        invalid_config = SiteConfig(
            id="test_site",
            name="Test Site",
            base_url="",
            page_url="",
            extraction_strategy="api_json",
            data_source=DataSource(type="api"),
            field_mappings={},
        )
        
        errors = manager.validate_config(invalid_config)
        assert len(errors) > 0


class TestExcelExporter:
    """Tests for Excel export."""
    
    def test_export_creates_file(self, tmp_path):
        """Test that export creates an Excel file."""
        from src.exporter.excel_exporter import ExcelExporter
        
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "value": [100, 200],
        })
        
        exporter = ExcelExporter(output_dir=tmp_path)
        output_path = exporter.export(df, filename="test.xlsx")
        
        assert output_path.exists()
        assert output_path.suffix == ".xlsx"
    
    def test_export_with_metadata(self, tmp_path):
        """Test export includes metadata sheet."""
        from src.exporter.excel_exporter import ExcelExporter
        
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "value": [100, 200],
        })
        
        exporter = ExcelExporter(output_dir=tmp_path, include_metadata=True)
        output_path = exporter.export(
            df,
            filename="test_meta.xlsx",
            metadata={"custom_key": "custom_value"},
        )
        
        # Read back and check metadata sheet exists
        xl = pd.ExcelFile(output_path)
        assert "Metadata" in xl.sheet_names


class TestScraperResult:
    """Tests for scraper result handling."""
    
    def test_scraper_result_to_dict(self):
        """Test ScraperResult serialization."""
        from src.scraper.base_scraper import ScraperResult
        
        result = ScraperResult(
            success=True,
            source="test",
            url="https://example.com",
            run_id="test_123",
            rows_extracted=100,
        )
        
        d = result.to_dict()
        
        assert d["success"] is True
        assert d["source"] == "test"
        assert d["rows_extracted"] == 100


# Integration tests (marked for separate execution)
class TestIntegration:
    """Integration tests for full workflows."""
    
    @pytest.mark.integration
    def test_coingecko_scraper(self):
        """Test CoinGecko API scraper (requires network)."""
        from src.scraper.fallback_scrapers import CoinGeckoScraper
        
        scraper = CoinGeckoScraper()
        result = scraper.scrape(override_robots=True)
        
        if result.success:
            assert result.data is not None
            assert len(result.data) > 0
    
    @pytest.mark.integration
    def test_pipeline_runner_with_url(self):
        """Test pipeline runner with a URL (requires network)."""
        from src.pipeline.pipeline_runner import PipelineRunner
        
        runner = PipelineRunner()
        result = runner.run(
            url="https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=7",
            override_robots=True,
            export=False,
        )
        
        if result.success:
            assert result.data is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "not integration"])

