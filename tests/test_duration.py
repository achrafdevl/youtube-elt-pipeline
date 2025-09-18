# tests/test_duration.py
from src.youtube_client import iso_duration_to_seconds

def test_iso_duration_parser():
    assert iso_duration_to_seconds("PT1H2M30S") == 3750
    assert iso_duration_to_seconds("PT45S") == 45
    assert iso_duration_to_seconds("PT0S") == 0
