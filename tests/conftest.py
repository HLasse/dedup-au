from pathlib import Path
import pytest


@pytest.fixture(scope='module')
def test_data(test_data_path) -> list[str]:
    return test_data_path.open('r').readlines()


@pytest.fixture(scope='module')
def test_data_path() -> Path:
    return Path(__file__).parent / "test_data.txt"