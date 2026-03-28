from src.utils.config import CONFIG


def test_config_paths_are_resolved():
    assert CONFIG.data_dir.name == "Data"
    assert CONFIG.notebooks_dir.name == "Notebooks"
    assert CONFIG.duckdb_path.name == "freightpulse.duckdb"
