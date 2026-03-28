from src.features.corridor_builder import corridor_definitions_frame


def test_corridor_definitions_frame_matches_locked_corridors():
    frame = corridor_definitions_frame()
    assert len(frame) == 5
    assert frame["corridor_name"].tolist()[0] == "LA_to_Chicago"
    assert set(frame.columns) == {"corridor_id", "corridor_name", "origin", "destination"}
