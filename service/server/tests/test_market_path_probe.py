from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.market_path_probe import eligible


def test_local_candidate_requires_fresh_frames_and_zero_protocol_errors():
    good = {'connected': True, 'frames': 30,
            'exchange_event_age': {'p95_ms': 20}, 'protocol_errors': 0}
    assert eligible(good, 30, 250)
    assert not eligible(good | {'connected': False}, 30, 250)
    assert not eligible(good | {'frames': 29}, 30, 250)
    assert not eligible(good | {'exchange_event_age': {'p95_ms': 251}}, 30, 250)
    assert not eligible(good | {'protocol_errors': 1}, 30, 250)
