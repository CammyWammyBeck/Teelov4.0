"""Unit tests for parse_tours in scripts/backfill_historical.py."""

import ast
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "backfill_historical.py"


def _load_parse_tours():
    """Load parse_tours and TOUR_ORDER directly from source without importing heavy deps."""
    source = SCRIPT_PATH.read_text()
    module_ast = ast.parse(source)

    selected_nodes = [
        node
        for node in module_ast.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "TOUR_ORDER" for target in node.targets)
        or isinstance(node, ast.FunctionDef)
        and node.name == "parse_tours"
    ]

    test_module = ast.Module(body=selected_nodes, type_ignores=[])
    compiled = compile(test_module, str(SCRIPT_PATH), "exec")
    namespace = {
        "TOUR_TYPES": {
            "ATP": {},
            "CHALLENGER": {},
            "WTA": {},
            "WTA_125": {},
            "ITF_MEN": {},
            "ITF_WOMEN": {},
        },
        "Optional": __import__("typing").Optional,
    }
    exec(compiled, namespace)

    return namespace["parse_tours"]


def test_parse_tours_filters_one_invalid_tour(capsys):
    """Invalid single tour should be filtered and warning emitted."""
    parse_tours = _load_parse_tours()
    result = parse_tours("ATP,INVALID")

    assert result == ["ATP"]
    captured = capsys.readouterr()
    assert "Unknown tour type 'INVALID'" in captured.out


def test_parse_tours_filters_two_consecutive_invalid_tours(capsys):
    """Consecutive invalid tours should both be filtered with warnings."""
    parse_tours = _load_parse_tours()
    result = parse_tours("ATP,INVALID_ONE,INVALID_TWO,WTA")

    assert result == ["ATP", "WTA"]
    captured = capsys.readouterr()
    assert "Unknown tour type 'INVALID_ONE'" in captured.out
    assert "Unknown tour type 'INVALID_TWO'" in captured.out


def test_parse_tours_mixed_valid_invalid_order_is_deterministic(capsys):
    """Output should follow TOUR_ORDER and never include invalid tours."""
    parse_tours = _load_parse_tours()
    result = parse_tours("WTA_125,BOGUS,ATP,NOPE,CHALLENGER")

    assert result == ["ATP", "CHALLENGER", "WTA_125"]
    assert "BOGUS" not in result
    assert "NOPE" not in result
    captured = capsys.readouterr()
    assert "Unknown tour type 'BOGUS'" in captured.out
    assert "Unknown tour type 'NOPE'" in captured.out
