import json
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from validate_policies import POLICY_DIR, PolicyError, validate


def copied_policies(tmp_path: Path) -> Path:
    target = tmp_path / "policies"
    shutil.copytree(POLICY_DIR, target)
    return target


def mutate(policy_dir: Path, filename: str, change) -> None:
    path = policy_dir / filename
    data = json.loads(path.read_text(encoding="utf-8"))
    change(data)
    path.write_text(json.dumps(data), encoding="utf-8")


def test_complete_policies_are_valid() -> None:
    policies = validate()
    assert policies["release_contract"]["taxonomy_schema_version"] == 2


def test_duplicate_language_alias_is_rejected(tmp_path: Path) -> None:
    policy_dir = copied_policies(tmp_path)
    mutate(policy_dir, "languages.yml", lambda data: data["languages"][1]["aliases"].append("nob"))
    with pytest.raises(PolicyError, match="maps to multiple"):
        validate(policy_dir)


def test_unknown_source_reference_is_rejected(tmp_path: Path) -> None:
    policy_dir = copied_policies(tmp_path)
    mutate(policy_dir, "scope.yml", lambda data: data["rules"][0].update(source="unknown"))
    with pytest.raises(PolicyError, match="unknown source reference"):
        validate(policy_dir)


def test_invalid_mapping_relationship_is_rejected(tmp_path: Path) -> None:
    policy_dir = copied_policies(tmp_path)
    mutate(policy_dir, "mapping_policy.yml", lambda data: data["relationships"].append("same_name"))
    with pytest.raises(PolicyError, match="relationship vocabulary"):
        validate(policy_dir)


def test_malformed_release_id_is_rejected(tmp_path: Path) -> None:
    policy_dir = copied_policies(tmp_path)
    mutate(policy_dir, "release_contract.yml", lambda data: data["valid_release_examples"].append("tax-latest"))
    with pytest.raises(PolicyError, match="valid release example"):
        validate(policy_dir)


def test_missing_required_scope_rule_is_rejected(tmp_path: Path) -> None:
    policy_dir = copied_policies(tmp_path)
    mutate(
        policy_dir,
        "scope.yml",
        lambda data: data.update(rules=[rule for rule in data["rules"] if rule["code"] != "global_fungi"]),
    )
    with pytest.raises(PolicyError, match="global_fungi"):
        validate(policy_dir)


def test_invalid_manual_mapping_is_rejected(tmp_path: Path) -> None:
    policy_dir = copied_policies(tmp_path)
    def add_invalid(data):
        data["mappings"].append({
            "mapping_id": "fixture-1",
            "source_usage": {"source": "nortaxa", "namespace": "nortaxa_dwc_id", "identifier": "1"},
            "target": {"sporely_taxon_id": 1},
            "relationship": "same_name"
        })
    mutate(policy_dir, "manual_mappings.yml", add_invalid)
    with pytest.raises(PolicyError, match="missing fields"):
        validate(policy_dir)
