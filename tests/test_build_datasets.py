import afw.dataset.definitions

import magic


# Overall Check
def test_overall_fills_xsec():
    template = [
        {
            "datasets": [magic.single_xsecdb_das_key],
        }
    ]

    result = afw.dataset.definitions.build_datasets(template)
    assert (
        result[magic.single_xsecdb_das_key]["metadata"]["xsec"]
        == magic.single_xsecdb_xsec
    )


def test_overall_respects_xsec_override():
    template = [
        {
            "metadata": {"xsec": 123},
            "datasets": [magic.single_xsecdb_das_key],
        }
    ]

    result = afw.dataset.definitions.build_datasets(template)
    assert result[magic.single_xsecdb_das_key]["metadata"]["xsec"] == 123


def test_overall_skips_data_xsec():
    template = [
        {
            "metadata": {"isData": True},
            "datasets": [magic.data_das_key],
        }
    ]
    result = afw.dataset.definitions.build_datasets(template)
    assert "xsec" not in result[magic.data_das_key]["metadata"]


def test_overall_can_have_multiple_datasets_with_different_xsecs():
    template = [
        {
            "datasets": [magic.single_xsecdb_das_key, magic.multiple_xsecdb_das_key],
        }
    ]

    result = afw.dataset.definitions.build_datasets(template)
    assert (
        result[magic.single_xsecdb_das_key]["metadata"]["xsec"]
        == magic.single_xsecdb_xsec
    )
    assert (
        result[magic.multiple_xsecdb_das_key]["metadata"]["xsec"]
        == magic.multiple_xsecdb_xsec
    )


# Check template building
def test_build_templates_adds_metadata_if_missing():
    template = [{"datasets": ["das-key-a"]}]
    rendered = afw.dataset.definitions.build_templates(template)
    assert "metadata" in rendered["das-key-a"]


def test_build_templates_copies_metadata_to_das_keys():
    template = [
        {"metadata": {"test-key": "test-value"}, "datasets": ["das-key-a", "das-key-b"]}
    ]
    rendered = afw.dataset.definitions.build_templates(template)
    print(f"Returned rendered values {rendered}")

    # Check metadata is copied
    assert rendered["das-key-a"]["metadata"]["test-key"] == "test-value"
    assert rendered["das-key-b"]["metadata"]["test-key"] == "test-value"


def test_build_templates_copies_metadata_objects():
    template = [
        {"metadata": {"test-key": "test-value"}, "datasets": ["das-key-a", "das-key-b"]}
    ]
    rendered = afw.dataset.definitions.build_templates(template)
    print(f"Returned rendered values {rendered}")

    # Check new keys are not shared between DAS keys
    rendered["das-key-b"]["metadata"]["test-key-2"] = "test-value-2"
    assert "test-key-2" not in rendered["das-key-a"]["metadata"]

    # Check modifying metadata only applies to a specific das key
    rendered["das-key-a"]["metadata"]["test-key"] = "new-test-value"
    assert rendered["das-key-b"]["metadata"]["test-key"] == "test-value"


# Check expand templates
def test_expand_templates():
    template = {magic.data_das_key_template: {"metadata": {"test-key": "test-value"}}}

    expanded_template = afw.dataset.definitions.expand_templates(template)
    print(f"Returned expanded template {expanded_template}")

    assert magic.data_das_key in expanded_template
    assert expanded_template[magic.data_das_key]["metadata"]["test-key"] == "test-value"


# Check the removal of obsolete versions
def test_remove_obsolete_versions():
    dataset = afw.dataset.definitions.remove_obsolete_versions(
        {
            "test-v1/NANOAOD": {"files": []},
            "test-v2/MINIAOD": {"files": ["testfile-1", "testfile-2"]},
            "test-v3/MINIAOD": {"files": ["testfile-3", "testfile-4", ["testfile-5"]]},
        }
    )

    print(f"Returned {dataset}")

    assert "test-v1/NANOAOD" not in dataset
    assert "test-v2/MINIAOD" not in dataset
    assert len(dataset["test-v3/MINIAOD"]["files"]) == 3


# Test file population
def test_populate_files():
    result = afw.dataset.definitions.populate_files(magic.data_das_key)
    assert "nevents" in result["metadata"]
    assert "nevents_total" in result["metadata"]
    assert result["metadata"]["nevents"] == result["metadata"]["nevents_total"]
    assert len(result["files"]) > 0


def test_populate_files_pass_metadata():
    result = afw.dataset.definitions.populate_files(
        magic.data_das_key,
        {"metadata": {"test-key": "test-value"}},
    )
    assert "test-key" in result["metadata"]
    assert result["metadata"]["test-key"] == "test-value"


def test_populate_files_nevents_limit():
    result = afw.dataset.definitions.populate_files(magic.data_das_key, {}, 1)
    assert "nevents" in result["metadata"]
    assert "nevents_total" in result["metadata"]
    assert result["metadata"]["nevents"] < result["metadata"]["nevents_total"]
