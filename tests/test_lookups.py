import afw.dataset.cached

import magic

## xsecdb
# Check one file
def test_xsecdb_lookup():
    process = magic.single_xsecdb_process_name
    result, res_code = afw.dataset.cached.do_request(process)

    assert res_code == 200

    # Magic Values
    assert len(result) == 1
    result = result[0]

    assert result["_id"]["$oid"] == magic.single_xsecdb_oid
    assert result["cross_section"] == str(magic.single_xsecdb_xsec)


# Check one cross section
def test_get_cross_section_one():
    assert (
        afw.dataset.cached.get_cross_section(
            magic.single_xsecdb_das_key
        )
        == magic.single_xsecdb_xsec
    )


# Check with multiple xsecdb results for one DID
def test_get_cross_section_multiple():
    assert (
        afw.dataset.cached.get_cross_section(
            magic.multiple_xsecdb_das_key
        )
        == magic.multiple_xsecdb_xsec
    )

## Rucio
def test_get_all_matching():
    # Use data as it never changes
    result = afw.dataset.cached.get_all_matching(magic.data_das_key_template)
    assert result == [magic.data_das_key]

## dasgoclient
def test_dasgoclient():
    result = afw.dataset.cached.run_dasgoclient(f"file dataset={magic.data_das_key}")
    assert isinstance(result, list)