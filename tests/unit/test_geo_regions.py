from teelo.utils.geo import ioc_to_region, REGION_MEMBERS, IOC_TO_REGION


def test_ioc_to_region_known_codes() -> None:
    assert ioc_to_region("GBR") == "Europe"
    assert ioc_to_region("USA") == "Americas"
    assert ioc_to_region("AUS") == "Asia-Pacific"
    assert ioc_to_region("UAE") == "Middle East & Africa"


def test_ioc_to_region_unknown() -> None:
    assert ioc_to_region("XYZ") is None


def test_no_duplicate_ioc_across_regions() -> None:
    all_codes: list[str] = []
    for members in REGION_MEMBERS.values():
        all_codes.extend(members)
    assert len(all_codes) == len(set(all_codes)), "Duplicate IOC code across regions"


def test_region_members_covers_major_tours() -> None:
    for ioc in ["AUS", "FRA", "GBR", "USA"]:
        assert ioc_to_region(ioc) is not None, f"{ioc} missing from region mapping"
