"""Allowed platform and country values for silver data-quality checks.

The sets are the distinct values in the seeded demo. ``SEGMENTS`` is the
source. Ticket text and deploy rows are included so a value that appears only
there is still allowed. A non-blank value outside the set is suspect.
"""

from __future__ import annotations


def canonical_platforms() -> tuple[str, ...]:
    from signal_service.demo.generate import OTHER_TEXTS, SEGMENTS, deploy_rows

    values = {segment[0] for segment in SEGMENTS}
    values.update(item[1] for item in OTHER_TEXTS)
    values.update(row[2] for row in deploy_rows())
    return tuple(sorted(values))


def canonical_countries() -> tuple[str, ...]:
    from signal_service.demo.generate import OTHER_TEXTS, SEGMENTS

    values = {segment[1] for segment in SEGMENTS}
    values.update(item[2] for item in OTHER_TEXTS)
    return tuple(sorted(values))


def segment_dq_reason(platform: object, country: object) -> str | None:
    """Return ``invalid_platform`` or ``invalid_country`` for a value outside the set.

    Blank and null values are not this rule. The load already calls those
    ``null segment``. Platform is checked first when both are outside the set.
    """
    if platform is None or str(platform).strip() == "":
        return None
    if country is None or str(country).strip() == "":
        return None
    if str(platform) not in canonical_platforms():
        return "invalid_platform"
    if str(country) not in canonical_countries():
        return "invalid_country"
    return None


def sql_in_list(values: tuple[str, ...]) -> str:
    quoted = []
    for value in values:
        if not value or "'" in value:
            raise ValueError("allowed segment values must be non-empty and quote-free")
        quoted.append("'" + value + "'")
    return ", ".join(quoted)
