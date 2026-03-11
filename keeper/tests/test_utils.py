from typing import List

import pytest

from keeper.exceptions import ValidationError
from keeper.utils import (
    auto_slugify_edition,
    validate_path_slug,
    validate_product_slug,
)


@pytest.mark.parametrize(
    "git_refs,expected",
    [
        (["tickets/DM-1234"], "DM-1234"),
        (["tickets/LCR-758"], "LCR-758"),
        (["main"], "main"),
        (["u/rowen/r12_patch1"], "u-rowen-r12_patch1"),
        (
            ["tickets/DM-1234", "tickets/DM-5678"],
            "tickets-DM-1234-tickets-DM-5678",
        ),
        (["v15_0"], "v15_0"),
        (["w_2018_01"], "w_2018_01"),
        (["1.0.0"], "1.0.0"),
    ],
)
def test_auto_slugify_edition(git_refs: List[str], expected: str) -> None:
    assert expected == auto_slugify_edition(git_refs)
    assert validate_path_slug(auto_slugify_edition(git_refs))


def test_validate_product_slug() -> None:
    with pytest.raises(ValidationError):
        validate_product_slug("DM-1234")
    with pytest.raises(ValidationError):
        validate_product_slug("DM_1234")
    assert validate_product_slug("dm-1234") is True
