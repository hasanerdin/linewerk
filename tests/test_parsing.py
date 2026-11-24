import re
import pytest
from linewerk.transforms.parsing import ColumnInfo, parse_column

@pytest.mark.parametrize("column_name, expected", [
    ("L3_S49_F4236", ColumnInfo(3, 49, "F", 4236)),
    ("L0_S0_D0", ColumnInfo(0, 0, "D", 0)),
    ("L123_S45_F6789", ColumnInfo(123, 45, "F", 6789)),
])
def test_valid_formats(column_name, expected):
    output = parse_column(column_name)
    assert output == expected

@pytest.mark.parametrize("invalid_column_name",[
    "L3S49F4236",      # Missing Under Score 
    "L3_S49_X4236",    # Not F or D 
    "L3_S49_F",        # No numbers at the end
    "L_S49_F4236",     # No number after L
    "",                # Empty String
])
def test_invalid_formats(invalid_column_name):
    with pytest.raises(ValueError, match=re.escape(invalid_column_name)):
        parse_column(invalid_column_name)