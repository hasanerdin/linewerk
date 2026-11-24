import pytest

from linewerk.jobs.profile_shape import (
    FileShape,
    build_column_lookup,
    column_count,
    file_size_gib,
    format_shape_table,
)
from linewerk.transforms.parsing import ColumnInfo


def test_build_column_lookup_keeps_names_and_skips_non_features():
    columns = ["Id", "L0_S0_F0", "L3_S49_D4236", "Response"]
    assert build_column_lookup(columns) == [
        ("L0_S0_F0", ColumnInfo(0, 0, "F", 0)),
        ("L3_S49_D4236", ColumnInfo(3, 49, "D", 4236)),
    ]


def test_build_column_lookup_does_not_mutate_input():
    columns = ["Id", "L0_S0_F0", "Response"]
    build_column_lookup(columns)
    assert columns == ["Id", "L0_S0_F0", "Response"]


def test_build_column_lookup_fails_fast_on_unknown_column():
    with pytest.raises(ValueError, match="L0_S0_X1"):
        build_column_lookup(["Id", "L0_S0_F0", "L0_S0_X1"])



def test_file_size_gib_uses_bytes_on_disk(tmp_path):
    path = tmp_path / "f.csv"
    path.write_bytes(b"x" * 1024**2)
    assert file_size_gib(path) == 1 / 1024


def test_column_count_reads_header_only(tmp_path):
    path = tmp_path / "f.csv"
    path.write_text("Id,L0_S0_F0,Response\n1,0.5,0\n")
    assert column_count(path) == 3


def test_format_shape_table():
    table = format_shape_table([FileShape("train_numeric.csv", 1.993, 1183747, 970)])
    assert table.splitlines()[-1] == "| train_numeric.csv | 1.99 | 1,183,747 | 970 |"
