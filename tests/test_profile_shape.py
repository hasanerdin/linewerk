from linewerk.jobs.profile_shape import (
    FileShape,
    column_count,
    file_size_gib,
    format_shape_table,
)


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
