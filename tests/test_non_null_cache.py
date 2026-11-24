import os

from linewerk.jobs.non_null_profile import source_fingerprint, stale_reason


def _csv(tmp_path, content="Id,L0_S0_F0\n1,0.1\n"):
    path = tmp_path / "train_numeric.csv"
    path.write_text(content)
    return path


def test_fresh_cache_is_used(tmp_path):
    csv = _csv(tmp_path)
    assert stale_reason({"source": source_fingerprint(csv)}, csv) is None


def test_size_change_is_stale(tmp_path):
    csv = _csv(tmp_path)
    cached = {"source": source_fingerprint(csv)}
    csv.write_text("Id,L0_S0_F0\n1,0.1\n2,0.2\n")
    assert "source changed" in stale_reason(cached, csv)


def test_same_size_new_mtime_is_stale(tmp_path):
    """Re-download with identical size: only the mtime gives it away."""
    csv = _csv(tmp_path)
    cached = {"source": source_fingerprint(csv)}
    st = csv.stat()
    os.utime(csv, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
    assert "source changed" in stale_reason(cached, csv)


def test_cache_without_fingerprint_is_stale(tmp_path):
    csv = _csv(tmp_path)
    assert "old cache format" in stale_reason({"total_rows": 1}, csv)
