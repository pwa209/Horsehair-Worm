import hashlib
import io
import json
import stat
import tarfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from pathlib import Path

import h5py
import pytest
from scipy.io import savemat

from horsehair.acquire import (
    AcquisitionError, asset, canonical_hash, download_asset, ena_fastq_assets,
    inventory_file, safe_destination, safe_extract, validate_manifest, verify_file,
)
from horsehair.acquire import discover_fly_reference
from horsehair.acquire import download_manifest


def object_for(body=b"valid scientific bytes", **kwargs):
    item = asset(source="fixture", name="input.bin", url="https://example.org/input.bin", version="v1", rights="CC0", size=len(body), checksum="md5:" + hashlib.md5(body).hexdigest())
    item.update(kwargs)
    return item


def test_manifest_modification_is_detected():
    manifest = {"schema_version": 1, "assets": [object_for()]}
    manifest["manifest_hash"] = canonical_hash(manifest)
    validate_manifest(manifest)
    manifest["assets"][0]["bytes"] += 1
    with pytest.raises(AcquisitionError, match="hash"):
        validate_manifest(manifest)


@pytest.mark.parametrize("path", ["../escaped", "/absolute", "C:/Windows/file", "safe\\..\\escaped", "name:stream"])
def test_destination_rejects_escape(tmp_path, path):
    with pytest.raises(AcquisitionError):
        safe_destination(tmp_path, path)


def test_checksum_mismatch_and_html_failure(tmp_path):
    path = tmp_path / "input.bin"
    body = b"source data"
    path.write_bytes(body)
    assert verify_file(path, object_for(body))["sha256"] == hashlib.sha256(body).hexdigest()
    with pytest.raises(AcquisitionError, match="checksum"):
        verify_file(path, object_for(body, provider_checksum="md5:" + "0"*32))
    html = b"<!DOCTYPE html><html>Repository access error</html>"
    path.write_bytes(html)
    with pytest.raises(AcquisitionError, match="HTML"):
        verify_file(path, object_for(html))


class Response:
    def __init__(self, body, status=200, headers=None):
        self.body, self.status_code, self.headers = body, status, headers or {}
        self.url = "https://example.org/input.bin"

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield self.body


class Session:
    def __init__(self, response):
        self.response, self.calls = response, []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def test_fly_reference_matches_one_release_and_published_protein_digest():
    class MetadataSession:
        def get(self, url, **kwargs):
            response = Response(b"metadata")
            response.text = "1844f571c897e51663c2eb4e144330e9  dmel-all-translation-r6.69.fasta.gz\n"
            return response
        def head(self, url, **kwargs):
            return Response(b"", headers={"Content-Length": "1000", "ETag": "fixed-provider-version"})
    metadata, assets = discover_fly_reference(MetadataSession())
    assert len(assets) == 5
    assert metadata["release"] == "FB2026_03"
    assert all(item["version"] == "FB2026_03:dmel-r6.69" for item in assets)
    assert set(metadata["file_roles"]) == {"protein_fasta", "gene_protein_map", "gene_catalog", "go_annotations", "go_ontology"}
    assert assets[0]["provider_checksum"] == "md5:1844f571c897e51663c2eb4e144330e9"


def test_concurrent_groups_preserve_independent_verified_download_receipts(tmp_path, monkeypatch):
    import horsehair.acquire as acquire
    bodies = {"https://example.org/a.bin": b"source-a", "https://example.org/b.bin": b"source-b"}
    assets = [asset(source=group, name=group+".bin", url=url, version="synthetic", rights="synthetic test fixture", size=len(body), checksum="md5:"+hashlib.md5(body).hexdigest()) for group, (url, body) in zip(["a", "b"], bodies.items())]
    manifest = {"schema_version": 1, "assets": assets}
    manifest["manifest_hash"] = canonical_hash(manifest)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    barrier = Barrier(2)
    class ConcurrentSession:
        def get(self, url, **kwargs):
            barrier.wait(timeout=10)
            return Response(bodies[url])
    monkeypatch.setattr(acquire, "http_session", lambda: ConcurrentSession())
    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(lambda source: download_manifest(manifest_path, tmp_path, sources=[source], bandwidth_bytes_per_second=None), ["a", "b"]))
    assert results[0]["receipt_path"] != results[1]["receipt_path"]
    for result in results:
        stored = json.loads(Path(result["receipt_path"]).read_text())
        assert stored["sources"] == result["sources"]
        assert len(stored["files"]) == 1
        assert stored["files"][0]["status"] == "verified"
        assert Path(stored["files"][0]["path"]).read_bytes() == bodies[assets[["a", "b"].index(result["sources"][0])]["url"]]


def test_resumed_download_requires_matching_provenance_and_range(tmp_path):
    body = b"0123456789"
    item = object_for(body)
    target = safe_destination(tmp_path, item["relative_path"])
    target.parent.mkdir(parents=True)
    partial = target.with_name(target.name + ".part")
    partial.write_bytes(body[:4])
    partial.with_name(partial.name + ".json").write_text(json.dumps({"asset_hash": canonical_hash(item), "etag": "stable"}))
    session = Session(Response(body[4:], 206, {"Content-Range": "bytes 4-9/10"}))
    receipt = download_asset(item, tmp_path, session=session)
    assert target.read_bytes() == body
    assert not partial.exists()
    assert session.calls[0][1]["headers"]["Range"] == "bytes=4-"
    assert receipt["status"] == "verified"


def test_server_ignoring_range_restarts_atomically(tmp_path):
    body = b"0123456789"
    item = object_for(body)
    target = safe_destination(tmp_path, item["relative_path"])
    target.parent.mkdir(parents=True)
    partial = target.with_name(target.name + ".part")
    partial.write_bytes(body[:4])
    partial.with_name(partial.name + ".json").write_text(json.dumps({"asset_hash": canonical_hash(item)}))
    download_asset(item, tmp_path, session=Session(Response(body)))
    assert target.read_bytes() == body


def test_fastq_download_is_gated_before_network(tmp_path):
    with pytest.raises(AcquisitionError, match="crosswalk"):
        download_asset(object_for(kind="fastq", eligible=False), tmp_path)


def test_unsafe_zip_and_tar_are_rejected_before_extraction(tmp_path):
    archive = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("good.txt", "safe")
        handle.writestr("../escape.txt", "unsafe")
    destination = tmp_path / "output"
    with pytest.raises(AcquisitionError):
        safe_extract(archive, destination)
    assert not (destination / "good.txt").exists()
    linked = tmp_path / "bad.tar"
    with tarfile.open(linked, "w") as handle:
        link = tarfile.TarInfo("link")
        link.type, link.linkname = tarfile.SYMTYPE, "/external"
        handle.addfile(link)
    with pytest.raises(AcquisitionError, match="type"):
        safe_extract(linked, destination)


def test_zip_size_limit_and_mat_selective_extraction(tmp_path):
    archive = tmp_path / "data.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("movie.avi", b"m"*100)
        handle.writestr("nested/data.mat", b"data")
    with pytest.raises(AcquisitionError, match="ceiling"):
        safe_extract(archive, tmp_path / "all", max_unpacked_bytes=20)
    files = safe_extract(archive, tmp_path / "select", max_unpacked_bytes=20, include_pattern=r"\.mat$")
    assert len(files) == 1
    assert not (tmp_path / "select" / "movie.avi").exists()


def test_mat_inventory_supports_conventional_and_hdf5(tmp_path):
    conventional = tmp_path / "ordinary.mat"
    savemat(conventional, {"position": [[1, 2], [3, 4]]})
    result = inventory_file(conventional)
    assert result["arrays"][0]["shape"] == [2, 2]
    hdf = tmp_path / "v73.mat"
    with h5py.File(hdf, "w") as handle:
        handle.create_dataset("trace", shape=(10, 2, 3), dtype="float32")
    result = inventory_file(hdf)
    assert result["format"] == "MATLAB-v7.3/HDF5"
    assert result["arrays"][0]["shape"] == [10, 2, 3]


def test_ena_mate_metadata_must_agree():
    row = {"run_accession": "SRR1", "sample_accession": "SAM1", "library_layout": "PAIRED", "fastq_ftp": "ftp.sra.ebi.ac.uk/run_1.fastq.gz;ftp.sra.ebi.ac.uk/run_2.fastq.gz", "fastq_md5": "0"*32 + ";" + "1"*32, "fastq_bytes": "10;20"}
    items = ena_fastq_assets([row], {"SRR1"}, "crosswalk")
    assert len(items) == 2 and all(item["eligible"] for item in items)
    row["fastq_md5"] = "0"*32
    with pytest.raises(AcquisitionError, match="mismatch"):
        ena_fastq_assets([row])
