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


def test_pinned_software_internal_links_are_copied_as_regular_files(tmp_path):
    import tarfile,io
    archive=tmp_path/'software.tar'
    with tarfile.open(archive,'w') as handle:
        entry=tarfile.TarInfo('root/examples/input.txt');entry.size=4;handle.addfile(entry,io.BytesIO(b'data'))
        link=tarfile.TarInfo('root/tests/input.txt');link.type=tarfile.SYMTYPE;link.linkname='../examples/input.txt';handle.addfile(link)
    with pytest.raises(AcquisitionError,match='Unsafe TAR'):safe_extract(archive,tmp_path/'strict')
    records=safe_extract(archive,tmp_path/'software',materialize_internal_links=True)
    path=tmp_path/'software/root/tests/input.txt'
    assert path.read_bytes()==b'data' and not path.is_symlink()
    assert any(record.get('materialized_internal_link')=='../examples/input.txt' for record in records)
    with pytest.raises(AcquisitionError,match='size'):safe_extract(archive,tmp_path/'cap',materialize_internal_links=True,max_unpacked_bytes=7)


def test_software_link_permission_never_allows_external_or_missing_targets(tmp_path):
    import tarfile
    for target in ['../../escape','/etc/passwd','missing.txt']:
        archive=tmp_path/(str(len(target))+'.tar')
        with tarfile.open(archive,'w') as handle:
            link=tarfile.TarInfo('root/link');link.type=tarfile.SYMTYPE;link.linkname=target;handle.addfile(link)
        with pytest.raises(AcquisitionError):safe_extract(archive,tmp_path/'output',materialize_internal_links=True)


def test_rna_pilot_selection_is_depth_based_and_excludes_refusal_outcomes():
    from horsehair.acquire import rna_pilot_asset_ids
    rows=[{'biological_id':name,'run_id':run,'read_count':depth,'included':True,'cohort':cohort}
          for name,run,depth,cohort in [('low','SRR1',10,'discovery'),('middle','SRR2',20,'discovery'),('high','SRR3',30,'discovery'),('refused','SRR4',999999,'challenge')]]
    assets=[{'asset_id':run,'source_id':'ant_rna','run_accession':run,'eligible':True} for run in ['SRR1','SRR2','SRR3','SRR4']]
    assets += [{'asset_id':'reference','source_id':'ant_reference'},{'asset_id':'counts','source_id':'mantis_rna'},{'asset_id':'proteome','source_id':'fly_reference'}]
    manifest={'sources':{'ant_rna':{'crosswalk_audit':{'status':'validated'},'crosswalk':rows}},'assets':assets}
    assert set(rna_pilot_asset_ids(manifest))=={'SRR1','SRR3','reference','counts','proteome'}
    manifest['sources']['ant_rna']['crosswalk_audit']['status']='unresolved'
    with pytest.raises(AcquisitionError,match='validated'):rna_pilot_asset_ids(manifest)


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


def _seed_resumable_fixture(tmp_path, prefix_size=4):
    body = b"0123456789"
    item = object_for(body)
    target = safe_destination(tmp_path, item["relative_path"])
    target.parent.mkdir(parents=True)
    partial = target.with_name(target.name + ".part")
    partial.write_bytes(body[:prefix_size])
    sidecar = partial.with_name(partial.name + ".json")
    sidecar.write_text(json.dumps({"asset_hash": canonical_hash(item), "etag": '"stable"',
        "last_modified": "Fri, 05 Dec 2025 23:29:36 GMT"}))
    return body, item, target, partial, sidecar


@pytest.mark.parametrize("headers", [
    {"Content-Range": "bytes 5-9/10"},
    {"Content-Range": "bytes 4-3/10"},
    {"Content-Range": "bytes 4-10/10"},
    {"Content-Range": "bytes 4-9/11"},
    {"Content-Range": "bytes 4-9/*"},
    {"Content-Range": "bytes 4-9/10 trailing"},
    {"Content-Range": "bytes 4-9/10", "Content-Length": "5"},
    {"Content-Range": "bytes 4-9/10", "Content-Length": "-6"},
    {"Content-Range": "bytes 4-9/10", "Content-Length": "6x"}])
def test_invalid_full_range_headers_preserve_existing_prefix_and_sidecar(tmp_path, headers):
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path)
    before = partial.read_bytes(), sidecar.read_bytes()
    with pytest.raises(AcquisitionError):
        download_asset(item, tmp_path, session=Session(Response(body[4:], 206, headers)))
    assert (partial.read_bytes(), sidecar.read_bytes()) == before
    assert not target.exists()


@pytest.mark.parametrize("status", [200, 206])
@pytest.mark.parametrize("encoding", ["gzip", "br", "gzip, identity"])
def test_unsolicited_encoding_is_rejected_before_restart_or_resume_write(tmp_path, status, encoding):
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path)
    before = partial.read_bytes(), sidecar.read_bytes()
    headers = {"Content-Encoding": encoding, "Content-Range": "bytes 4-9/10"}
    session = Session(Response(body if status == 200 else body[4:], status, headers))
    with pytest.raises(AcquisitionError, match="encoded download representation"):
        download_asset(item, tmp_path, session=session)
    assert session.calls[0][1]["headers"]["Accept-Encoding"] == "identity"
    assert (partial.read_bytes(), sidecar.read_bytes()) == before
    assert not target.exists()


@pytest.mark.parametrize("headers", [
    {"Content-Range": "bytes 4-9/10", "ETag": '"different"'},
    {"Content-Range": "bytes 4-9/10", "Last-Modified": "Sat, 06 Dec 2025 20:54:25 GMT"}])
def test_changed_returned_resume_validators_preserve_existing_prefix_and_sidecar(tmp_path, headers):
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path)
    before = partial.read_bytes(), sidecar.read_bytes()
    with pytest.raises(AcquisitionError, match="changed during resumed transfer"):
        download_asset(item, tmp_path, session=Session(Response(body[4:], 206, headers)))
    assert (partial.read_bytes(), sidecar.read_bytes()) == before
    assert not target.exists()


def test_short_stream_retains_old_validator_and_valid_prefix_for_next_resume(tmp_path):
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path)
    with pytest.raises(AcquisitionError, match="shorter than its declared byte span"):
        download_asset(item, tmp_path, session=Session(Response(body[4:7], 206,
            {"Content-Range": "bytes 4-9/10", "Content-Length": "6"})))
    assert not target.exists() and partial.read_bytes() == body[:7]
    retained = json.loads(sidecar.read_text())
    assert retained["etag"] == '"stable"'
    assert retained["last_modified"] == "Fri, 05 Dec 2025 23:29:36 GMT"
    assert retained["transfer_response"]["expected_body_bytes"] == 6
    session = Session(Response(body[7:], 206, {"Content-Range": "bytes 7-9/10", "Content-Length": "3", "Content-Encoding": "identity"}))
    assert download_asset(item, tmp_path, session=session)["status"] == "verified"
    assert session.calls[0][1]["headers"]["If-Range"] == '"stable"'
    assert target.read_bytes() == body and not partial.exists() and not sidecar.exists()


def test_overlong_ranged_stream_never_appends_the_excess_chunk_or_promotes(tmp_path):
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path)
    with pytest.raises(AcquisitionError, match="exceeds its declared byte span"):
        download_asset(item, tmp_path, session=Session(Response(body[4:] + b"x", 206,
            {"Content-Range": "bytes 4-9/10", "Content-Length": "6"})))
    assert not target.exists() and partial.read_bytes() == body[:4]


def test_source_without_pinned_size_cannot_promote_a_shorter_declared_range(tmp_path):
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path)
    item["bytes"] = None
    receipt = json.loads(sidecar.read_text())
    receipt["asset_hash"] = canonical_hash(item)
    sidecar.write_text(json.dumps(receipt))
    with pytest.raises(AcquisitionError, match="does not complete its declared representation"):
        download_asset(item, tmp_path, session=Session(Response(body[4:8], 206,
            {"Content-Range": "bytes 4-7/10", "Content-Length": "4"})))
    assert partial.read_bytes() == body[:8] and not target.exists()


def test_full_restart_length_mismatch_is_rejected_before_existing_prefix_is_truncated(tmp_path):
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path)
    before = partial.read_bytes(), sidecar.read_bytes()
    with pytest.raises(AcquisitionError, match="Content-Length differs from pinned"):
        download_asset(item, tmp_path, session=Session(Response(body, 200, {"Content-Length": "9"})))
    assert (partial.read_bytes(), sidecar.read_bytes()) == before
    assert not target.exists()


def test_compliant_full_restart_with_new_validator_verifies_original_source_pin(tmp_path):
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path)
    session = Session(Response(body, 200, {"Content-Length": "10", "Content-Encoding": "identity", "ETag": '"new"'}))
    assert download_asset(item, tmp_path, session=session)["status"] == "verified"
    assert target.read_bytes() == body and not partial.exists() and not sidecar.exists()


def test_interrupted_restart_cannot_bind_old_prefix_to_replacement_validator(tmp_path, monkeypatch):
    import horsehair.acquire as acquire
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path)
    old_sidecar = sidecar.read_bytes()
    def interrupted_sidecar(path, value):
        # Simulate failure at the restart binding boundary, before any payload
        # write. Old representation bytes must already be gone at this point.
        assert partial.stat().st_size == 0
        raise OSError("Synthetic sidecar-write interruption")
    monkeypatch.setattr(acquire, "atomic_json", interrupted_sidecar)
    with pytest.raises(AcquisitionError, match="Synthetic sidecar-write interruption"):
        download_asset(item, tmp_path, attempts=1,
            session=Session(Response(body, 200, {"ETag": '"new"', "Content-Length": "10"})))
    assert not target.exists() and partial.stat().st_size == 0
    assert sidecar.read_bytes() == old_sidecar


@pytest.mark.parametrize("valid", [True, False])
def test_complete_partial_416_still_requires_pinned_checksum(tmp_path, valid):
    body, item, target, partial, sidecar = _seed_resumable_fixture(tmp_path, 10)
    if not valid:
        partial.write_bytes(b"xxxxxxxxxx")
    before = partial.read_bytes(), sidecar.read_bytes()
    session = Session(Response(b"", 416))
    if valid:
        assert download_asset(item, tmp_path, session=session)["status"] == "verified"
        assert target.read_bytes() == body and not partial.exists() and not sidecar.exists()
    else:
        with pytest.raises(AcquisitionError, match="Provider checksum mismatch"):
            download_asset(item, tmp_path, session=session)
        assert not target.exists() and (partial.read_bytes(), sidecar.read_bytes()) == before


def test_bad_complete_partial_is_preserved_before_fresh_verified_get(tmp_path):
    from horsehair.acquire import quarantine_invalid_complete_partial
    body = b'0123456789'
    bad = b'xxxxxxxxxx'
    item = object_for(body)
    target = safe_destination(tmp_path, item['relative_path'])
    target.parent.mkdir(parents=True)
    partial = target.with_name(target.name + '.part')
    sidecar = partial.with_name(partial.name + '.json')
    partial.write_bytes(bad)
    sidecar.write_text(json.dumps({'asset_hash': canonical_hash(item), 'etag': 'old-object'}))
    original_sidecar = sidecar.read_bytes()
    record = quarantine_invalid_complete_partial(item, tmp_path, 'fixture-repair')
    assert not partial.exists() and not sidecar.exists() and not target.exists()
    assert Path(record['preserved_partial']).read_bytes() == bad
    assert Path(record['preserved_sidecar']).read_bytes() == original_sidecar
    assert record['observed_digests']['md5'] == hashlib.md5(bad).hexdigest()
    assert record['provider_checksum'] == item['provider_checksum']
    session = Session(Response(body))
    assert download_asset(item, tmp_path, session=session)['status'] == 'verified'
    assert 'Range' not in session.calls[0][1]['headers']
    assert target.read_bytes() == body
    assert Path(record['preserved_partial']).read_bytes() == bad


@pytest.mark.parametrize('variant', ['unknown-provenance', 'incomplete', 'valid', 'final'])
def test_partial_recovery_never_moves_other_or_verified_source(tmp_path, variant):
    from horsehair.acquire import quarantine_invalid_complete_partial
    body = b'0123456789'
    item = object_for(body)
    target = safe_destination(tmp_path, item['relative_path'])
    target.parent.mkdir(parents=True)
    partial = target.with_name(target.name + '.part')
    sidecar = partial.with_name(partial.name + '.json')
    partial.write_bytes(body if variant == 'valid' else b'xxxx' if variant == 'incomplete' else b'xxxxxxxxxx')
    sidecar.write_text(json.dumps({'asset_hash': 'unknown' if variant == 'unknown-provenance' else canonical_hash(item)}))
    if variant == 'final':
        target.write_bytes(body)
    before = partial.read_bytes(), sidecar.read_bytes()
    with pytest.raises(AcquisitionError):
        quarantine_invalid_complete_partial(item, tmp_path, 'fixture-repair')
    assert (partial.read_bytes(), sidecar.read_bytes()) == before
    assert not (target.parent / '.quarantine').exists()


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
