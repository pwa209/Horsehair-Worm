"""The storage audit must count small artifacts without the stalled full stat."""
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from horsehair import budget
from horsehair.core import read_json,digest


def fixture(tmp_path,monkeypatch):
    from horsehair import storage_artifacts
    monkeypatch.setattr(budget,'PERSONAL_STUDY_ROOT',tmp_path)
    monkeypatch.setattr(budget.os,'getuid',lambda:3164768,raising=False)
    (tmp_path/'state').mkdir()
    quota=('Filesystem kbytes quota limit grace files quota limit grace\n'
           '/scratch/pwa209 1024 20000000000 20000000000 - 1000 1000000 1000000 -\n')
    def runner(command,**kwargs):
        assert command==['lfs','quota','-u','3164768','/scratch/pwa209']
        return SimpleNamespace(returncode=0,stdout=quota,stderr='')
    def put(relative,data):
        path=tmp_path/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data);return path
    return storage_artifacts,runner,put


def test_census_counts_exact_eof_artifacts_and_keeps_scientific_size_measurement(tmp_path,monkeypatch):
    artifacts,runner,put=fixture(tmp_path,monkeypatch)
    prefix='raw/ant_rna/SRR22989345_1.fastq.gz.curl-v3/attempts/0002/'
    stdout=put(prefix+'segment-000009.stdout.private',b'')
    stderr=put(prefix+'segment-000009.stderr.private',b'warning')
    text=put('tests/qualification-123/test_archive/report/figure.txt',b'x'*60)
    science=put('raw/ant_rna/SRR22989345_1.fastq.gz',b'raw')
    selected={stdout,stderr,text};calls=[]
    def measure(path,cap,*,root):
        assert root==tmp_path and path in selected
        calls.append(path);data=path.read_bytes()
        return {'status':'measured','method':'owned_regular_eof','bytes':len(data),'read_cap_bytes':cap,
            'sha256':hashlib.sha256(data).hexdigest(),'eof_observed':True,'identity':{'inode':10}}
    monkeypatch.setattr(artifacts,'measure_regular_eof',measure)
    original_stat,original_link=Path.stat,Path.is_symlink
    def stat(path,*args,**kwargs):
        assert path not in selected,'Candidate fell back to full Lustre size stat'
        return original_stat(path,*args,**kwargs)
    def is_symlink(path):
        assert path not in selected,'Candidate triggered full no-follow stat before safe measurement'
        return original_link(path)
    monkeypatch.setattr(Path,'stat',stat);monkeypatch.setattr(Path,'is_symlink',is_symlink)
    result=budget.storage_snapshot(tmp_path,runner=runner)
    assert set(calls)==selected and science not in calls
    assert result['study_bytes']==70 and result['study_files']==4
    assert result['personal_available_files']==999000
    descriptor=result['artifact_eof_measurements'];proof=read_json(descriptor['receipt_path'])
    assert descriptor['receipt_sha256']==digest(descriptor['receipt_path'])
    assert descriptor['count']==proof['count']==3 and descriptor['bytes']==proof['bytes']==67
    assert [row['relative_path'] for row in proof['items']]==sorted(str(p.relative_to(tmp_path)) for p in selected)
    assert all(row['eof_observed'] for row in proof['items'])


@pytest.mark.parametrize('failure',[OSError('read unavailable'),ValueError('cap exceeded')])
def test_artifact_measurement_failure_does_not_publish_an_underestimate(tmp_path,monkeypatch,failure):
    artifacts,runner,put=fixture(tmp_path,monkeypatch)
    put('raw/ant_rna/SRR123_1.fastq.gz.curl-v3/attempts/0001/segment-000000.stderr.private',b'x')
    prior=put('state/storage-budget.json',b'{"prior":"retained"}')
    def refuse(*args,**kwargs):raise failure
    monkeypatch.setattr(artifacts,'measure_regular_eof',refuse)
    with pytest.raises(type(failure),match=str(failure)):
        budget.storage_snapshot(tmp_path,runner=runner)
    assert prior.read_bytes()==b'{"prior":"retained"}'
    assert not (tmp_path/'state/storage-artifacts.json').exists()


def test_selected_symlink_marker_is_not_counted_as_a_zero_length_regular_file(tmp_path,monkeypatch):
    artifacts,runner,put=fixture(tmp_path,monkeypatch)
    put('raw/ant_rna/SRR123_1.fastq.gz.curl-v3/attempts/0001/segment-000000.stderr.private',b'placeholder')
    monkeypatch.setattr(artifacts,'measure_regular_eof',lambda *args,**kwargs:{'status':'symlink'})
    result=budget.storage_snapshot(tmp_path,runner=runner)
    assert result['study_bytes']==0 and result['study_files']==0
    assert 'artifact_eof_measurements' not in result
