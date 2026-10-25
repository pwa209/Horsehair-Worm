import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile
import pytest

spec=importlib.util.spec_from_file_location('public_source_archive_sync',Path(__file__).parents[1]/'scripts/sync_public_source_archive.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def make(tmp_path,names):
    path=tmp_path/'source.tar.gz';files={}
    with tarfile.open(path,'w:gz') as tar:
        for name,kind,body in names:
            member=tarfile.TarInfo(name)
            if kind=='file':member.size=len(body);files[name]=hashlib.sha256(body).hexdigest();tar.addfile(member,io.BytesIO(body))
            elif kind=='symlink':member.type=tarfile.SYMTYPE;member.linkname='outside';tar.addfile(member)
            else:member.type=tarfile.DIRTYPE;tar.addfile(member)
    return path,{'archive_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'files':files}


def test_complete_source_is_verified_without_extracting_files(tmp_path):
    path,manifest=make(tmp_path,[('src','dir',b''),('src/a.py','file',b'pass\n'),('README.md','file',b'documentation')])
    assert m.verify_archive(path,manifest)==2
    assert sorted(p.name for p in tmp_path.iterdir())==['source.tar.gz']


@pytest.mark.parametrize('name,kind',[('../outside','file'),('/absolute','file'),('src/../../outside','file'),('linked','symlink')])
def test_unsafe_archive_members_are_rejected(tmp_path,name,kind):
    path,manifest=make(tmp_path,[(name,kind,b'x')])
    with pytest.raises(ValueError):m.verify_archive(path,manifest)


def test_archive_byte_integrity_is_required(tmp_path):
    path,manifest=make(tmp_path,[('a','file',b'x')]);manifest['archive_sha256']='0'*64
    with pytest.raises(ValueError,match='bytes changed'):m.verify_archive(path,manifest)


@pytest.mark.parametrize('change',['missing','extra','modified'])
def test_every_public_source_file_must_match(tmp_path,change):
    path,manifest=make(tmp_path,[('a','file',b'x')])
    if change=='missing':manifest['files'].pop('a')
    elif change=='extra':manifest['files']['unseen']='0'*64
    else:manifest['files']['a']='0'*64
    with pytest.raises(ValueError,match='complete committed'):m.verify_archive(path,manifest)


def test_duplicate_members_are_not_silently_replaced(tmp_path):
    path,manifest=make(tmp_path,[('a','file',b'x'),('a','file',b'x')])
    with pytest.raises(ValueError,match='Duplicate'):m.verify_archive(path,manifest)
