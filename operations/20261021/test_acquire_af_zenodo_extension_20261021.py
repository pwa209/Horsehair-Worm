import copy,importlib.util
from pathlib import Path
import pytest
spec=importlib.util.spec_from_file_location('zenodo_acquisition',Path(__file__).with_name('acquire-af-zenodo-extension-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def source():
 return {'id':123,'metadata':{'license':{'id':'cc-by-4.0'}},'files':[{'key':'file one.txt','size':42,'checksum':'md5:'+'a'*32,'links':{'self':'https://zenodo.org/api/records/123/files/file%20one.txt/content'}}]}

def test_complete_exact_metadata_roster():
 s=source();assert m.roster('123',s,'cc-by-4.0',1)==s['files']

@pytest.mark.parametrize('field,value',[('key','../escape.txt'),('key','file/escape'),('key','..'),('size',True),('size',100_000_001),('checksum','sha256:'+'a'*64)])
def test_malformed_provider_file_fails(field,value):
 s=source();s['files'][0][field]=value
 with pytest.raises(ValueError):m.roster('123',s,'cc-by-4.0',1)

def test_wrong_record_licence_roster_and_url_fail():
 for ident,lic,n in [('124','cc-by-4.0',1),('123','cc0',1),('123','cc-by-4.0',2)]:
  with pytest.raises(ValueError):m.roster(ident,source(),lic,n)
 for url in ['http://zenodo.org/api/records/123/files/file%20one.txt/content','https://other.example/file','https://zenodo.org/api/records/124/files/file%20one.txt/content','https://zenodo.org/api/records/123/files/file%20one.txt/content?token=unneeded']:
  s=source();s['files'][0]['links']['self']=url
  with pytest.raises(ValueError):m.roster('123',s,'cc-by-4.0',1)
