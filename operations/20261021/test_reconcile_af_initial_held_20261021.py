import importlib.util
from pathlib import Path
import pytest
spec=importlib.util.spec_from_file_location('reconcile',Path(__file__).with_name('reconcile-af-initial-held-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

@pytest.mark.parametrize('value,accepted',[('1',True),('1-1',True),('1-2',False),('0-1',False),('01',False),('1,1',False),(None,False)])
def test_exact_single_native_node(value,accepted):
 assert m.exact_one_node(value) is accepted

def test_other_resource_proofs_remain_strict():
 helpers={'seconds':lambda s:{'02:00:00':7200}[s],'memory_bytes':lambda s:{'64G':64}[s]}
 resources={'cpus':1,'gpu':False,'hours':2,'memory':'64G'}
 fields={'NumNodes':'1-1','NumCPUs':'1','TimeLimit':'02:00:00','MinMemoryNode':'64G','ReqTRES':'cpu=1,mem=64G,node=1','AllocTRES':'(null)'}
 m.native_resources(fields,resources,helpers)
 for override in [{'NumCPUs':'2'},{'ReqTRES':'cpu=1,gres/gpu=1,gres/gpu:a100=1'},{'ReqTRES':'cpu=1,gres/gpu=0,gres/gpu=0'},{'ReqTRES':'cpu=1,gres/gpu=0,gres/gpu:a100=-1'}]:
  with pytest.raises(ValueError):m.native_resources(dict(fields,**override),resources,helpers)

def test_duplicate_scheduler_fields_rejected():
 with pytest.raises(ValueError):m.parse_fields('JobId=123 JobId=124 NumNodes=1-1')
