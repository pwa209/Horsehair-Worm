"""Structural source and ownership contracts for the local425 envelope factory."""
from pathlib import Path
import ast
import base64
import importlib.util
import json
import zlib

import pytest

W=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('coordinator_factory',W/'build-reviewed-registration-coordinator-425.py');b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)

def shell(ident):
    value={'id':ident,'helper_sha256':'a'*64,'fixture':'0123456789abcdefghijklmnopqrstuvwxyz'*16}
    blob=base64.b64encode(zlib.compress(json.dumps(value).encode())).decode()
    return ("#!/bin/bash\npython -u - <<'PY'\nimport json,zlib,base64\npayload=json.loads(zlib.decompress(base64.b64decode("+repr(blob)+")))\npass\nPY\n").encode()

@pytest.fixture
def graph(tmp_path):
    directory=tmp_path/'graph';directory.mkdir();helper=W/'coordinate-reviewed-registrations-v3-20261023.py'
    for ident in b.CHILD_IDS:(directory/('hhw-recovery-stage-20261021-'+ident+'.sh')).write_bytes(shell(ident))
    for name in b.CLOSURE:(directory/name).write_text('# source fixture '+name+'\n')
    qualified={'kind':'streaming-census-operational-source-manifest-v2','files':{name:b.sha((directory/name).read_bytes()) for name in b.CLOSURE},'suites':{name:{} for name in b.SCOPES}}
    (directory/'source-manifest.json').write_text(json.dumps(qualified))
    return directory,helper,qualified

def unpack(raw):
    source=raw.decode();body=source.split("python -u - <<'PY'\n",1)[1].rsplit('\nPY',1)[0];tree=ast.parse(body)
    namespace={}
    for node in tree.body:
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id in {'payload','manifest_raw','sources'} for t in node.targets):
            exec(compile(ast.Module(body=[node],type_ignores=[]),'<static-envelope-data>','exec'),{'json':json,'zlib':zlib,'base64':base64},namespace)
    return source,body,tree,namespace

def test_complete_eight_source_graph_exact_native_425_resources_and_task_intent(graph):
    directory,helper,_=graph;raw,manifest,summary=b.render(directory,helper,'452');source,body,tree,ns=unpack(raw)
    value=json.loads(manifest);payload=ns['payload']
    assert len(value['files'])==8 and set(value['files'])=={helper.name,*['hhw-recovery-stage-20261021-'+ident+'.sh' for ident in b.CHILD_IDS]}
    assert len(ns['sources'])==7 and payload['helper_sha256']==b.sha(helper.read_bytes())
    assert value['qualification']['task']=='recovery-452' and value['qualification']['validator']['path'].endswith('support-v3-20261023/run-fresh-storage-census-v6-20261023.py')
    assert b.sha(manifest)==payload['coordinator_manifest_sha256']==summary['source_manifest_sha256']
    assert "'resources':{'cpus':1,'memory':'16G','hours':4,'gpu':False,'array':None}" in body
    assert "'reviewed_registration_coordinator'" in body and "'external_dependencies':[qjob]" in body and "'dependency_kind':'afterok'" in body
    assert b'\r' not in raw;compile(body,'<envelope425>','exec')

@pytest.mark.parametrize('ident',b.CHILD_IDS)
def test_missing_actual_child_fails_before_any_output_is_written(graph,ident):
    directory,helper,_=graph;(directory/('hhw-recovery-stage-20261021-'+ident+'.sh')).unlink()
    before={p.name for p in directory.iterdir()}
    with pytest.raises(FileNotFoundError):b.main(['--directory',str(directory),'--helper',str(helper),'--qualification-id','452'])
    assert {p.name for p in directory.iterdir()}==before
    assert not (directory/'coordinator-source-manifest.json').exists() and not (directory/'hhw-recovery-stage-20261021-425.sh').exists()

@pytest.mark.parametrize('change',['source_bytes','extra_file','missing_file','extra_suite','missing_suite','old_kind','wrong_q_lineage','wrong_child_id','crlf'])
def test_graph_refuses_changed_or_incomplete_source_lineage(graph,change):
    directory,helper,manifest=graph
    if change=='source_bytes':(directory/'run-fresh-storage-census-v6-20261023.py').write_text('changed')
    elif change=='extra_file':manifest['files']['unknown.py']='a'*64
    elif change=='missing_file':manifest['files'].pop('run-fresh-storage-census-v6-20261023.py')
    elif change=='extra_suite':manifest['suites']['unknown']={}
    elif change=='missing_suite':manifest['suites'].pop('wrapper')
    elif change=='old_kind':manifest['kind']='streaming-census-operational-source-manifest-v1'
    elif change=='wrong_child_id':(directory/'hhw-recovery-stage-20261021-451.sh').write_bytes(shell('450'))
    elif change=='crlf':
        p=directory/'hhw-recovery-stage-20261021-451.sh';p.write_bytes(p.read_bytes().replace(b'\n',b'\r\n'))
    (directory/'source-manifest.json').write_text(json.dumps(manifest))
    with pytest.raises((ValueError,FileNotFoundError)):b.render(directory,helper,'444' if change=='wrong_q_lineage' else '452')

def test_every_embedded_shell_and_actual_helper_have_their_exact_source_bytes(graph):
    directory,helper,_=graph;raw,manifest,_=b.render(directory,helper,'452');_,_,_,ns=unpack(raw)
    assert zlib.decompress(base64.b64decode(ns['payload']['helper_body']))==helper.read_bytes()
    for name,item in ns['sources'].items():
        actual=zlib.decompress(base64.b64decode(item['data']));assert actual==(directory/name).read_bytes() and b.sha(actual)==item['sha256']==json.loads(manifest)['files'][name]

def test_envelope_orders_guard_source_and_actual_q_native_checks_before_submit(graph):
    directory,helper,_=graph;raw,_,_=b.render(directory,helper,'452');_,body,_,_=unpack(raw)
    assert body.index("runpy.run_path(str(gp))['install']")<body.index("with api['registration_lock']")<body.index("observed=repair._repair_native_states")<body.index('admission=campaign.combined_admission')<body.index('submitted=submit')
    assert "native['state'] in {'PENDING','CONFIGURING','RUNNING','COMPLETING','COMPLETED'}" in body
    assert "qr['state']=='submitted'" in body and "qt['rolling_census_qualification']" in body
    assert "qc[:3]==['python','-u',str(qhelper)]" in body and 'dict(zip(tail[::2],tail[1::2]))==flags' in body

def test_local_existing_candidate_cannot_be_overwritten(graph):
    directory,helper,_=graph;target=directory/'hhw-recovery-stage-20261021-425.sh';target.write_text('immutable')
    with pytest.raises(ValueError):b.main(['--directory',str(directory),'--helper',str(helper),'--qualification-id','452'])
    assert target.read_text()=='immutable'
