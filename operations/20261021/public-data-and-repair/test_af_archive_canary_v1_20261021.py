"""Canary-wrapper JUnit contract; actual local archive suite XML, no native I/O."""
from pathlib import Path
import importlib.util
import runpy
import xml.etree.ElementTree as ET

import pytest

spec=importlib.util.spec_from_file_location('native_archive_canary_wrapper',Path(__file__).with_name('run-af-archive-canary-v1-20261021.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
lib=runpy.run_path(str(Path(__file__).with_name('verified-evidence-archive-20261021.py')))
NAMES=lib['EXPECTED_CANARY_TEST_NAMES']


def source_xml():
    root=ET.Element('testsuites');suite=ET.SubElement(root,'testsuite',tests='39',failures='0',errors='0',skipped='0')
    for name in NAMES:
        ET.SubElement(suite,'testcase',name=name,classname='work.test_verified_evidence_archive_20261021')
    return root,suite


def test_actual_all39_names_and_counts_accepted(tmp_path):
    root,suite=source_xml();path=tmp_path/'junit.xml';ET.ElementTree(root).write(path)
    assert m.verify_junit(path,NAMES)==m.canonical_hash(sorted(NAMES))


@pytest.mark.parametrize('change',['missing','duplicate','failure','skipped','wrong_class','wrong_counts','attributes_only','entity'])
def test_incomplete_or_false_native_junit_is_refused(tmp_path,change):
    root,suite=source_xml();cases=suite.findall('testcase')
    if change=='missing':suite.remove(cases[-1])
    elif change=='duplicate':cases[-1].set('name',cases[0].get('name'))
    elif change=='failure':ET.SubElement(cases[-1],'failure',message='native failure')
    elif change=='skipped':ET.SubElement(cases[-1],'skipped',message='native skip')
    elif change=='wrong_class':cases[-1].set('classname','different_suite')
    elif change=='wrong_counts':suite.set('tests','1')
    elif change=='attributes_only':
        for child in cases:suite.remove(child)
    path=tmp_path/'junit.xml';ET.ElementTree(root).write(path)
    if change=='entity':path.write_text('<!DOCTYPE testsuites [<!ENTITY x "unsafe">]>'+path.read_text())
    with pytest.raises(ValueError):m.verify_junit(path,NAMES)
