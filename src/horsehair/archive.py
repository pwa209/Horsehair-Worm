"""Finite, integrity-checked preservation of completed outputs; never deletes scratch."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from .core import atomic_json, canonical_hash, digest, native_job_id, read_json, source_identity

MAX_BYTES = 100_000_000_000
SMALL_TABLE_BYTES = 256*1024**2
RESERVE_BYTES = 1024**3
TABLE_SUFFIXES = {".json",".tsv",".csv",".txt",".parquet",".rds"}
TEMP_SUFFIXES = {".lock",".partial",".part",".tmp",".temp"}
VOLATILE = {"resource-budget.json","storage-budget.json","native-accounting.tsv","transfer-launcher.json"}


class ArchiveError(ValueError):
    pass


def _linked(path):
    return path.is_symlink() or (hasattr(path,"is_junction") and path.is_junction())


def _safe(path, base):
    path,base = Path(path),Path(base)
    if not path.is_absolute() or not base.is_absolute():
        raise ArchiveError("Archive paths must be absolute")
    current = path
    while True:
        if _linked(current):
            raise ArchiveError("Archive symlinks/junctions are forbidden")
        if current==base:
            break
        if current==current.parent:
            raise ArchiveError("Archive path escapes its declared namespace")
        current = current.parent
    if not path.resolve().is_relative_to(base.resolve()):
        raise ArchiveError("Archive traversal outside declared namespace")
    return path


def _quota_row(text):
    rows = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields)==9 and fields[0].startswith("/"):
            try:
                values = [int(fields[index].rstrip("*")) for index in (1,2,3,5,6,7)]
            except ValueError as error:
                raise ArchiveError("Unknown native project quota format; preserve and review the output") from error
            if any(value<0 for value in values):
                raise ArchiveError("Invalid negative native quota value")
            used,soft,hard,used_files,soft_files,hard_files = values
            byte_limits = [value*1024 for value in (soft,hard) if value>0]
            file_limits = [value for value in (soft_files,hard_files) if value>0]
            rows.append({"filesystem":fields[0],"used_bytes":used*1024,"byte_limits":byte_limits,
                         "available_bytes":min(byte_limits)-used*1024 if byte_limits else None,
                         "used_files":used_files,"file_limits":file_limits,
                         "available_files":min(file_limits)-used_files if file_limits else None,
                         "zero_limit_interpretation":"explicitly unlimited; never invented available capacity"})
    if len(rows)!=1:
        raise ArchiveError("Exactly one native user/group project quota row is required")
    return rows[0]


def _quota_capacity(user, group, bytes_required, files_required):
    if not group["byte_limits"]:
        raise ArchiveError("A positive project/group allocation limit has not been measured")
    for kind,row in (("user",user),("project_group",group)):
        if row["available_bytes"] is not None and row["available_bytes"]<bytes_required+RESERVE_BYTES:
            raise ArchiveError(f"Measured {kind} byte quota cannot hold archive plus reserve")
        if row["available_files"] is not None and row["available_files"]<files_required+100:
            raise ArchiveError(f"Measured {kind} inode quota cannot hold archive plus reserve")


def _native_quota(project, bytes_required, files_required, runner=subprocess.run):
    project = Path(project).resolve()
    authorized = Path("/project/def-ptewarie/pwa209/horsehair-worm")
    if os.name!="posix" or not project.is_relative_to(authorized):
        raise ArchiveError("Native preservation requires the configured personal Narval project namespace")
    records = {}
    for kind,selector,value in (("user","-u",str(os.getuid())),("project_group","-g","def-ptewarie")):
        command = ["lfs","quota",selector,value,"/project/def-ptewarie"]
        response = runner(command,text=True,capture_output=True,timeout=120)
        if response.returncode:
            raise ArchiveError(f"Native {kind} project quota is unavailable; no archive copy authorized")
        records[kind] = {"command":command,"stdout":response.stdout,"parsed":_quota_row(response.stdout)}
    _quota_capacity(records["user"]["parsed"],records["project_group"]["parsed"],bytes_required,files_required)
    available = shutil.disk_usage(project).free
    if available<bytes_required+RESERVE_BYTES:
        raise ArchiveError("Measured project filesystem free space cannot hold archive plus reserve")
    return {"captured_utc":datetime.now(timezone.utc).isoformat(),"records":records,
            "filesystem_available_bytes":available,"archive_forecast_bytes":bytes_required,
            "archive_forecast_files":files_required,"reserve_bytes":RESERVE_BYTES,
            "quota_semantics":"Lustre default kbytes are 1024 bytes; zero limits are unlimited; all finite limits enforced",
            "quota_documentation":["https://github.com/lustre/lustre-release/blob/master/Documentation/man1/lfs-quota.1",
                                    "https://github.com/lustre/lustre-release/blob/master/Documentation/man1/lfs-setquota.1"]}


def _temporary(path):
    return path.name==".stage.lock" or path.suffix.lower() in TEMP_SUFFIXES or any(part in {"tmp","temp","__pycache__"} for part in path.parts)


def _tree(directory, base):
    if not directory.exists():
        return []
    _safe(directory,base)
    files = []
    for parent,dirs,names in os.walk(directory,followlinks=False):
        for name in list(dirs):
            child = _safe(Path(parent)/name,base)
            if _temporary(child):
                dirs.remove(name)
        for name in names:
            child = _safe(Path(parent)/name,base)
            if child.is_file() and not _temporary(child):
                files.append(child)
    return sorted(files)


def _collect(root, release, identity):
    selected,omitted = {},[]
    def add(source,relative):
        if relative in selected:
            raise ArchiveError("Duplicate durable archive destination")
        selected[relative] = source
    for path in _tree(root/"results",root):
        add(path,path.relative_to(root).as_posix())
    for section in ("ant","mantis","freeze"):
        for path in _tree(root/"data/molecular"/section,root):
            if path.suffix.lower() in TABLE_SUFFIXES and path.stat().st_size<=SMALL_TABLE_BYTES:
                add(path,path.relative_to(root).as_posix())
            else:
                omitted.append({"source_path":str(path),"reason":"working/intermediate format or exceeds 256 MiB table limit"})
    for directory in (root/"manifests",root/"plans"):
        for path in _tree(directory,root):
            add(path,path.relative_to(root).as_posix())
    state = root/"state"
    patterns = ["*submission*.json","deployment-*.json","admission-*.json","*benchmark*.json","*qualification*.json",
                "environment.json","measured-admission.json","source-*.json","transfer-*.json","rna-pilot-*.json","quota-*.txt"]
    for pattern in patterns:
        for path in sorted(state.glob(pattern)):
            _safe(path,root)
            if path.name not in VOLATILE and path.is_file() and not _temporary(path):
                selected.setdefault(path.relative_to(root).as_posix(),path)
    source_names = set(identity["files"])|{"release.json"}
    if (release/"README.md").exists():
        source_names.add("README.md")
    for path in _tree(release/"docs",release):
        if path.suffix==".md":
            source_names.add(path.relative_to(release).as_posix())
    for relative in sorted(source_names):
        name = PurePosixPath(relative)
        if name.is_absolute() or ".." in name.parts:
            raise ArchiveError("Invalid immutable source identity path")
        add(_safe(release/relative,release),"source/"+relative)
    items = [{"relative_path":name,"source_path":str(path.resolve()),"source_sha256":digest(path),
              "source_bytes":path.stat().st_size,"bytes":path.stat().st_size,"sha256":digest(path),"kind":"immutable_file"}
             for name,path in sorted(selected.items())]
    return items,omitted


def _snapshot(path,relative):
    # JSON writers are atomic. Native accounting is captured only if every
    # line has a complete schema; the source may legitimately advance later.
    body = path.read_bytes()
    text = body.decode("utf-8")
    if path.suffix==".json":
        payload = json.loads(text)
    else:
        lines = text.splitlines()
        if not lines or lines[0]!="JobIDRaw|State|ElapsedRaw|AllocCPUS|MaxRSS|AllocTRES" or any(len(row.split("|"))!=6 for row in lines[1:]):
            raise ArchiveError("Native accounting snapshot is being written or has an unknown schema")
        payload = text
    record = {"captured_utc":datetime.now(timezone.utc).isoformat(),"original_source_path":str(path.resolve()),
              "original_source_sha256":hashlib.sha256(body).hexdigest(),"original_source_bytes":len(body),"captured_payload":payload,
              "interpretation":"coherent point-in-time evidence snapshot; live source may subsequently advance"}
    data = (json.dumps(record,sort_keys=True,indent=2,allow_nan=False)+"\n").encode()
    return {"relative_path":relative,"source_path":str(path.resolve()),"source_sha256":record["original_source_sha256"],
            "source_bytes":len(body),"bytes":len(data),"sha256":hashlib.sha256(data).hexdigest(),"kind":"captured_snapshot","content":data}


def _copy_item(item,directory):
    target = _safe(directory/item["relative_path"],directory)
    target.parent.mkdir(parents=True,exist_ok=True)
    temporary = target.with_name(target.name+".partial")
    with temporary.open("xb") as output:
        if item["kind"]=="captured_snapshot":
            output.write(item["content"])
        else:
            with Path(item["source_path"]).open("rb") as source:
                shutil.copyfileobj(source,output,8*1024**2)
        output.flush();os.fsync(output.fileno())
    if temporary.stat().st_size!=item["bytes"] or digest(temporary)!=item["sha256"]:
        raise ArchiveError("Archive copy bytes/hash differ; preserve incomplete destination for review")
    if item["kind"]=="immutable_file" and digest(Path(item["source_path"]))!=item["source_sha256"]:
        raise ArchiveError("Source changed during archive copy; preserve incomplete destination")
    if target.exists():
        raise ArchiveError("Archive destination file appeared during copy; preserve ambiguity")
    temporary.replace(target)


def verify(directory, config, identity, release_id):
    directory = Path(directory)
    path = directory/"archive_manifest.json"
    if not path.is_file():
        raise ArchiveError("Archive directory lacks completion receipt; preserve it and review ambiguous copy")
    record = read_json(path)
    payload = dict(record);self_hash = payload.pop("receipt_sha256",None)
    if self_hash!=canonical_hash(payload) or record.get("status")!="complete":
        raise ArchiveError("Archive completion receipt self-hash/status is invalid")
    if (record.get("config_sha256")!=canonical_hash(config) or record.get("source_sha256")!=identity["sha256"]
            or record.get("release_id")!=release_id):
        raise ArchiveError("Durable archive belongs to another source/configuration release")
    expected = {item["relative_path"] for item in record["files"]}|{"archive_manifest.json"}
    actual = {path.relative_to(directory).as_posix() for path in _tree(directory,directory)}
    if expected!=actual:
        raise ArchiveError("Durable archive file roster differs from its completion receipt")
    for item in record["files"]:
        relative = PurePosixPath(item["relative_path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ArchiveError("Archive receipt contains unsafe file traversal")
        target = _safe(directory/item["relative_path"],directory)
        if target.stat().st_size!=item["bytes"] or digest(target)!=item["sha256"]:
            raise ArchiveError("Durable archive file bytes/hash changed")
    return record


def run(config, root):
    job_id = native_job_id()
    if not job_id:
        raise ArchiveError("Preservation is finite native Slurm work; local calls require unit mocks")
    root = Path(root).absolute()
    release = Path(__file__).resolve().parents[2]
    identity = source_identity(release)
    release_record = read_json(release/"release.json")
    release_id = release_record.get("release_id","")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}",release_id):
        raise ArchiveError("Unsafe durable release identity")
    if release_record.get("source_identity")!=identity or release_record.get("config_sha256")!=canonical_hash(config):
        raise ArchiveError("Immutable source/configuration differs from its preservation release")
    project = Path(config["paths"]["project"]).absolute()
    _safe(root,root);_safe(project,project)
    if project==root or project.resolve().is_relative_to(root.resolve()):
        raise ArchiveError("Durable archive must be outside the scratch source namespace")
    report_path = _safe(root/"results/report/report.json",root)
    report = read_json(report_path)
    if (report.get("config_sha256")!=canonical_hash(config) or report.get("source_sha256")!=identity["sha256"]
            or report.get("analysis_complete") is not True or report.get("figures_complete") is not True):
        raise ArchiveError("Preservation requires matching completed actual analyses and figures")
    if report.get("evidence_status")=="synthetic_fixture":
        raise ArchiveError("Synthetic QA output is not a completed scientific archive")
    if ({claim.get("claim_id") for claim in report.get("claims",[])}!={"H1","H2","H3","H4"}
            or len(report.get("claims",[]))!=4
            or any(claim.get("status") not in {"supported","not_supported"} for claim in report["claims"])
            or {figure.get("id") for figure in report.get("figures",[])}!={f"{number:02}" for number in range(1,7)}
            or len(report.get("figures",[]))!=6):
        raise ArchiveError("Completed report must contain all four assessed claims and six actual figure rosters")
    for figure in report["figures"]:
        paths = figure.get("paths",[])
        if not paths or set(paths)!=set(figure.get("rendered_file_hashes",{})):
            raise ArchiveError("Report figure lacks complete rendered-file integrity provenance")
        for name in paths:
            path = _safe(Path(name),root)
            if not path.is_file() or digest(path)!=figure["rendered_file_hashes"][name]:
                raise ArchiveError("Actual figure changed after report completion")
    directory = _safe(project/"artifacts"/release_id,project)
    if directory.exists():
        record = verify(directory,config,identity,release_id)
        if record["report_sha256"]!=digest(report_path):
            raise ArchiveError("Report changed after durable preservation; use a reviewed new archive release")
        reused = True
    else:
        items,omitted = _collect(root,release,identity)
        for path in [root/"state"/name for name in sorted(VOLATILE)]+[root/"state/pipeline"/release_id/"status.json"]:
            if path.is_file():
                _safe(path,root)
                relative = "evidence_snapshots/"+path.relative_to(root).as_posix()+".json"
                items.append(_snapshot(path,relative))
        payload_bytes = sum(item["bytes"] for item in items)
        forecast = payload_bytes+max(1024**2,len(items)*4096)
        if forecast>MAX_BYTES:
            raise ArchiveError("Durable archive exceeds the finite 100 GB preservation ceiling")
        quota = _native_quota(project,forecast,len(items)*2+10)
        directory.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        directory.mkdir(mode=0o700)
        for item in items:
            _copy_item(item,directory)
        for item in items:
            if item["kind"]=="immutable_file" and digest(Path(item["source_path"]))!=item["source_sha256"]:
                raise ArchiveError("Source changed after archive copy; completion receipt withheld")
        if source_identity(release)!=identity:
            raise ArchiveError("Source release changed during archive copy; completion receipt withheld")
        record = {"schema_version":1,"status":"complete","release_id":release_id,
                  "config_sha256":canonical_hash(config),"source_sha256":identity["sha256"],
                  "source_identity":identity,"slurm_job_id":job_id,"captured_utc":datetime.now(timezone.utc).isoformat(),
                  "report_source_path":str(report_path),"report_sha256":digest(report_path),
                  "analysis_complete":True,"figures_complete":True,"publication_ready":report.get("publication_ready") is True,
                  "archive_directory":str(directory),"payload_bytes":payload_bytes,"file_count":len(items),
                  "files":[{key:value for key,value in item.items() if key!="content"} for item in items],
                  "omitted_molecular_intermediates":omitted,"native_project_quota":quota,
                  "preservation_scope":"all completed result outputs, small final ant/mantis/freeze tables, manifests, source, plans, selected stable evidence and explicit live-state snapshots",
                  "scratch_deleted":False}
        record["receipt_sha256"] = canonical_hash(record)
        if payload_bytes+len((json.dumps(record,sort_keys=True,indent=2)+"\n").encode())>MAX_BYTES:
            raise ArchiveError("Completion metadata would exceed finite archive ceiling; receipt withheld")
        atomic_json(directory/"archive_manifest.json",record)
        verify(directory,config,identity,release_id)
        reused = False
    result = {"status":"complete","release_id":release_id,"config_sha256":canonical_hash(config),
              "source_sha256":identity["sha256"],"archive_directory":str(directory),
              "completion_receipt":str(directory/"archive_manifest.json"),"manifest_sha256":digest(directory/"archive_manifest.json"),
              "receipt_sha256":record["receipt_sha256"],"payload_bytes":record["payload_bytes"],"file_count":record["file_count"],
              "slurm_job_id":job_id,"reused_verified_archive":reused,"publication_ready":record["publication_ready"],"scratch_deleted":False}
    atomic_json(root/"state"/("archive-"+release_id+".json"),result)
    return result
