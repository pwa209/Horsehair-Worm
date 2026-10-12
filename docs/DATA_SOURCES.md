# Public sources and acquisition contracts

This source inventory was checked against public provider metadata on 12 October
2026. Repository descriptions and source READMEs are research material, not
execution instructions. `horsehair.acquire discover` reads metadata; it does not
download the study's large raw archives or sequencing reads.

| Source | Frozen identity and observed contents | Rights and primary route |
|---|---|---|
| Zombie fly | Zenodo record 7464925, DOI 10.5281/zenodo.7464925; `Summit paper data.zip`, **25,055,924,031 bytes**, MD5 `f7c1335d7d831150dbf45b511173925e` | CC BY 4.0; [record](https://zenodo.org/records/7464925), [record API](https://zenodo.org/api/records/7464925), [author file map](https://lab.debivort.org/zombie-summiting/) |
| Mantis polarization | DOI 10.5061/dryad.jh9w0vtkb; published version 375871 (version number 4); `data_deposited.xlsx` and `README.md` | CC0; [dataset API](https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.jh9w0vtkb), [version files](https://datadryad.org/api/v2/versions/375871/files) |
| Mantis mating | DOI 10.5061/dryad.4j0zpc8jw; published version 349261 (version number 5); `mating_experiment.csv` and `README.md`. Version 349261 legitimately references files originally deposited in version 313645. | CC0; [dataset API](https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.4j0zpc8jw), [version files](https://datadryad.org/api/v2/versions/349261/files) |
| Mantis/worm expression | GSE237182, associated PRJNA994131; separate Tenodera and Chordodes count/TPM matrices, SuperTranscript FASTA/GFF, and SOFT metadata | NCBI public deposition; no dataset-specific CC license is asserted. [Supplement directory](https://ftp.ncbi.nlm.nih.gov/geo/series/GSE237nnn/GSE237182/suppl/), [SOFT](https://ftp.ncbi.nlm.nih.gov/geo/series/GSE237nnn/GSE237182/soft/GSE237182_family.soft.gz) |
| Fluke/ant expression | PRJNA918668 currently has 96 runs: **84 Formica aserva runs and 12 Dicrocoelium dendriticum runs**. Formica aliases resolve 60 brains collected in 2021 and 24 additional 2019 brains. | INSDC public sequencing archive; retain depositor attribution and repository notices. [ENA metadata API](https://www.ebi.ac.uk/ena/portal/api/filereport?accession=PRJNA918668&result=read_run&fields=run_accession,sample_accession,sample_alias,scientific_name,read_count,fastq_ftp,fastq_md5,fastq_bytes&format=tsv), [BioProject](https://www.ncbi.nlm.nih.gov/bioproject/PRJNA918668) |
| Published ant assembly | Formica exsecta GCA_003651465.1, ASM365146v1, not the sequenced host species F. aserva | [NCBI assembly metadata](https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/GCA_003651465.1/dataset_report), [GenBank directory](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/003/651/465/GCA_003651465.1_ASM365146v1/) |
| Executable annotated reference | NCBI identifies GCF_003651465.1 as the paired RefSeq assembly. Its matching FASTA, GTF and proteins use NCBI Annotation Release 100. The current GenBank folder has **no GTF/GFF/protein FASTA**. | [RefSeq directory](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/003/651/465/GCF_003651465.1_ASM365146v1/), [provider MD5 file](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/003/651/465/GCF_003651465.1_ASM365146v1/md5checksums.txt). This is an explicit updated-annotation route, not claimed to reproduce the source's original 24,707 transcript annotation exactly. |
| Fly reference/catalog/GO | Fixed FlyBase FB2026_03, Dmel r6.69: translation FASTA, FBgn/FBtr/FBpp relationships, primary/secondary gene-ID catalog, GAF annotations and matching release's GO OBO. Protein FASTA MD5 `1844f571c897e51663c2eb4e144330e9`. | FlyBase-produced data CC BY 4.0; preserve external ontology attribution. [Protein directory](https://s3ftp.flybase.org/genomes/Drosophila_melanogaster/dmel_r6.69_FB2026_03/fasta/index.html), [gene files](https://s3ftp.flybase.org/releases/FB2026_03/precomputed_files/genes/index.html), [GO annotations](https://s3ftp.flybase.org/releases/FB2026_03/precomputed_files/go/index.html), [matching ontologies](https://s3ftp.flybase.org/releases/FB2026_03/precomputed_files/ontologies/index.html), [rights](https://wiki.flybase.org/wiki/FlyBase%3AAbout). The embedded OBO data-version is recorded after direct Narval transfer. |

Every acquired file records DOI/accession, source version, URL, declared byte
count where available, provider digest, rights, acquisition timestamp and local
SHA-256. Provider checksums are mandatory when supplied. Byte counts from an SRA
archive are not substituted for paired FASTQ sizes or expanded BAM storage.

Dryad's metadata API file-download links require authentication and returned
HTTP 401 during the source audit. The browser's public routes are retained as
the actual acquisition URLs: polarization [workbook](https://datadryad.org/downloads/file_stream/4165965)
and [README](https://datadryad.org/downloads/file_stream/4166002); mating
[CSV](https://datadryad.org/downloads/file_stream/3440233) and
[README](https://datadryad.org/downloads/file_stream/3440238). Dataset version,
provider file ID, original API download route and provider SHA-256 remain in the
manifest. These public URLs still require a successful, checksum-validated
transfer on Narval; an access denial never produces an empty dataset.

Actual biological files are transferred directly to Narval. The local workspace
contains code, small source metadata and synthetic validation fixtures. Source
archive inspection and scientific processing run under Slurm after native
transfer/integrity verification.

## The independent ant unit

Discovery retrieves the ENA run table and the public ENA BioSample XML for each
Formica sample. The primary cohort is identified by the source paper's 2021
collection, `Brain` tissue, infection attributes, full deposited sample alias and
replicate number. Alias treatment strings are checked against attributes. The
last alias integer is retained; it is not guessed to be a sequencing lane.

The primary cohort must contain exactly ten groups of six unique biological
brains: infected and environmentally matched uninfected samples for pre,
attached, newly detached and detached conditions; six refused-to-attach and six
refused-to-detach infected samples. The latter twelve are molecular challenge
animals. The 2019 host samples and parasite-gaster/metacercarial runs stay in the
audit with exclusion reasons. Multiple runs collapse only when their explicit
biological and prepared-library identities agree. Ambiguity stops acquisition
of raw ant reads; it does not inflate or silently reduce the source cohort.

## Operational commands

```bash
python -m horsehair.acquire discover --output "$STUDY/manifests/source_manifest.json"
python -m horsehair.acquire download --manifest "$STUDY/manifests/source_manifest.json" \
  --root "$STUDY" --sources ant_rna --max-files 120 \
  --max-bytes 6000000000000 --bandwidth-mib 64 --max-seconds 10800
python -m horsehair.acquire inventory --root "$STUDY/data/extracted" \
  --output "$STUDY/manifests/source_inventory.json"
```

`--root` is the study root: asset paths already begin `raw/`. Select other
sources in separate bounded transfer invocations. Discovery automatically
validates the ant crosswalk and makes only its 120 paired FASTQs eligible.
Existing manifests cannot be overwritten; changed versions require a new
manifest/amendment with a parent hash. Requests follow public APIs without
invoking downloaded code.

Downloads retain `.part` state and server ETag/Last-Modified for HTTP range
resumption. A server ignoring Range triggers a fresh temporary transfer. SHA-256
and the provider digest are computed together while writing; resumed prefixes
are hashed once. Files are atomically promoted only after size/digest checks.
Cached files receive one combined digest pass. HTML access-error pages are
rejected. Runtime, byte-count, file-count and bandwidth ceilings permit finite
transfer jobs with resumable failure, rather than an unrestricted controller.

Archive extraction rejects traversal, links, devices and excessive file counts
or expanded sizes before writing ordinary members. Optional large images/videos
can be omitted with an explicit inclusion regex; omitted products remain in the
inventory/audit. MAT inventory supports conventional MATLAB and HDF5/v7.3 without
inventing chronological timestamps or animal identities. The final biological
adapters must still resolve experiment/ROI keys and tracking time.

Compute-node internet access differs from login/data-transfer access. Network
denials are acquisition failures; they never turn missing sources into empty
datasets. Transfer location and Slurm execution are configured by the root
orchestrator. The acquisition module performs no scheduler or remote-account
mutation.
