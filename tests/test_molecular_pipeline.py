import gzip

import pandas as pd
import pytest

from horsehair.molecular import MolecularError
from horsehair.molecular_pipeline import ant_protein_map, go_gene_annotations, parse_fly_catalog, parse_fly_relationships


def gz(path, content):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(content)
    return path


def test_source_fly_relationships_and_primary_catalog_fields(tmp_path):
    relationship = gz(tmp_path / "relationships.gz", "#gene_ID\ttranscript_ID\tpolypeptide_ID\nFBgn0000001\tFBtr0000001\tFBpp0000001\nFBgn0000001\tFBtr0000002\tFBpp0000002\n")
    frame, audit = parse_fly_relationships(relationship)
    assert frame.gene_id.nunique() == 1 and audit["proteins"] == 2
    catalog = gz(tmp_path / "catalog.gz", "#gene_symbol\tprimary_FBgn\tsecondary_FBgn\tannotation_ID\nDmel\\geneA\tFBgn0000001\tFBgn0000002\tCG1\n")
    result = parse_fly_catalog(catalog)
    assert result.gene_id.item() == "FBgn0000001"
    assert result.secondary_gene_ids.item() == "FBgn0000002"
    assert result.gene_symbol.item() == "Dmel\\geneA"
    catalog = gz(tmp_path / "ambiguous.gz", "geneA\tFBgn0000001\tFBgn0000002\n")
    with pytest.raises(MolecularError, match="header is unresolved"):
        parse_fly_catalog(catalog)


def test_source_refseq_protein_ids_must_match_same_gtf_gene_namespace(tmp_path):
    gtf = tmp_path / "genes.gtf"
    gtf.write_text('NC_1\tRefSeq\tCDS\t1\t12\t.\t+\t0\tgene_id "LOC1"; protein_id "XP_1.1";\n')
    proteins = tmp_path / "proteins.fa"
    proteins.write_text(">XP_1.1 source annotation\nMKKK\n")
    frame = ant_protein_map(gtf, proteins)
    assert frame.gene_id.item() == "LOC1"
    proteins.write_text(">XP_1.2 changed release\nMKKK\n")
    with pytest.raises(MolecularError, match="exact protein mapping"):
        ant_protein_map(gtf, proteins)


def test_fixed_go_descendants_negation_taxon_and_source_version(tmp_path):
    obo = gz(tmp_path / "go.obo.gz", "format-version: 1.2\ndata-version: synthetic_fixture\n[Term]\nid: GO:0050877\n[Term]\nid: GO:0007623\n[Term]\nid: GO:0000001\nis_a: GO:0050877 ! fixed neuronal root\n")
    def row(gene, term, qualifier="", taxon="taxon:7227"):
        return "\t".join(["FB", gene, "gene", qualifier, term, "PMID:1", "IDA", "", "P", "gene", "", "protein", taxon, "20260101", "FlyBase", "", ""])+"\n"
    gaf = gz(tmp_path / "go.gaf.gz", "!gaf-version: 2.2\n"+row("FBgn0000001", "GO:0000001")+row("FBgn0000002", "GO:0007623", "NOT")+row("FBgn0000003", "GO:0007623", taxon="taxon:9606"))
    frame, audit = go_gene_annotations(obo, gaf, ["FBgn0000001", "FBgn0000002", "FBgn0000003"])
    assert frame.neuronal_circadian.tolist() == [1, 0, 0]
    assert audit["ontology_data_version"] == "synthetic_fixture"
