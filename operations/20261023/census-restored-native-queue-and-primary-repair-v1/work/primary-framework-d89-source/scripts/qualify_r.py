"""Execute both production count-model branches on declared synthetic fixtures."""
import json
import os
from pathlib import Path
import subprocess
import numpy as np
import pandas as pd
from horsehair.core import atomic_json,digest


def run(root):
    source=Path(__file__).resolve().parent/'rna_counts.R'
    destination=Path(root)/'tests'/('native-r-'+os.environ.get('SLURM_JOB_ID','local'))
    destination.mkdir(parents=True,exist_ok=False)
    rng=np.random.default_rng(49128)
    records=[]
    for mode in ['ant','mantis']:
        rows=[]
        if mode=='ant':
            groups=[f'{infection}_{phase}' for infection in ['infected','uninfected'] for phase in ['pre','attached','newly_detached','detached']]+['infected_refused_attach','infected_refused_detach']
            for group in groups:
                for replicate in range(6):
                    ident=f'{group}_{replicate}'
                    rows.append({'count_column':ident,'biological_id':ident,'technical_library_id':ident,'group':group,
                        'infection':group.split('_')[0],'phase':group.split('_',1)[1],
                        'cohort':'challenge' if 'refused' in group else 'discovery','collection_year':2021,'batch':'fixture'})
            batch='batch'
        else:
            for group,count in [('infected_before',4),('infected_during',5),('uninfected',6)]:
                for replicate in range(count):
                    ident=f'{group}_{replicate}'
                    rows.append({'count_column':ident,'biological_id':ident,'technical_library_id':ident,'group':group,
                        'infection':'uninfected' if group=='uninfected' else 'infected','phase':group.split('_')[-1],
                        'collection_year':2020+replicate%2,'organism':'Tenodera angustipennis','tissue':'brain'})
            batch='collection_year'
        samples=pd.DataFrame(rows)
        means=np.exp(rng.uniform(3,7,size=200))[:,None]*rng.uniform(.7,1.3,size=len(rows))[None,:]
        counts=rng.negative_binomial(10,10/(10+means))
        table=pd.DataFrame(counts,columns=samples.count_column)
        table.insert(0,'gene_id',[f'SYNTHETIC_GENE_{i}' for i in range(200)])
        directory=destination/mode;directory.mkdir()
        table.to_csv(directory/'counts.tsv',sep='\t',index=False)
        samples.to_csv(directory/'samples.tsv',sep='\t',index=False)
        response=subprocess.run(['Rscript',str(source),'--mode',mode,'--counts',str(directory/'counts.tsv'),'--samples',str(directory/'samples.tsv'),'--output',str(directory/'output'),'--batch-column',batch],text=True,capture_output=True)
        (directory/'stdout.txt').write_text(response.stdout)
        (directory/'stderr.txt').write_text(response.stderr)
        record={'mode':mode,'returncode':response.returncode,'evidence_class':'synthetic software qualification fixture; not biological evidence',
                'fixture_counts_sha256':digest(directory/'counts.tsv'),'fixture_samples_sha256':digest(directory/'samples.tsv')}
        records.append(record)
        atomic_json(destination/'qualification.json',{'status':'complete' if response.returncode==0 else 'failed','cases':records,'script_sha256':digest(source),'scientific_result':False})
        if response.returncode:raise RuntimeError(f'{mode} native production R fixture failed; inspect {directory}/stderr.txt')
        audit=json.loads((directory/'output/rna_analysis_audit.json').read_text())
        if audit['independent_brains']!=len(rows) or (mode=='ant' and (audit['discovery_brains']!=48 or audit['heldout_brains']!=12)):
            raise ValueError('Native R qualification failed independent-cohort contract')
    return {'status':'complete','cases':records,'script_sha256':digest(source),'scientific_result':False,'fixture_directory':str(destination)}


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,required=True)
    print(json.dumps(run(parser.parse_args().root)))
