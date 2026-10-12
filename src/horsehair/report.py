"""Claim-to-result reporting and six figure contracts, conditional on real evidence."""
from pathlib import Path
import json
from .core import atomic_json,read_json


FIGURES=[
 ('01','Data architecture and alternative computations','source_audit'),
 ('02','Behavioural discrimination and identifiability','behaviour'),
 ('03','Independent neural intervention predictions','interventions'),
 ('04','Stage-specific molecular signals and gene prediction','molecular'),
 ('05','Agent redirection, competence and restoration','agents'),
 ('06','Generalization boundaries and supported conclusions','synthesis')]


def build_report(config,root):
    root=Path(root)
    output=root/'results/report'
    output.mkdir(parents=True,exist_ok=True)
    claims=[]
    for claim,test,unit in [('H1','Held-out gain versus transition model discrimination','experiment with nested animals'),('H2','Independent intervention-shape prediction','intervention-control contrast with shared-control covariance'),('H3','Frozen ant score predicts unique fly gene relevance','gene/orthogroup with clustered reagents'),('H4','Redirection with separately preserved competence and restoration','independent training seed with nested evaluation episodes')]:
        path=root/'results/inference'/f'{claim}.json'
        if path.exists():
            result=read_json(path)
            if result.get('claim_id')!=claim or result.get('independent_unit')!=unit:raise ValueError('Claim result/unit contract mismatch')
            claims.append({'claim_id':claim,'test':test,'independent_unit':unit,'status':'result_available','result':result})
        else:claims.append({'claim_id':claim,'test':test,'independent_unit':unit,'status':'pending','result':None})
    atomic_json(output/'claim_evidence.json',claims)
    lines=['claim_id\ttest\tindependent_unit\tstatus']
    lines += ['\t'.join(c[k] for k in ['claim_id','test','independent_unit','status']) for c in claims]
    (output/'claim_evidence.tsv').write_text('\n'.join(lines)+'\n',encoding='utf8')
    figures=[]
    for number,title,component in FIGURES:
        input_path=root/'results/figure_inputs'/f'{number}.json'
        figure={'id':number,'title':title,'component':component,'status':'pending','reason':'Designated scientific plotting input not available'}
        if input_path.exists():
            figure=render_figure(input_path,output,title,number)
        figures.append(figure)
    atomic_json(output/'figure_manifest.json',figures)
    report={'protocol_id':config['protocol_id'],'claims':claims,'figures':figures,'complete':all(c['status']=='result_available' for c in claims)}
    atomic_json(output/'report.json',report)
    text=['# Study execution and evidence report','',f'Protocol: {config["protocol_id"]}','', 'Each claim below refers to its designated comparison and independent unit. Unavailable results remain pending.','']
    text += [f'- {c["claim_id"]}: {c["status"]}; {c["test"]}.' for c in claims]
    (output/'report.md').write_text('\n'.join(text)+'\n',encoding='utf8')
    return report


def render_figure(input_path,output,title,number):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    specification=read_json(input_path)
    if specification.get('evidence_class') not in ['biological_reanalysis','computational_experiment','source_audit']:
        raise ValueError('Figure requires declared observed evidence class')
    source=specification.get('source_result_paths')
    if not source or not all(Path(p).is_file() for p in source):raise ValueError('Figure source results missing')
    panels=specification['panels']
    fig,axes=plt.subplots(1,len(panels),figsize=(4*len(panels),3.5),squeeze=False,layout='constrained')
    for ax,panel in zip(axes[0],panels):
        x=np.asarray(panel['x']);y=np.asarray(panel['y'],dtype=float)
        if len(x)!=len(y) or not np.isfinite(y).all():raise ValueError('Invalid plotted data')
        if panel.get('kind')=='interval':
            lo=np.asarray(panel['lower']);hi=np.asarray(panel['upper'])
            if np.any(lo>y) or np.any(hi<y):raise ValueError('Invalid uncertainty interval')
            ax.errorbar(x,y,yerr=[y-lo,hi-y],fmt='o',capsize=3)
        elif panel.get('kind')=='scatter':ax.scatter(x,y,s=18)
        else:ax.plot(x,y,'o-',lw=1)
        ax.set(title=panel['title'],xlabel=panel['xlabel'],ylabel=panel['ylabel'])
        ax.spines[['top','right']].set_visible(False)
    fig.suptitle(title)
    paths=[]
    for extension in ['png','pdf','svg']:
        path=output/f'figure_{number}.{extension}';fig.savefig(path,dpi=300);paths.append(str(path))
    plt.close(fig)
    atomic_json(output/f'figure_{number}.plotted_data.json',specification)
    (output/f'figure_{number}.caption.txt').write_text(specification['caption'],encoding='utf8')
    return {'id':number,'title':title,'status':'rendered; visual inspection pending','paths':paths,'evidence_class':specification['evidence_class']}

