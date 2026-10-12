"""Validated claim decisions, immutable plotting inputs and paginated figures."""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np

from .core import atomic_json, read_json, canonical_hash, digest, source_identity

FIGURES = [("01", "Data architecture and alternative computations", "source_audit"),
           ("02", "Behavioural discrimination and identifiability", "behaviour"),
           ("03", "Independent neural intervention predictions", "interventions"),
           ("04", "Stage-specific molecular signals and gene prediction", "molecular"),
           ("05", "Agent redirection, competence and restoration", "agents"),
           ("06", "Generalization boundaries and supported conclusions", "synthesis")]
CLAIMS = [("H1", "Held-out gain versus transition model discrimination", "experiment with nested animals"),
          ("H2", "Independent intervention-shape prediction", "intervention-control contrast with shared-control covariance"),
          ("H3", "Frozen ant score predicts unique fly gene relevance", "gene/orthogroup with clustered reagents"),
          ("H4", "Redirection with separately preserved competence and restoration", "independent training seed with nested evaluation episodes")]


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _source_sha():
    return source_identity(Path(__file__).resolve().parents[2])["sha256"]


def verify_observed_inputs(record):
    paths = record.get("source_result_paths", [])
    hashes = record.get("source_result_hashes", {})
    if not isinstance(paths, list) or not isinstance(hashes, dict) or not hashes:
        raise ValueError("Scientific inputs require recorded SHA-256 hashes")
    if len(set(paths)) != len(paths) or not set(paths) <= set(hashes):
        raise ValueError("Every declared scientific input must have its recorded hash")
    for path, expected in hashes.items():
        if not isinstance(path, str) or not Path(path).is_absolute() or not Path(path).is_file():
            raise ValueError("Scientific source path is missing or is not absolute")
        if not isinstance(expected, str) or len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected.lower()) or digest(Path(path)) != expected:
            raise ValueError("Scientific source digest mismatch")
    return hashes


def _operational_gates(result, claim, config):
    """Compute criteria; an asserted operational_gates_passed flag is insufficient."""
    missing = []
    if claim == "H1":
        for key in ("parameter_recovery_passed", "parameter_identifiability_passed"):
            if not isinstance(result.get(key), bool):missing.append(key)
        diagnostic = result.get("multivariate_diagnostic_improvement")
        if not _finite(diagnostic):missing.append("multivariate_diagnostic_improvement")
        effect = result["estimate"] > 0 and result["ci95"][0] > 0
        passed = effect and not missing and result["parameter_recovery_passed"] and result["parameter_identifiability_passed"] and diagnostic >= .05
    elif claim == "H2":
        for key in ("family_generalization_passed", "shared_control_covariance_preserved"):
            if not isinstance(result.get(key), bool):missing.append(key)
        families = result.get("intervention_families_evaluated")
        if not isinstance(families, int) or isinstance(families, bool) or families < 1:missing.append("intervention_families_evaluated")
        effect = result["estimate"] >= .1 and result["ci95"][0] > 0
        passed = effect and not missing and result["family_generalization_passed"] and result["shared_control_covariance_preserved"] and families >= 2
    else:
        threshold = float(config.get("molecular", {}).get("incremental_rank_effect_threshold", .2))
        effect = result["estimate"] >= threshold and result["ci95"][0] > 0
        passed = effect
    return {"passed": bool(passed), "missing": missing, "effect_criterion_passed": bool(effect)}


def _validate_h4(result, config, root):
    from . import agents, agent_analysis
    settings = agent_analysis._settings(config)
    protocol_path = agents._directory(Path(root), settings)/"frozen_protocol.json"
    protocol = read_json(protocol_path)
    if protocol.get("full_config_sha256") != canonical_hash(config) or protocol.get("H4_inference_specification") != agents.h4_specification(config):
        raise ValueError("H4 analysis choices differ from the frozen full configuration")
    if result.get("frozen_protocol_sha256") != agents._digest(protocol) or protocol.get("implementation_sha256") != agents._implementation_sha256():
        raise ValueError("H4 frozen protocol/source identity mismatch")
    if result.get("status") != "complete" or result.get("computational_analysis_complete") is not True:
        raise ValueError("H4 receipt does not establish a complete computational analysis")
    cells = agents._cells(settings)
    if result.get("planned_policy_cells") != len(cells) or result.get("independent_seed_count_per_cell") != len(settings.get("final_seeds", range(1000, 1020))):
        raise ValueError("H4 independent-policy roster differs from the declared design")
    curve_path = result.get("response_curves")
    if curve_path not in result["source_result_hashes"]:raise ValueError("H4 response curves require a recorded input hash")
    curves = read_json(Path(curve_path))
    if not isinstance(curves, list):raise ValueError("Invalid H4 response-curve table")
    architecture_tasks = {(architecture, task) for task, architecture, _, _ in cells}
    expected = {(*pair, *condition) for pair in architecture_tasks for condition in agent_analysis._conditions(settings, protocol)}
    observed = [tuple(row.get(key) for key in agent_analysis.KEYS) for row in curves]
    if len(observed) != len(set(observed)) or set(observed) != expected:raise ValueError("H4 response curves omit or duplicate a frozen condition")
    n = len(settings.get("final_seeds", range(1000, 1020)))
    primary = []
    for row in curves:
        if row.get("independent_seeds") != n or not isinstance(row.get("available_for_H4"), bool):raise ValueError("H4 curve has invalid independent-unit/qualification metadata")
        if not isinstance(row.get("baseline_qualified"),bool) or not isinstance(row.get("strength_matched"),bool):raise ValueError("H4 calibration/competence qualification flags are missing")
        for outcome in agent_analysis.OUTCOMES:
            item = row.get("outcomes", {}).get(outcome, {})
            interval = item.get("ci95", [])
            if len(interval) != 2 or not all(_finite(x) for x in [item.get("estimate"), *interval]) or interval[0] > interval[1]:raise ValueError("Invalid H4 component estimate or interval")
        declared_primary = row.get("perturbation") in agent_analysis.PRIMARY
        if row.get("primary_grid_point") is not declared_primary:raise ValueError("H4 primary multiplicity roster was changed")
        if declared_primary:
            if not _finite(row.get("raw_joint_pvalue")) or not 0 <= row["raw_joint_pvalue"] <= 1:raise ValueError("Invalid H4 joint P value")
            component = row.get("component_pvalues",{})
            comparators = row.get("comparators",{})
            if set(component)!=set(agent_analysis.OUTCOMES) or set(comparators)!={"random_subspace","output_gain"}:raise ValueError("H4 joint endpoint/comparator roster is incomplete")
            pvalues = list(component.values())
            if any(not _finite(p) or not 0<=p<=1 for p in pvalues):raise ValueError("Invalid H4 component P value")
            matched = True
            for comparator in comparators.values():
                if comparator.get("independent_seed_pairs")!=n or not isinstance(comparator.get("strength_matched"),bool) or not _finite(comparator.get("pvalue")) or not 0<=comparator["pvalue"]<=1:raise ValueError("Invalid paired H4 comparator metadata")
                pvalues.append(comparator["pvalue"]);matched &= comparator["strength_matched"]
            if abs(row["raw_joint_pvalue"]-max(pvalues))>1e-10:raise ValueError("H4 joint test must use the maximum endpoint/comparator P value")
            available = bool(row["baseline_qualified"] and row["strength_matched"] and matched)
            if row["available_for_H4"] is not available:raise ValueError("H4 availability ignores failed competence or calibration")
            primary.append(row)
    from .inference import holm
    adjusted = holm([r["raw_joint_pvalue"] if r["available_for_H4"] else 1.0 for r in primary])
    alpha = float(config["inference"]["alpha"])
    for row, value in zip(primary, adjusted):
        if not _finite(row.get("holm_pvalue")) or abs(row["holm_pvalue"]-value) > 1e-10:raise ValueError("H4 Holm adjustment does not match its complete grid")
        expected_support = bool(row["available_for_H4"] and value < alpha)
        if row.get("operating_region_supported") is not expected_support:raise ValueError("H4 support flag contradicts adjusted evidence")
    correction = result.get("correction", {})
    if correction.get("method") != "Holm" or correction.get("family") != "H4 prespecified computational grid" or correction.get("tests") != len(primary) or correction.get("alpha") != alpha:raise ValueError("H4 computational multiplicity contract mismatch")
    biological = bool(protocol.get("biological_directions"))
    if biological:raise ValueError("Synthetic primary results cannot activate a biological bridge")
    qualified = result.get("all_baseline_competence_qualified")
    if not isinstance(qualified, bool) or result.get("biological_constraints_available") is not biological:raise ValueError("H4 biological/competence qualification metadata mismatch")
    if qualified is not all(row["baseline_qualified"] for row in curves):raise ValueError("H4 overall qualification flag contradicts its policy grid")
    supported = [r for r in primary if r["operating_region_supported"]]
    bio_supported = [r for r in supported if r["rule_type"] == "biological"]
    if result.get("supported_grid_points") != len(supported) or result.get("biologically_constrained_supported_grid_points") != len(bio_supported):raise ValueError("H4 supported-point count mismatch")
    synthetic_supported = [r for r in supported if r["rule_type"]=="synthetic"]
    status = "supported" if qualified and synthetic_supported else "not_supported" if qualified else "pending"
    if result.get("primary_evidence_class")!="computational_experiment" or result.get("biological_transfer_status")!="pending":raise ValueError("H4 primary evidence must remain computational with a separate pending biological bridge")
    if result.get("claim_status") != status or result.get("headline_claim_accepted") is not (status == "supported") or result.get("computational_claim_accepted") is not (status=="supported"):raise ValueError("H4 claim is unsupported by competence/operating-region evidence")
    candidates = [r["holm_pvalue"] for r in primary if r["rule_type"] == ("biological" if biological else "synthetic")]
    if abs(result["pvalue"]-min(candidates)) > 1e-10:raise ValueError("H4 omnibus P value differs from its corrected grid")


def validate_result(result, claim, unit, config, root):
    if not isinstance(result, dict) or result.get("claim_id") != claim or result.get("independent_unit") != unit:raise ValueError("Claim result/unit contract mismatch")
    if result.get("evidence_status") == "synthetic_fixture":raise ValueError("A synthetic fixture cannot establish a study claim")
    expected = "computational_experiment" if claim == "H4" else "biological_reanalysis"
    if result.get("evidence_class") != expected:raise ValueError("Scientific evidence class missing or incorrect")
    if result.get("config_sha256") != canonical_hash(config) or result.get("source_sha256") != _source_sha():raise ValueError("Scientific configuration or implementation changed")
    ci = result.get("ci95")
    if not isinstance(ci, list) or len(ci) != 2 or not all(_finite(x) for x in [result.get("estimate"), result.get("pvalue"), *ci]) or not 0 <= result["pvalue"] <= 1 or ci[0] > ci[1]:raise ValueError("Invalid scientific estimate, interval or P value")
    verify_observed_inputs(result)
    if claim != "H4":
        manifest = Path(root)/"manifests/source_manifest.json"
        if not manifest.is_file() or result.get("data_manifest_sha256") != digest(manifest):raise ValueError("Biological source manifest mismatch")
    else:_validate_h4(result, config, root)


def _synthesis_input(config, root, claims):
    """Four separate estimands: no shared axis, pooled unit or invented result."""
    if any(c["result"] is None for c in claims):return
    labels = {"H1":"Predictive log-score difference", "H2":"Relative withheld-error reduction",
              "H3":"Incremental rank association", "H4":"Reference redirection probability change"}
    paths, panels, components = [], [], []
    for claim in claims:
        result = claim["result"]
        path = Path(root)/"results/inference"/f"{claim['claim_id']}.json"
        paths.append(str(path))
        components.append({"claim_id":claim["claim_id"],"evidence_class":result["evidence_class"],
                           "independent_unit":claim["independent_unit"],"source_result_path":str(path),
                           "source_result_sha256":digest(path),"decision":claim["status"]})
        panels.append({"kind":"interval","title":f"{claim['claim_id']}: {claim['status']}","x":[0],
                       "y":[result["estimate"]],"lower":[result["ci95"][0]],"upper":[result["ci95"][1]],
                       "xlabel":result["evidence_class"].replace("_"," "),"ylabel":labels[claim["claim_id"]],
                       "horizontal_reference":0.})
    specification = {"evidence_class":"mixed_study_evidence","config_sha256":canonical_hash(config),"source_sha256":_source_sha(),
        "source_result_paths":paths,"source_result_hashes":{p:digest(p) for p in paths},"evidence_components":components,
        "layout":{"columns":2,"max_panels_per_page":4},"panels":panels,
        "caption":"Four actual study estimands and their own 95% intervals, displayed on separate axes without pooling units or effect scales. H1–H3 are biological reanalyses with experiment, intervention-contrast and gene/orthogroup units respectively; H4 is a computational experiment with independently trained seeds and nested episodes. H4's estimate/interval describe its prespecified reference point; its omnibus multiplicity-adjusted P value tests existence of a joint operating point across the frozen grid, not that reference effect. Decisions use the accompanying corrected claim table. Unsupported hypotheses and pending diagnostics/constraints remain explicit; computational restoration does not establish an insect mechanism."}
    atomic_json(Path(root)/"results/figure_inputs/06.json",specification)


def _visual_review(figure, output):
    path = Path(output)/f"figure_{figure['id']}.visual_review.json"
    if not path.exists():return False
    record = read_json(path)
    pngs = {p:sha for p,sha in figure.get("rendered_file_hashes",{}).items() if Path(p).suffix==".png"}
    return bool(record.get("status")=="verified" and record.get("reviewer") and
                record.get("figure_input_sha256")==figure.get("input_sha256") and record.get("image_sha256")==pngs and pngs)


def build_report(config, root):
    root = Path(root)
    output = root/"results/report"
    output.mkdir(parents=True, exist_ok=True)
    claims = []
    for claim, test, unit in CLAIMS:
        path = root/"results/inference"/f"{claim}.json"
        result = read_json(path) if path.exists() else None
        if result is not None:validate_result(result, claim, unit, config, root)
        claims.append({"claim_id":claim, "test":test, "independent_unit":unit, "status":"analysis_available" if result else "pending", "result":result})
    if config.get("inference", {}).get("primary_family") != ["H1", "H2", "H3"]:raise ValueError("The primary statistical family must be the declared H1/H2/H3 family")
    family = claims[:3]
    family_complete = all(c["result"] is not None for c in family)
    if family_complete:
        from .inference import holm
        adjusted = holm([c["result"]["pvalue"] for c in family])
        for claim, value in zip(family, adjusted):
            gates = _operational_gates(claim["result"], claim["claim_id"], config)
            claim["primary_family_holm_pvalue"] = float(value)
            claim["operational_gates_passed"] = gates["passed"]
            claim["required_diagnostics_missing"] = gates["missing"]
            if value >= float(config["inference"]["alpha"]) or not gates["effect_criterion_passed"]:claim["status"] = "not_supported"
            elif gates["missing"]:claim["status"] = "required_diagnostics_pending"
            else:claim["status"] = "supported" if gates["passed"] else "not_supported"
    else:
        for claim in family:
            if claim["result"] is not None:claim["status"] = "joint_primary_family_pending"
    if claims[3]["result"] is not None:claims[3]["status"] = claims[3]["result"]["claim_status"]
    _synthesis_input(config,root,claims)
    atomic_json(output/"claim_evidence.json", claims)
    (output/"claim_evidence.tsv").write_text("claim_id\ttest\tindependent_unit\tstatus\n"+"\n".join("\t".join(c[k] for k in ("claim_id","test","independent_unit","status")) for c in claims)+"\n", encoding="utf-8")
    figures = []
    for number, title, component in FIGURES:
        path = root/"results/figure_inputs"/f"{number}.json"
        figure = {"id":number,"title":title,"component":component,"status":"pending","reason":"Designated scientific plotting input is unavailable"}
        if path.exists() and (number!="06" or all(c["result"] is not None for c in claims)):
            figure = render_figure(path, output, title, number, config=config)
            figure["visual_inspection_verified"] = _visual_review(figure,output)
        figures.append(figure)
    atomic_json(output/"figure_manifest.json", figures)
    analysis_complete = family_complete and all(c["status"] in ("supported","not_supported") for c in claims)
    figures_complete = all(f.get("paths") for f in figures)
    visual_complete = figures_complete and all(f.get("visual_inspection_verified") for f in figures)
    report = {"protocol_id":config["protocol_id"],"config_sha256":canonical_hash(config),"claims":claims,"figures":figures,
        "primary_family":{"tests":["H1","H2","H3"],"method":"Holm","status":"computed" if family_complete else "pending"},
        "analyses_available":all(c["result"] is not None for c in claims),
        "analysis_complete":analysis_complete,"figures_complete":bool(figures_complete),"visual_inspection_complete":bool(visual_complete),
        "complete":bool(analysis_complete and figures_complete),"publication_ready":bool(analysis_complete and visual_complete),
        "interpretation":"Analysis completion, hypothesis support and publication readiness are separate. Unsupported means that acceptance criteria were not met, not proof of no effect. Negative results can complete an analysis; missing diagnostics, constraints and visual checks remain pending."}
    atomic_json(output/"report.json", report)
    text = ["# Study execution and evidence report","",f"Protocol: {config['protocol_id']}","",
            "Each claim uses its designated comparison and independent unit. Result availability alone does not establish support.",""]
    text += [f"- {c['claim_id']}: {c['status']}; {c['test']}." for c in claims]
    (output/"report.md").write_text("\n".join(text)+"\n",encoding="utf-8")
    return report


def render_figure(input_path, output, title, number, *, config=None, allow_fixture=False):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    specification = read_json(input_path)
    fixture = specification.get("evidence_status") == "synthetic_fixture"
    if fixture and not allow_fixture:raise ValueError("Synthetic QA figures cannot be rendered as observed study results")
    if specification.get("evidence_class") not in ("biological_reanalysis","computational_experiment","source_audit","mixed_study_evidence"):raise ValueError("Figure requires a declared observed evidence class")
    if not fixture:
        if config is None or specification.get("config_sha256") != canonical_hash(config) or specification.get("source_sha256") != _source_sha():raise ValueError("Figure input requires the matching scientific configuration/source")
    verify_observed_inputs(specification)
    if specification.get("evidence_class")=="mixed_study_evidence":
        components = specification.get("evidence_components",[])
        expected_components = {c:u for c,_,u in CLAIMS}
        if len(components)!=4 or {c.get("claim_id") for c in components}!=set(expected_components):raise ValueError("Mixed figure requires all four separate evidence components")
        for component in components:
            expected_class = "computational_experiment" if component["claim_id"]=="H4" else "biological_reanalysis"
            path = component.get("source_result_path")
            if component.get("evidence_class")!=expected_class or component.get("independent_unit")!=expected_components[component["claim_id"]] or specification["source_result_hashes"].get(path)!=component.get("source_result_sha256"):
                raise ValueError("Mixed figure component loses its unit/class/source identity")
    panels = specification.get("panels")
    if not isinstance(panels,list) or not panels or not isinstance(specification.get("caption"),str):raise ValueError("Missing figure panels/caption")
    layout = specification.get("layout",{})
    per_page = min(12,int(layout.get("max_panels_per_page",specification.get("panels_per_page",12))))
    columns = min(3,int(layout.get("columns",3)))
    if per_page < 1 or columns < 1:raise ValueError("Invalid figure pagination")
    output = Path(output)
    output.mkdir(parents=True,exist_ok=True)
    paths = []
    for start in range(0,len(panels),per_page):
        subset = panels[start:start+per_page]
        cols = min(columns,len(subset));rows = (len(subset)+cols-1)//cols
        fig,axes = plt.subplots(rows,cols,figsize=(4.3*cols,3.4*rows),squeeze=False,layout="constrained")
        try:
            for ax,panel in zip(axes.flat,subset):
                series = panel.get("series",[panel])
                if not isinstance(series,list) or not series:raise ValueError("Empty figure series")
                for item in series:
                    x,y = np.asarray(item.get("x")),np.asarray(item.get("y"),dtype=float)
                    if x.ndim != 1 or y.ndim != 1 or not len(x) or len(x) != len(y) or not np.isfinite(y).all():raise ValueError("Invalid plotted data")
                    if np.issubdtype(x.dtype,np.number):
                        if not np.isfinite(x).all():raise ValueError("Nonfinite plot coordinate")
                    elif not all(isinstance(v,str) and v.strip() for v in x.tolist()):raise ValueError("Invalid categorical plot coordinate")
                    label = item.get("label");kind = item.get("kind",panel.get("kind"))
                    line, = ax.plot(x,y,"o" if kind=="scatter" else "o-",label=label,markersize=3,lw=1)
                    if ("lower" in item) != ("upper" in item):raise ValueError("Incomplete uncertainty interval")
                    if "lower" in item:
                        lo,hi = np.asarray(item["lower"],dtype=float),np.asarray(item["upper"],dtype=float)
                        if lo.shape != y.shape or hi.shape != y.shape or not np.isfinite([lo,hi]).all() or np.any(lo>hi):raise ValueError("Invalid uncertainty interval")
                        # Percentile intervals need not contain their point. Draw
                        # the actual interval, never clamp it to the estimate.
                        ax.errorbar(x,(lo+hi)/2,yerr=(hi-lo)/2,fmt="none",ecolor=line.get_color(),capsize=2,lw=.8)
                    for field,marker in (("baseline_qualified","x"),("strength_matched","s")):
                        if field in item:
                            flags = np.asarray(item[field])
                            if flags.shape != y.shape or any(not isinstance(v,(bool,np.bool_)) for v in flags):raise ValueError("Invalid figure qualification flags")
                            failed = ~flags
                            if failed.any():ax.plot(x[failed],y[failed],marker,ls="none",mfc="none",mec=line.get_color(),ms=7)
                if len(series)>1:ax.legend(fontsize=6)
                ax.set(title=panel["title"],xlabel=panel["xlabel"],ylabel=panel["ylabel"]);ax.title.set_fontsize(8)
                if "horizontal_reference" in panel:
                    if not _finite(panel["horizontal_reference"]):raise ValueError("Invalid horizontal reference")
                    ax.axhline(panel["horizontal_reference"],ls="--",lw=.8,color="gray")
                ax.spines[["top","right"]].set_visible(False)
            for ax in list(axes.flat)[len(subset):]:ax.set_visible(False)
            page = start//per_page+1
            fig.suptitle(("SYNTHETIC QA FIXTURE — " if fixture else "")+title+(f" — page {page}" if len(panels)>per_page else ""))
            suffix = f"_page{page:02d}" if len(panels)>per_page else ""
            for extension in ("png","pdf","svg"):
                path = output/f"figure_{number}{suffix}.{extension}";fig.savefig(path,dpi=300);paths.append(str(path))
        finally:plt.close(fig)
    atomic_json(output/f"figure_{number}.plotted_data.json",specification)
    caption = specification["caption"]
    if any(any("baseline_qualified" in item or "strength_matched" in item for item in panel.get("series",[panel])) for panel in panels):
        caption += " Crosses mark unqualified baseline competence; open squares mark an unmatched strength target."
    (output/f"figure_{number}.caption.txt").write_text(caption,encoding="utf-8")
    return {"id":number,"title":title,"status":"rendered; visual inspection pending","paths":paths,
            "pages":math.ceil(len(panels)/per_page),"evidence_class":specification["evidence_class"],
            "evidence_status":"synthetic_fixture" if fixture else "observed_results","input_sha256":digest(input_path),
            "source_result_hashes":specification["source_result_hashes"],"rendered_file_hashes":{p:digest(p) for p in paths}}
