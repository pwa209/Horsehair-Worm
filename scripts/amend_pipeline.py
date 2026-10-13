"""Register a frozen, positively acknowledged scoped repair; never submits jobs."""
import argparse
import json
from horsehair.repair import register_amendment

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--descriptor", required=True)
    args = parser.parse_args()
    result = register_amendment(args.descriptor)
    print(json.dumps({key:result[key] for key in ("status", "amendment_id", "replaced_tasks", "amended_plan_sha256", "amended_ledger_sha256")}, sort_keys=True))
