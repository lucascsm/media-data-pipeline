"""Plan Cloud Run deployments offline; execute only with --apply."""
from __future__ import annotations

import argparse
import json
import re
import shlex
import shutil
import subprocess
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build_plan(config: dict, mode: str, start: str | None, end: str | None, channels: str):
    if mode not in {"daily", "reprocess", "reach"}:
        raise ValueError("Invalid run mode")
    selected = channels.split(",")
    if not selected or any(channel not in {"google", "meta", "tiktok", "kwai"} for channel in selected):
        raise ValueError("Invalid channels")
    project, region = config["project_id"], config["region"]
    name = config[f"{mode}_job"]
    sa = f'{config["service_account"]}@{project}.iam.gserviceaccount.com'
    image = f'{region}-docker.pkg.dev/{project}/{config["repository"]}/{config["image"]}:{config["image_tag"]}'
    module = "src.main" if mode == "daily" else "src.main_reprocess"
    python_args = ["python", "-u", "-m", module]
    if mode == "daily":
        python_args += ["--channels", channels]
    else:
        if not start or not end or date.fromisoformat(start) > date.fromisoformat(end):
            raise ValueError("Reprocess and reach modes need an inclusive start/end interval")
        python_args += ["--start_date", start, "--end_date", end, "--channels", channels]
        if mode == "reach":
            python_args.append("--reach-only")
        else:
            python_args += ["--google-block-days", "7", "--meta-block-days", "7", "--tiktok-block-days", "7",
                            "--blocks-per-batch", "5", "--batch-sleep-seconds", "150"]
    mount = config["gcs_mount"]
    command = f"mkdir -p {shlex.quote(mount + '/output/backup')} {shlex.quote(mount + '/output/logs')} && exec {shlex.join(python_args)}"
    required_files = {f"{channel}.yaml" for channel in selected} | {"accounts.yaml"}
    secret_refs = config["config_secrets"]
    secrets = ",".join(f"/app/config/{key}={secret_refs[key]}" for key in sorted(required_files))
    env = f'^|^GCS_MOUNT={mount}|CONFIG_DIR=/app/config|ACCOUNT_CATALOG_PATH=/app/config/accounts.yaml|PIPELINE_TIMEZONE={config["timezone"]}'
    common = [f"--image={image}", f"--region={region}", f'--memory={config["memory"]}', f'--cpu={config["cpu"]}',
              f'--task-timeout={config[mode + "_timeout"]}', "--max-retries=0", f"--service-account={sa}",
              f'--add-volume=name=gcs-data,type=cloud-storage,bucket={config["data_bucket"]}',
              f"--add-volume-mount=volume=gcs-data,mount-path={mount}", "--command=/bin/sh",
              f"--args=^|^-c|{command}", f"--set-env-vars={env}", f"--set-secrets={secrets}"]
    prefix = ["gcloud", f"--project={project}"]
    plan = {
        "build": prefix + ["builds", "submit", str(ROOT), f"--tag={image}"],
        "job_probe": prefix + ["run", "jobs", "describe", name, f"--region={region}", "--format=json"],
        "job_create": prefix + ["run", "jobs", "create", name] + common,
        "job_update": prefix + ["run", "jobs", "update", name, "--clear-volumes", "--clear-volume-mounts"] + common,
    }
    if mode == "daily":
        scheduler = config["scheduler"]
        scheduler_args = [f"--location={region}", f'--schedule={config["schedule"]}', f'--time-zone={config["timezone"]}',
                          f"--uri=https://run.googleapis.com/v2/projects/{project}/locations/{region}/jobs/{name}:run",
                          "--http-method=POST", f"--oauth-service-account-email={sa}",
                          "--oauth-token-scope=https://www.googleapis.com/auth/cloud-platform"]
        plan["scheduler_probe"] = prefix + ["scheduler", "jobs", "describe", scheduler, f"--location={region}", "--format=json"]
        plan["scheduler_create"] = prefix + ["scheduler", "jobs", "create", "http", scheduler] + scheduler_args
        plan["scheduler_update"] = prefix + ["scheduler", "jobs", "update", "http", scheduler] + scheduler_args
    return plan


def validate_live_config(config: dict, channels: str | None = None):
    selected = channels.split(",") if channels is not None else ["google", "meta", "tiktok", "kwai"]
    if not selected or any(channel not in {"google", "meta", "tiktok", "kwai"} for channel in selected):
        raise ValueError("Invalid channels")
    required_secrets = {f"{channel}.yaml" for channel in selected} | {"accounts.yaml"}
    required_values = [config[key] for key in ("project_id", "data_bucket", "region", "repository", "image",
                                             "image_tag", "service_account", "gcs_mount", "timezone")]
    required_values += [config["config_secrets"][name] for name in required_secrets]
    if any("REPLACE_" in str(value) for value in required_values):
        raise ValueError("Configure project, data bucket and secrets for the selected channels before applying")
    patterns = {
        "project_id": r"[a-z][a-z0-9-]{4,28}[a-z0-9]",
        "data_bucket": r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]",
        "service_account": r"[a-z][a-z0-9-]{4,28}[a-z0-9]",
        "gcs_mount": r"/[a-zA-Z0-9_/-]+",
    }
    for key in ("region", "repository", "image", "daily_job", "reprocess_job", "reach_job", "scheduler"):
        patterns[key] = r"[a-z][a-z0-9-]+"
    for key, pattern in patterns.items():
        if not re.fullmatch(pattern, config[key]):
            raise ValueError(f"Invalid configuration field: {key}")
    if "/../" in config["gcs_mount"] or config["gcs_mount"].endswith("/.."):
        raise ValueError("Invalid mount path")
    for key, reference in config["config_secrets"].items():
        if key not in {"accounts.yaml", "google.yaml", "meta.yaml", "tiktok.yaml", "kwai.yaml"}:
            raise ValueError("Unexpected configuration secret filename")
        if key in required_secrets and not re.fullmatch(r"[A-Za-z0-9_-]+:(latest|[0-9]+)", reference):
            raise ValueError("Invalid secret reference")
    if any(separator in config["timezone"] for separator in ("|", ",", "=")):
        raise ValueError("Invalid timezone")


def run_command(command):
    result = subprocess.run(command, check=False)
    if result.returncode:
        raise RuntimeError(f"Cloud command failed with exit code {result.returncode}")


def resource_exists(command):
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return True
    error = result.stderr.lower()
    if "not_found" in error or "not found" in error or "does not exist" in error:
        return False
    raise RuntimeError("Resource lookup failed; verify CLI authentication and permissions")


def initialize_resources(config):
    project, region = config["project_id"], config["region"]
    prefix = ["gcloud", f"--project={project}"]
    run_command(prefix + ["services", "enable", "run.googleapis.com", "artifactregistry.googleapis.com",
                         "cloudscheduler.googleapis.com", "iam.googleapis.com", "bigquery.googleapis.com",
                         "cloudbuild.googleapis.com", "secretmanager.googleapis.com", "storage.googleapis.com"])
    sa = f'{config["service_account"]}@{project}.iam.gserviceaccount.com'
    if not resource_exists(prefix + ["iam", "service-accounts", "describe", sa, "--format=json"]):
        run_command(prefix + ["iam", "service-accounts", "create", config["service_account"], "--display-name=Media pipeline job"])
    for role in ("roles/run.invoker", "roles/bigquery.dataEditor", "roles/bigquery.jobUser", "roles/secretmanager.secretAccessor"):
        run_command(prefix + ["projects", "add-iam-policy-binding", project, f"--member=serviceAccount:{sa}", f"--role={role}", "--quiet"])
    run_command(prefix + ["storage", "buckets", "add-iam-policy-binding", f'gs://{config["data_bucket"]}',
                          f"--member=serviceAccount:{sa}", "--role=roles/storage.objectUser"])
    if not resource_exists(prefix + ["artifacts", "repositories", "describe", config["repository"], f"--location={region}", "--format=json"]):
        run_command(prefix + ["artifacts", "repositories", "create", config["repository"], "--repository-format=docker", f"--location={region}"])


def prune_images(config: dict, keep_count: int):
    """Optional retention for this image only, following the original deployment flow."""
    if keep_count < 1:
        raise ValueError("Keep at least one image")
    prefix = ["gcloud", f'--project={config["project_id"]}']
    image_path = f'{config["region"]}-docker.pkg.dev/{config["project_id"]}/{config["repository"]}/{config["image"]}'
    result = subprocess.run(prefix + ["artifacts", "docker", "images", "list", image_path, "--include-tags",
                                      "--limit=999999", "--sort-by=~UPDATE_TIME", "--format=value(digest)"],
                            capture_output=True, text=True, check=True)
    digests = list(dict.fromkeys(value.strip() for value in result.stdout.splitlines() if value.strip()))
    for digest in digests[keep_count:]:
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            raise ValueError("Unexpected image digest")
        run_command(prefix + ["artifacts", "docker", "images", "delete", f"{image_path}@{digest}", "--delete-tags", "--quiet"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/cloud.example.json")
    parser.add_argument("--mode", choices=("daily", "reprocess", "reach"), default="daily")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--channels", default="google,meta,tiktok,kwai")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--initialize-resources", action="store_true")
    parser.add_argument("--prune-images", action="store_true")
    parser.add_argument("--keep-images", type=int, default=10)
    args = parser.parse_args()
    if args.keep_images < 1:
        parser.error("Keep at least one image")
    config = json.loads(args.config.read_text(encoding="utf-8-sig"))
    plan = build_plan(config, args.mode, args.start, args.end, args.channels)
    if not args.apply:
        print(json.dumps({"mode": "SIMULACAO", "initialize_resources": args.initialize_resources,
                          "prune_images": args.prune_images, "keep_images": args.keep_images, "plan": plan}, indent=2))
        return
    validate_live_config(config, channels=args.channels)
    if not shutil.which("gcloud"):
        raise RuntimeError("Install the Google Cloud CLI before applying")
    if args.initialize_resources:
        initialize_resources(config)
    run_command(plan["build"])
    run_command(plan["job_update"] if resource_exists(plan["job_probe"]) else plan["job_create"])
    if args.mode == "daily":
        run_command(plan["scheduler_update"] if resource_exists(plan["scheduler_probe"]) else plan["scheduler_create"])
    if args.prune_images:
        prune_images(config, args.keep_images)
    print("Deploy complete. Job execution and SQL publication are separate steps.")


if __name__ == "__main__":
    main()
