"""Utilities for Zipline admin CLI health checks."""

import json
import logging
import shutil
import subprocess
from functools import partial
from urllib.parse import urlparse

from rich.table import Table

from ai.chronon.cli.theme import console

logger = logging.getLogger(__name__)

_KUBECTL_TIMEOUT = 10  # seconds


def _run_kubectl(args, timeout=_KUBECTL_TIMEOUT, context=None):
    """Run a kubectl subcommand with a timeout.

    Returns a CompletedProcess-like object; on TimeoutExpired, logs a warning
    and returns a namespace with returncode=1 and empty stdout/stderr so
    callers that check returncode behave correctly without special-casing.

    When `context` is set, `--context <name>` is prepended so the call targets
    that kubeconfig context instead of the ambient active one. Callers that
    want deterministic env routing should pass it; legacy callers can leave it
    None to preserve the old behavior.
    """
    cmd = ["kubectl"]
    if context:
        cmd.extend(["--context", context])
    cmd.extend(args)
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("kubectl %s timed out after %ss", " ".join(args), timeout)

        class _TimedOut:
            returncode = 1
            stdout = ""
            stderr = f"timed out after {timeout}s"

        return _TimedOut()


def run_infra_checks(cloud="aws", kube_context=None):
    """Run Kubernetes infrastructure checks for the Flink streaming setup.

    Returns a list of (check_name, what, status, detail) tuples. Preserved for
    the `streaming-health` command; composes the shared preconditions with the
    streaming + hub-url check groups so its output is unchanged.

    When `kube_context` is set, every kubectl call targets that context — used
    by the env-aware streaming-health command so we deterministically check
    the prod or canary cluster rather than the ambient active one.
    """
    kctl = partial(_run_kubectl, context=kube_context)
    results, reachable = _check_preconditions(kctl)
    if not reachable:
        return results
    results.extend(_run_streaming_checks(kctl))
    results.extend(_run_hub_url_checks(kctl))
    if cloud == "azure":
        results.extend(_run_azure_infra_checks(kube_context=kube_context))
    return results


def _check_preconditions(kctl):
    """kubectl present + API reachable. Returns (results, reachable); when
    reachable is False the caller should stop (nothing else can run)."""
    results = []
    if not shutil.which("kubectl"):
        results.append(
            (
                "kubectl",
                "kubectl binary present",
                "FAIL",
                "not found — install it and configure kubeconfig",
            )
        )
        return results, False
    results.append(("kubectl", "kubectl binary present", "ok", ""))

    r = kctl(["cluster-info"])
    if r.returncode != 0:
        results.append(
            (
                "cluster",
                "Can reach the Kubernetes API",
                "FAIL",
                r.stderr.strip() or "kubectl cluster-info failed",
            )
        )
        return results, False
    results.append(("cluster", "Can reach the Kubernetes API", "ok", ""))
    return results, True


def _run_streaming_checks(kctl):
    """Flink streaming infra: namespaces, RBAC, service accounts, operator CRD."""
    results = []

    for ns in ("zipline-flink", "zipline-system"):
        r = kctl(["get", "namespace", ns])
        if r.returncode != 0:
            results.append(
                (
                    ns,
                    f"{ns} namespace exists",
                    "FAIL",
                    "not found — Terraform may not have been applied",
                )
            )
        else:
            results.append((ns, f"{ns} namespace exists", "ok", ""))

    r = kctl(["get", "role", "orchestration-flink-role", "-n", "zipline-flink"])
    if r.returncode != 0:
        results.append(
            (
                "orchestration-flink-role",
                "Flink RBAC role exists",
                "FAIL",
                "role missing — Terraform may not have been applied",
            )
        )
    else:
        results.append(("orchestration-flink-role", "Flink RBAC role exists", "ok", ""))

    r = kctl(
        [
            "auth",
            "can-i",
            "create",
            "flinkdeployments.flink.apache.org",
            "--namespace",
            "zipline-flink",
            "--as",
            "system:serviceaccount:zipline-system:orchestration-sa",
        ]
    )
    if r.returncode != 0 or r.stdout.strip() != "yes":
        results.append(
            (
                "FlinkDeployment RBAC",
                "orchestration-sa can submit Flink jobs",
                "FAIL",
                "orchestration-sa lacks create permission on flinkdeployments",
            )
        )
    else:
        results.append(("FlinkDeployment RBAC", "orchestration-sa can submit Flink jobs", "ok", ""))

    r = kctl(
        [
            "auth",
            "can-i",
            "create",
            "ingresses.networking.k8s.io",
            "--namespace",
            "zipline-flink",
            "--as",
            "system:serviceaccount:zipline-system:orchestration-sa",
        ]
    )
    if r.returncode != 0 or r.stdout.strip() != "yes":
        results.append(
            (
                "ingress RBAC",
                "orchestration-sa can manage Flink ingresses",
                "FAIL",
                "orchestration-sa lacks ingress permissions — check orchestration-flink-role",
            )
        )
    else:
        results.append(("ingress RBAC", "orchestration-sa can manage Flink ingresses", "ok", ""))

    r = kctl(["get", "serviceaccount", "zipline-flink-sa", "-n", "zipline-flink"])
    if r.returncode != 0:
        results.append(
            (
                "zipline-flink-sa",
                "Flink job service account exists",
                "FAIL",
                "SA missing — Terraform may not have been applied",
            )
        )
    else:
        results.append(("zipline-flink-sa", "Flink job service account exists", "ok", ""))

    r = kctl(["get", "crd", "flinkdeployments.flink.apache.org"])
    if r.returncode != 0:
        results.append(
            (
                "FlinkDeployment CRD",
                "Flink operator CRD installed",
                "FAIL",
                "CRD missing — flink-operator Helm chart or CRD manifest not applied",
            )
        )
    else:
        results.append(("FlinkDeployment CRD", "Flink operator CRD installed", "ok", ""))

    return results


def _run_hub_url_checks(kctl):
    """Hub base URL wiring: env var set, ingress host match, domain-less ELB job."""
    results = []

    hub_base_url = None
    r = kctl(
        [
            "get",
            "deployment",
            "zipline-orchestration-hub",
            "-n",
            "zipline-system",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ]
    )
    if r.returncode != 0:
        results.append(
            (
                "HUB_BASE_URL",
                "Hub base URL is configured",
                "FAIL",
                "could not read hub deployment env vars",
            )
        )
    else:
        try:
            env_vars = json.loads(r.stdout)
            hub_base_url = next(
                (e.get("value") for e in env_vars if e.get("name") == "HUB_BASE_URL" and e.get("value") is not None),
                None,
            )
            if hub_base_url:
                results.append(("HUB_BASE_URL", "Hub base URL is configured", "ok", hub_base_url))
            else:
                results.append(
                    (
                        "HUB_BASE_URL",
                        "Hub base URL is configured",
                        "FAIL",
                        "not set — check hub_domain/hub_external_url vars or set-hub-base-url Job",
                    )
                )
        except (json.JSONDecodeError, StopIteration):
            results.append(
                (
                    "HUB_BASE_URL",
                    "Hub base URL is configured",
                    "FAIL",
                    "could not parse deployment env vars",
                )
            )

    if hub_base_url:
        hub_hostname = urlparse(hub_base_url).hostname or ""
        r = kctl(
            [
                "get",
                "ingress",
                "orchestration-hub-ingress",
                "-n",
                "zipline-system",
                "-o",
                "jsonpath={.spec.rules[0].host}",
            ]
        )
        if r.returncode != 0:
            results.append(
                (
                    "hub ingress",
                    "Hub ingress host matches HUB_BASE_URL",
                    "WARN",
                    "could not read ingress — may not exist yet",
                )
            )
        else:
            ingress_host = r.stdout.strip()
            if ingress_host == hub_hostname:
                results.append(
                    (
                        "hub ingress",
                        "Hub ingress host matches HUB_BASE_URL",
                        "ok",
                        ingress_host,
                    )
                )
            else:
                results.append(
                    (
                        "hub ingress",
                        "Hub ingress host matches HUB_BASE_URL",
                        "WARN",
                        f"ingress host {ingress_host!r} != HUB_BASE_URL hostname {hub_hostname!r}",
                    )
                )

        is_elb = ".elb." in hub_base_url and ".amazonaws.com" in hub_base_url
        if is_elb:
            r = kctl(
                [
                    "get",
                    "events",
                    "-n",
                    "zipline-system",
                    "--field-selector",
                    "reason=Completed",
                ]
            )
            if r.returncode == 0 and "set-hub-base-url" in r.stdout:
                results.append(
                    (
                        "set-hub-base-url",
                        "ELB hostname discovery job completed",
                        "ok",
                        "completed successfully",
                    )
                )
            else:
                results.append(
                    (
                        "set-hub-base-url",
                        "ELB hostname discovery job completed",
                        "WARN",
                        "no completion event — check: kubectl logs -n zipline-system -l job-name=set-hub-base-url",
                    )
                )

    return results


def _run_azure_infra_checks(kube_context=None):
    """Run Azure-specific checks for Flink-on-AKS with Workload Identity.

    Returns a list of (check_name, what, status, detail) tuples.
    """
    results = []
    kctl = partial(_run_kubectl, context=kube_context)

    # Namespace label: tells the WI mutating webhook to activate for pods in this namespace.
    # Without it, the webhook skips the namespace entirely — SA annotation and client-id are irrelevant.
    r = kctl(
        [
            "get",
            "namespace",
            "zipline-flink",
            "-o",
            "jsonpath={.metadata.labels.azure\\.workload\\.identity/use}",
        ]
    )
    if r.returncode != 0 or r.stdout.strip() != "true":
        results.append(
            (
                "WI namespace label",
                "zipline-flink has azure.workload.identity/use=true",
                "FAIL",
                "label missing — token injection webhook won't activate for Flink pods",
            )
        )
    else:
        results.append(
            ("WI namespace label", "zipline-flink has azure.workload.identity/use=true", "ok", "")
        )

    # SA annotation: binds the SA to an Azure managed identity (client-id).
    # Without it, the webhook won't inject the federated token volume — Flink pods can't authenticate to ABFS or Key Vault.
    r = kctl(
        [
            "get",
            "serviceaccount",
            "zipline-flink-sa",
            "-n",
            "zipline-flink",
            "-o",
            "jsonpath={.metadata.annotations.azure\\.workload\\.identity/client-id}",
        ]
    )
    client_id = r.stdout.strip() if r.returncode == 0 else ""
    if not client_id:
        results.append(
            (
                "WI SA annotation",
                "zipline-flink-sa has azure.workload.identity/client-id",
                "FAIL",
                "annotation missing — Flink pods won't get Azure tokens for ABFS access",
            )
        )
    else:
        results.append(
            (
                "WI SA annotation",
                "zipline-flink-sa has azure.workload.identity/client-id",
                "ok",
                client_id,
            )
        )

    # Flink operator: reconciles FlinkDeployment CRs into JM/TM pods.
    # Without a running operator, submitted jobs will be accepted by the API but never materialize.
    r = kctl(
        [
            "get",
            "deployment",
            "flink-kubernetes-operator",
            "-n",
            "flink-operator",
            "-o",
            "jsonpath={.status.availableReplicas}",
        ]
    )
    available = r.stdout.strip() if r.returncode == 0 else ""
    if not available or available == "0":
        results.append(
            (
                "Flink operator",
                "flink-kubernetes-operator deployment is available",
                "FAIL",
                "no available replicas — check: kubectl get pods -n flink-operator",
            )
        )
    else:
        results.append(
            (
                "Flink operator",
                "flink-kubernetes-operator deployment is available",
                "ok",
                f"availableReplicas={available}",
            )
        )

    # Hub env vars: AksFlinkSubmitter reads these to configure the WI identity and target namespace for submitted jobs.
    # Missing values mean jobs are submitted with the wrong (or no) identity, causing silent ABFS/Event Hubs auth failures.
    r = kctl(
        [
            "get",
            "deployment",
            "zipline-orchestration-hub",
            "-n",
            "zipline-system",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ]
    )
    if r.returncode != 0:
        for check, what in [
            ("Flink Azure env vars", "FLINK_AZURE_CLIENT_ID and FLINK_AZURE_TENANT_ID set on hub"),
            ("Flink AKS env vars", "FLINK_AKS_SERVICE_ACCOUNT and FLINK_AKS_NAMESPACE set on hub"),
        ]:
            results.append((check, what, "FAIL", "could not read hub deployment env vars"))
    else:
        try:
            env_vars = json.loads(r.stdout)
            env_map = {e.get("name"): e.get("value") for e in env_vars if e.get("name")}

            azure_client_id = env_map.get("FLINK_AZURE_CLIENT_ID", "")
            azure_tenant_id = env_map.get("FLINK_AZURE_TENANT_ID", "")
            if azure_client_id and azure_tenant_id:
                results.append(
                    (
                        "Flink Azure env vars",
                        "FLINK_AZURE_CLIENT_ID and FLINK_AZURE_TENANT_ID set on hub",
                        "ok",
                        f"client_id={azure_client_id}",
                    )
                )
            else:
                missing = ", ".join(
                    v for v in ["FLINK_AZURE_CLIENT_ID", "FLINK_AZURE_TENANT_ID"] if not env_map.get(v)
                )
                results.append(
                    (
                        "Flink Azure env vars",
                        "FLINK_AZURE_CLIENT_ID and FLINK_AZURE_TENANT_ID set on hub",
                        "FAIL",
                        f"missing: {missing} — check helm values flink.azureClientId / flink.azureTenantId",
                    )
                )

            sa = env_map.get("FLINK_AKS_SERVICE_ACCOUNT", "")
            ns = env_map.get("FLINK_AKS_NAMESPACE", "")
            if sa and ns:
                results.append(
                    (
                        "Flink AKS env vars",
                        "FLINK_AKS_SERVICE_ACCOUNT and FLINK_AKS_NAMESPACE set on hub",
                        "ok",
                        f"sa={sa}, ns={ns}",
                    )
                )
            else:
                missing = ", ".join(
                    v for v in ["FLINK_AKS_SERVICE_ACCOUNT", "FLINK_AKS_NAMESPACE"] if not env_map.get(v)
                )
                results.append(
                    (
                        "Flink AKS env vars",
                        "FLINK_AKS_SERVICE_ACCOUNT and FLINK_AKS_NAMESPACE set on hub",
                        "FAIL",
                        f"missing: {missing} — check helm values flink.aksServiceAccount / flink.aksNamespace",
                    )
                )
        except (json.JSONDecodeError, AttributeError):
            for check, what in [
                ("Flink Azure env vars", "FLINK_AZURE_CLIENT_ID and FLINK_AZURE_TENANT_ID set on hub"),
                ("Flink AKS env vars", "FLINK_AKS_SERVICE_ACCOUNT and FLINK_AKS_NAMESPACE set on hub"),
            ]:
                results.append((check, what, "FAIL", "could not parse hub deployment env vars"))

    # WI webhook: the mutating webhook that injects the federated token volume into pods at admission time.
    # Without it, namespace label and SA annotation are both no-ops — pods never receive an Azure token.
    # AKS names this differently depending on installation method:
    #   - Helm/standalone: azure-workload-identity-webhook
    #   - AKS managed add-on: azure-wi-webhook-mutating-webhook-configuration
    _WI_WEBHOOK_NAMES = [
        "azure-wi-webhook-mutating-webhook-configuration",
        "azure-workload-identity-webhook",
    ]
    wi_webhook_found = None
    for webhook_name in _WI_WEBHOOK_NAMES:
        r = kctl(["get", "mutatingwebhookconfiguration", webhook_name])
        if r.returncode == 0:
            wi_webhook_found = webhook_name
            break
    if wi_webhook_found:
        results.append(
            ("WI webhook", "Azure Workload Identity webhook is installed", "ok", wi_webhook_found)
        )
    else:
        results.append(
            (
                "WI webhook",
                "Azure Workload Identity webhook is installed",
                "FAIL",
                "webhook missing — token injection won't work; check AKS workload identity add-on",
            )
        )

    return results


def _hub_env_map(kctl):
    """Return the hub container's env as {name: value}, or None if unreadable."""
    r = kctl(
        [
            "get",
            "deployment",
            "zipline-orchestration-hub",
            "-n",
            "zipline-system",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ]
    )
    if r.returncode != 0:
        return None
    try:
        return {e.get("name"): e.get("value") for e in json.loads(r.stdout) if e.get("name")}
    except (json.JSONDecodeError, AttributeError):
        return None


def _run_network_checks(kctl):
    """Cluster/network health: nodes Ready, DNS up, no image-pull failures, hub up.

    Image-pull failures in zipline-system are the observable symptom of the two
    connectivity breaks we hit in practice: an expired Docker Hub pull token and
    loss of node egress to the registry.
    """
    results = []

    r = kctl(["get", "nodes", "-o", "jsonpath={range .items[*]}{.metadata.name}={.status.conditions[?(@.type=='Ready')].status};{end}"])
    if r.returncode != 0:
        results.append(("nodes", "All nodes are Ready", "FAIL", "could not list nodes"))
    else:
        not_ready = [p.split("=")[0] for p in r.stdout.strip(";").split(";") if p and not p.endswith("=True")]
        if not_ready:
            results.append(("nodes", "All nodes are Ready", "WARN", f"NotReady: {', '.join(not_ready)}"))
        else:
            results.append(("nodes", "All nodes are Ready", "ok", ""))

    r = kctl(["get", "deployment", "coredns", "-n", "kube-system", "-o", "jsonpath={.status.availableReplicas}"])
    avail = r.stdout.strip() if r.returncode == 0 else ""
    if not avail or avail == "0":
        results.append(("coredns", "Cluster DNS is available", "FAIL", "no available coredns replicas — in-cluster DNS/egress broken"))
    else:
        results.append(("coredns", "Cluster DNS is available", "ok", f"availableReplicas={avail}"))

    # Waiting reasons across zipline-system: ImagePullBackOff/ErrImagePull point at
    # a bad/expired image-pull secret or lost registry egress.
    r = kctl(["get", "pods", "-n", "zipline-system", "-o", "jsonpath={range .items[*]}{.metadata.name}={.status.containerStatuses[*].state.waiting.reason};{end}"])
    if r.returncode == 0:
        bad = [p for p in r.stdout.strip(";").split(";") if p and ("ImagePullBackOff" in p or "ErrImagePull" in p)]
        if bad:
            results.append(("image pulls", "No image-pull failures in zipline-system", "FAIL", f"{'; '.join(bad)} — check image-pull secret / registry egress"))
        else:
            results.append(("image pulls", "No image-pull failures in zipline-system", "ok", ""))

    r = kctl(["get", "deployment", "zipline-orchestration-hub", "-n", "zipline-system", "-o", "jsonpath={.status.availableReplicas}"])
    avail = r.stdout.strip() if r.returncode == 0 else ""
    if not avail or avail == "0":
        results.append(("hub", "Hub deployment is available", "FAIL", "no available hub replicas — check: kubectl get pods -n zipline-system"))
    else:
        results.append(("hub", "Hub deployment is available", "ok", f"availableReplicas={avail}"))

    return results


def _run_karpenter_checks(kctl):
    """Karpenter provisioning stack: controller, NodePools, EC2NodeClass readiness."""
    results = []

    r = kctl(["get", "deployment", "karpenter", "-n", "kube-system", "-o", "jsonpath={.status.availableReplicas}"])
    avail = r.stdout.strip() if r.returncode == 0 else ""
    if not avail or avail == "0":
        results.append(("karpenter", "Karpenter controller is available", "FAIL", "no available replicas — node provisioning is down"))
    else:
        results.append(("karpenter", "Karpenter controller is available", "ok", f"availableReplicas={avail}"))
        # Chart runs 2 replicas + a PDB so a drain never leaves 0 controllers.
        if avail.isdigit() and int(avail) < 2:
            results.append(("karpenter HA", "Karpenter runs >=2 replicas", "WARN", f"only {avail} replica — a node drain can stall all provisioning"))

    r = kctl(["get", "nodepools.karpenter.sh", "-o", "jsonpath={range .items[*]}{.metadata.name};{end}"])
    if r.returncode != 0:
        results.append(("NodePools", "Karpenter NodePools exist", "FAIL", "could not list nodepools — CRD missing or Karpenter not installed"))
    else:
        pools = [p for p in r.stdout.strip(";").split(";") if p]
        if not pools:
            results.append(("NodePools", "Karpenter NodePools exist", "FAIL", "no NodePools — nothing can be provisioned"))
        else:
            results.append(("NodePools", "Karpenter NodePools exist", "ok", ", ".join(pools)))

    r = kctl(["get", "ec2nodeclasses.karpenter.k8s.aws", "-o", "jsonpath={range .items[*]}{.metadata.name}={.status.conditions[?(@.type=='Ready')].status};{end}"])
    if r.returncode != 0:
        results.append(("EC2NodeClass", "EC2NodeClass is Ready", "FAIL", "could not list ec2nodeclasses"))
    else:
        entries = [p for p in r.stdout.strip(";").split(";") if p]
        not_ready = [e.split("=")[0] for e in entries if not e.endswith("=True")]
        if not entries:
            results.append(("EC2NodeClass", "EC2NodeClass is Ready", "FAIL", "none defined — NodePools reference a missing EC2NodeClass"))
        elif not_ready:
            results.append(("EC2NodeClass", "EC2NodeClass is Ready", "FAIL", f"not Ready: {', '.join(not_ready)} — check subnet/AMI/role resolution"))
        else:
            results.append(("EC2NodeClass", "EC2NodeClass is Ready", "ok", ""))

    return results


def _run_autoscaling_checks(kctl):
    """Autoscaling can actually schedule: NodePool requirements + no stuck NodeClaims.

    A NodeClaim stuck with Launched=False is the observable signature of the
    missing EC2 Spot service-linked role (ServiceLinkedRoleCreationNotPermitted)
    and of instance-type requirements that filter out every offering.
    """
    results = []

    r = kctl(["get", "nodepools.karpenter.sh", "-o", "json"])
    if r.returncode != 0:
        results.append(("NodePool requirements", "NodePools have instance requirements", "WARN", "could not read nodepools"))
    else:
        try:
            items = json.loads(r.stdout).get("items", [])
            empty = [
                it.get("metadata", {}).get("name", "?")
                for it in items
                if not it.get("spec", {}).get("template", {}).get("spec", {}).get("requirements")
            ]
            if not items:
                results.append(("NodePool requirements", "NodePools have instance requirements", "WARN", "no NodePools to inspect"))
            elif empty:
                results.append(("NodePool requirements", "NodePools have instance requirements", "FAIL", f"empty requirements: {', '.join(empty)} — Karpenter will filter out all instance types"))
            else:
                results.append(("NodePool requirements", "NodePools have instance requirements", "ok", f"{len(items)} nodepool(s)"))
        except (json.JSONDecodeError, AttributeError):
            results.append(("NodePool requirements", "NodePools have instance requirements", "WARN", "could not parse nodepools"))

    r = kctl(["get", "nodeclaims.karpenter.sh", "-o", "jsonpath={range .items[*]}{.metadata.name}={.status.conditions[?(@.type=='Launched')].status};{end}"])
    if r.returncode == 0:
        entries = [p for p in r.stdout.strip(";").split(";") if p]
        stuck = [e.split("=")[0] for e in entries if e.endswith("=False")]
        if stuck:
            results.append(("NodeClaims", "No NodeClaims stuck unlaunched", "FAIL", f"not Launched: {', '.join(stuck)} — check EC2 Spot service-linked role / instance-type availability"))
        else:
            results.append(("NodeClaims", "No NodeClaims stuck unlaunched", "ok", f"{len(entries)} claim(s)"))

    return results


def _run_url_checks(kctl):
    """Spark UI / History Server reachability wiring (domain-less installs).

    Guards the finished-job History Server 404 and the SHS_NOT_CONFIGURED gaps:
    the SHS ingress host must match the hub host, and the hub must carry
    CRUCIBLE_SPARK_HISTORY_PUBLIC_URL for its Spark-UI proxy.
    """
    results = []

    r = kctl(["get", "deployment", "spark-history-server", "-n", "zipline-system", "-o", "jsonpath={.status.availableReplicas}"])
    avail = r.stdout.strip() if r.returncode == 0 else ""
    if not avail or avail == "0":
        results.append(("Spark History Server", "spark-history-server is available", "WARN", "no available replicas — finished-job Spark UIs won't render"))
    else:
        results.append(("Spark History Server", "spark-history-server is available", "ok", f"availableReplicas={avail}"))

    hub_host = ""
    env_map = _hub_env_map(kctl)
    if env_map and env_map.get("HUB_BASE_URL"):
        hub_host = urlparse(env_map["HUB_BASE_URL"]).hostname or ""

    r = kctl(["get", "ingress", "orchestration-spark-history-ingress", "-n", "zipline-system", "-o", "jsonpath={.spec.rules[0].host}"])
    if r.returncode != 0:
        results.append(("SHS ingress", "Spark History ingress host matches hub", "WARN", "spark-history ingress not found"))
    else:
        shs_host = r.stdout.strip()
        if not shs_host:
            # Host-less ingress falls into nginx's default server, shadowed by the
            # hub/UI host block -> /spark-history 404s with the UI SPA.
            results.append(("SHS ingress", "Spark History ingress host matches hub", "WARN", "ingress has no host — /spark-history is shadowed by the hub host, serves UI 404"))
        elif hub_host and shs_host != hub_host:
            results.append(("SHS ingress", "Spark History ingress host matches hub", "WARN", f"host {shs_host!r} != hub host {hub_host!r}"))
        else:
            results.append(("SHS ingress", "Spark History ingress host matches hub", "ok", shs_host))

    if env_map is not None:
        if env_map.get("CRUCIBLE_SPARK_HISTORY_PUBLIC_URL"):
            results.append(("SHS public URL", "Hub has CRUCIBLE_SPARK_HISTORY_PUBLIC_URL", "ok", env_map["CRUCIBLE_SPARK_HISTORY_PUBLIC_URL"]))
        else:
            results.append(("SHS public URL", "Hub has CRUCIBLE_SPARK_HISTORY_PUBLIC_URL", "WARN", "unset — hub Spark-UI proxy returns SHS_NOT_CONFIGURED for finished jobs"))

    return results


def _run_observability_checks(kctl):
    """Logging + metrics wiring: promtail/loki present, metrics exporter configured."""
    results = []

    r = kctl(["get", "daemonset", "zipline-orchestration-promtail", "-n", "zipline-system", "-o", "jsonpath={.status.numberReady}"])
    ready = r.stdout.strip() if r.returncode == 0 else ""
    if r.returncode != 0 or ready == "" or ready == "0":
        results.append(("promtail", "Log shipping (promtail) is running", "WARN", "promtail daemonset absent/not ready — pod logs won't ship to Loki"))
    else:
        results.append(("promtail", "Log shipping (promtail) is running", "ok", f"numberReady={ready}"))

    r = kctl(["get", "service", "zipline-orchestration-loki", "-n", "zipline-system"])
    if r.returncode != 0:
        results.append(("loki", "Log store (loki) service exists", "WARN", "loki service absent — no in-cluster log store"))
    else:
        results.append(("loki", "Log store (loki) service exists", "ok", ""))

    # Metrics exporter is wired via a spark.driver.extraJavaOptions -Dai.chronon.metrics.*
    # env var on the hub; without it, backfill jobs emit no metrics.
    env_map = _hub_env_map(kctl)
    if env_map is None:
        results.append(("metrics", "Metrics exporter configured on hub", "WARN", "could not read hub env"))
    else:
        has_metrics = any("chronon.metrics" in (v or "") or "METRICS" in (k or "") for k, v in env_map.items())
        if has_metrics:
            results.append(("metrics", "Metrics exporter configured on hub", "ok", ""))
        else:
            results.append(("metrics", "Metrics exporter configured on hub", "WARN", "no metrics-exporter env on hub — jobs may emit no metrics"))

    return results


def _run_flink_checks(kctl):
    """K8s-native Flink streaming setup (crucible/in-cluster compute): operator,
    CRD, compute-namespace Flink SA, hub submit RBAC + env.

    Distinct from run_infra_checks/streaming-health, which target the
    EMR-serverless + Azure Flink setups (different namespace/SA/role names).
    """
    results = []

    r = kctl(["get", "deployment", "flink-kubernetes-operator", "-n", "flink-operator", "-o", "jsonpath={.status.availableReplicas}"])
    avail = r.stdout.strip() if r.returncode == 0 else ""
    if not avail or avail == "0":
        results.append(("Flink operator", "flink-kubernetes-operator is available", "FAIL", "no available replicas — FlinkDeployments won't reconcile; check: kubectl get pods -n flink-operator"))
    else:
        results.append(("Flink operator", "flink-kubernetes-operator is available", "ok", f"availableReplicas={avail}"))

    r = kctl(["get", "crd", "flinkdeployments.flink.apache.org"])
    if r.returncode != 0:
        results.append(("FlinkDeployment CRD", "flinkdeployments CRD installed", "FAIL", "CRD missing — flink operator not installed"))
    else:
        results.append(("FlinkDeployment CRD", "flinkdeployments CRD installed", "ok", ""))

    # Compute namespace(s) carry the label spark/flink operators select on. Discover
    # rather than hardcode `zipline-default` so custom compute namespaces still work.
    r = kctl(["get", "namespace", "-l", "zipline.ai/namespace-type=compute", "-o", "jsonpath={range .items[*]}{.metadata.name};{end}"])
    compute_ns = [n for n in r.stdout.strip(";").split(";") if n] if r.returncode == 0 else []
    if not compute_ns:
        results.append(("compute namespace", "compute namespace exists", "FAIL", "no namespace labelled zipline.ai/namespace-type=compute — chart not applied"))
        return results
    results.append(("compute namespace", "compute namespace exists", "ok", ", ".join(compute_ns)))
    ns = compute_ns[0]

    r = kctl(["get", "serviceaccount", "flink", "-n", ns])
    if r.returncode != 0:
        results.append(("flink SA", f"flink service account exists in {ns}", "FAIL", "SA missing — compute RBAC (compute.rbac.create) not applied"))
    else:
        results.append(("flink SA", f"flink service account exists in {ns}", "ok", ""))

    # The hub SA submits FlinkDeployments via the zipline-compute-submitter role.
    r = kctl(["auth", "can-i", "create", "flinkdeployments.flink.apache.org", "--namespace", ns, "--as", "system:serviceaccount:zipline-system:orchestration-sa"])
    if r.returncode != 0 or r.stdout.strip() != "yes":
        results.append(("Flink submit RBAC", "hub can submit FlinkDeployments", "FAIL", "orchestration-sa lacks create on flinkdeployments — check zipline-compute-submitter role/binding"))
    else:
        results.append(("Flink submit RBAC", "hub can submit FlinkDeployments", "ok", ""))

    env_map = _hub_env_map(kctl)
    if env_map is not None:
        sa = env_map.get("CRUCIBLE_FLINK_SERVICE_ACCOUNT", "")
        cns = env_map.get("CRUCIBLE_DEFAULT_NAMESPACE", "")
        if sa and cns:
            results.append(("Flink hub env", "hub has CRUCIBLE_FLINK_SERVICE_ACCOUNT + CRUCIBLE_DEFAULT_NAMESPACE", "ok", f"sa={sa}, ns={cns}"))
        else:
            missing = ", ".join(v for v in ["CRUCIBLE_FLINK_SERVICE_ACCOUNT", "CRUCIBLE_DEFAULT_NAMESPACE"] if not env_map.get(v))
            results.append(("Flink hub env", "hub has CRUCIBLE_FLINK_SERVICE_ACCOUNT + CRUCIBLE_DEFAULT_NAMESPACE", "WARN", f"missing: {missing} — check compute.flinkDefaults / compute.defaultNamespace"))

    return results


# Health-check groups selectable via `zipline admin doctor infra-health --<group>`.
# Each value takes the kctl partial and returns a list of result tuples. The
# `streaming` group here targets the K8s-native Flink setup (crucible/infratest);
# the EMR-serverless + Azure streaming checks live in run_infra_checks /
# streaming-health and are intentionally kept separate.
_HEALTH_GROUPS = {
    "network": _run_network_checks,
    "domain": _run_hub_url_checks,
    "karpenter": _run_karpenter_checks,
    "autoscaling": _run_autoscaling_checks,
    "streaming": _run_flink_checks,
    "urls": _run_url_checks,
    "observability": _run_observability_checks,
}


def run_health_checks(groups=None, cloud="aws", kube_context=None):
    """Run selected infra health-check groups (all when `groups` is falsy).

    Returns (check_name, what, status, detail) tuples. Preconditions (kubectl +
    API reachability) always run first; if unreachable, nothing else can.
    """
    kctl = partial(_run_kubectl, context=kube_context)
    results, reachable = _check_preconditions(kctl)
    if not reachable:
        return results

    selected = list(groups) if groups else list(_HEALTH_GROUPS)
    for name in selected:
        results.extend(_HEALTH_GROUPS[name](kctl))
    return results


def print_check_table(title, results):
    """Render a diagnostics results table and exit 1 if any check FAILed."""
    table = Table(title=title, expand=False)
    table.add_column("Check", style="cyan", no_wrap=True)
    table.add_column("What", style="white")
    table.add_column("", width=1)
    table.add_column("Detail", style="white")

    all_ok = True
    for check, what, status, detail in results:
        if status == "ok":
            status_cell = "[green]✓[/green]"
        elif status == "WARN":
            status_cell = "[yellow]⚠[/yellow]"
        else:
            status_cell = "[red]✗[/red]"
            all_ok = False
        table.add_row(check, what, status_cell, detail)

    console.print(table)

    if all_ok:
        console.print("\n[bold green]All checks passed.[/bold green]")
    else:
        console.print("\n[bold red]Some checks failed.[/bold red]")
        raise SystemExit(1)
