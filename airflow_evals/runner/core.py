import json
import logging
import os
import statistics
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from agent_core.graph import run_agent_with_tracing

from airflow_evals.runner.config import EvaluationCaseResult, EvaluationConfig, EvaluationSummary
from airflow_evals.runner.regression import RegressionDetector, RegressionReport

# Optional MLflow import
try:
    import mlflow

    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False

# Configure logging
logger = logging.getLogger("eval_runner")


class EvaluationRunner:
    """Orchestrates the evaluation process."""

    def __init__(self, config: EvaluationConfig):
        """Initialize the runner with configuration."""
        self.config = config
        self.run_id = config.run_id or f"run_{int(time.time())}"
        self.artifact_dir = Path(config.output_dir) / self.run_id

        # Ensure artifact directory exists
        self.artifact_dir.mkdir(parents=True, exist_ok=True)

    def load_dataset(self) -> List[Dict[str, Any]]:
        """Load validation cases from the dataset file."""
        cases = []
        try:
            with open(self.config.dataset_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        cases.append(json.loads(line))
        except FileNotFoundError:
            logger.error(f"Dataset not found: {self.config.dataset_path}")
            raise

        if self.config.limit:
            cases = cases[: self.config.limit]

        return cases

    async def run_single_case(self, case: Dict[str, Any]) -> EvaluationCaseResult:
        """Run a single evaluation case against the agent."""
        case_id = case.get("id", str(uuid.uuid4()))
        question = case.get("question")
        expected_sql = case.get("sql")  # Assuming golden dataset has 'sql' field

        start_time = time.time()
        trace_id = str(uuid.uuid4())

        try:
            # Invoke the agent
            result = await run_agent_with_tracing(
                question=question,
                tenant_id=self.config.tenant_id,
                session_id=trace_id,
                # We could pass additional config here if agent supports it
            )

            end_time = time.time()
            latency_ms = (end_time - start_time) * 1000

            generated_sql = result.get("current_sql")
            error = result.get("error")

            # Determine status
            execution_status = "SUCCESS"
            if error:
                execution_status = "FAILURE"
            elif result.get("ambiguity_type"):
                execution_status = "CLARIFICATION_REQUIRED"

            # Basic correctness check (Exact Match for MVP)
            # In Phase 5 we can improve this to be execution-based
            is_correct = False
            if generated_sql and expected_sql:
                # Normalize whitespace for basic comparison
                is_correct = " ".join(generated_sql.split()) == " ".join(expected_sql.split())

            return EvaluationCaseResult(
                case_id=case_id,
                question=question,
                expected_sql=expected_sql,
                generated_sql=generated_sql,
                is_correct=is_correct,
                execution_status=execution_status,
                error=error,
                latency_ms=latency_ms,
                trace_id=trace_id,
            )

        except Exception as e:
            logger.exception(f"Error running case {case_id}")
            end_time = time.time()
            return EvaluationCaseResult(
                case_id=case_id,
                question=question,
                expected_sql=expected_sql,
                generated_sql=None,
                is_correct=False,
                execution_status="SYSTEM_ERROR",
                error=str(e),
                latency_ms=(end_time - start_time) * 1000,
                trace_id=trace_id,
            )

    async def run_evaluation(self) -> EvaluationSummary:
        """Execute the full evaluation suite."""
        logger.info(f"Starting evaluation run: {self.run_id}")
        cases = self.load_dataset()
        logger.info(f"Loaded {len(cases)} cases")

        results: List[EvaluationCaseResult] = []

        # Run cases (sequential for MVP, leverage concurrency later)
        # TODO: Use asyncio.gather logic if concurrency > 1
        for case in cases:
            result = await self.run_single_case(case)
            results.append(result)

            # Intermediate logging could go here
            logger.info(
                f"Case {result.case_id}: {result.execution_status} ({result.latency_ms:.2f}ms)"
            )

        # Compute metrics
        total_cases = len(results)
        # Note: successful_runs != correct answers. separate concept.

        correct_cases = len([r for r in results if r.is_correct])
        failed_cases = total_cases - correct_cases  # Definition of failure for accuracy purposes

        accuracy = (correct_cases / total_cases) if total_cases > 0 else 0.0

        latencies = [r.latency_ms for r in results]
        avg_latency = statistics.mean(latencies) if latencies else 0.0
        p95_latency = (
            statistics.quantiles(latencies, n=20)[-1] if len(latencies) >= 20 else avg_latency
        )

        summary = EvaluationSummary(
            run_id=self.run_id,
            config=self.config,
            total_cases=total_cases,
            successful_cases=correct_cases,  # Map successful to correctness for summary
            failed_cases=failed_cases,
            accuracy=accuracy,
            avg_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
        )

        self.persist_artifacts(results, summary)

        # Log to MLflow if available
        self.log_to_mlflow(summary, results)

        # Check Regression
        regression_report = self.compare_with_baseline(summary)
        if regression_report.is_regression:
            logger.warning(f"Regression detected: {regression_report.details}")
            # We don't fail here, we return the report or handle it?
            # The summary doesn't contain regression info.
            # We might want to persist the regression report too.
            self.persist_regression_report(regression_report)

        return summary

    def compare_with_baseline(self, summary: EvaluationSummary) -> RegressionReport:
        """Check for regression against baseline."""
        detector = RegressionDetector()  # Use defaults or config
        baseline = self.load_baseline()
        return detector.check_regression(summary, baseline)

    def load_baseline(self) -> Optional[EvaluationSummary]:
        """Load baseline summary.

        Strategy:
        1. Try to find 'baseline_summary.json' in current dir (injected by DAG).
        2. Or try to find 'latest' in MLflow (not implemented for MVP).
        """
        # MVP: Look for a specific file path defined in env or config
        baseline_path = os.getenv("EVAL_BASELINE_PATH")
        if baseline_path and os.path.exists(baseline_path):
            try:
                with open(baseline_path, "r") as f:
                    return EvaluationSummary.model_validate_json(f.read())
            except Exception as e:
                logger.warning(f"Failed to load baseline from {baseline_path}: {e}")

        return None

    def persist_regression_report(self, report: RegressionReport):
        """Save regression report."""
        report_path = self.artifact_dir / "regression_report.json"
        with open(report_path, "w") as f:
            f.write(report.model_dump_json(indent=2))

    def log_to_mlflow(self, summary: EvaluationSummary, results: List[EvaluationCaseResult]):
        """Log metrics and artifacts to MLflow."""
        if not MLFLOW_AVAILABLE:
            logger.warning("MLflow not available, skipping logging")
            return

        try:
            # Use existing tracking URI or default
            tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
            mlflow.set_tracking_uri(tracking_uri)

            # Set experiment
            experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "text2sql_evaluations")
            mlflow.set_experiment(experiment_name)

            with mlflow.start_run(run_name=self.run_id):
                # Log Params
                mlflow.log_params(
                    {
                        "dataset": self.config.dataset_path,
                        "concurrency": self.config.concurrency,
                        "tenant_id": self.config.tenant_id,
                        "seed": self.config.seed,
                    }
                )
                if self.config.git_sha:
                    mlflow.log_param("git_sha", self.config.git_sha)

                # Log Metrics
                mlflow.log_metrics(
                    {
                        "accuracy": summary.accuracy,
                        "avg_latency_ms": summary.avg_latency_ms,
                        "p95_latency_ms": summary.p95_latency_ms,
                        "total_cases": summary.total_cases,
                        "successful_cases": summary.successful_cases,
                        "failed_cases": summary.failed_cases,
                    }
                )

                # Log Artifacts
                # We log the specific files we generated in persist_artifacts
                mlflow.log_artifact(str(self.artifact_dir / "results.json"))
                mlflow.log_artifact(str(self.artifact_dir / "summary.json"))
                mlflow.log_artifact(str(self.artifact_dir / "cases.jsonl"))

                logger.info(f"Logged results to MLflow run: {mlflow.active_run().info.run_id}")

        except Exception as e:
            logger.error(f"Failed to log to MLflow: {e}")
            # Non-fatal error as per requirements

    def persist_artifacts(self, results: List[EvaluationCaseResult], summary: EvaluationSummary):
        """Write results to disk."""
        # 1. Detailed Results
        results_path = self.artifact_dir / "results.json"
        with open(results_path, "w") as f:
            f.write(json.dumps([r.model_dump() for r in results], indent=2))

        # 2. Summary
        summary_path = self.artifact_dir / "summary.json"
        with open(summary_path, "w") as f:
            f.write(summary.model_dump_json(indent=2))

        # 3. Cases (JSONL for easy viewing/loading by other tools)
        cases_path = self.artifact_dir / "cases.jsonl"
        with open(cases_path, "w") as f:
            for r in results:
                f.write(json.dumps(r.model_dump()) + "\n")

        logger.info(f"Artifacts persisted to {self.artifact_dir}")


async def run_evaluation(config: EvaluationConfig) -> EvaluationSummary:
    """Run evaluation based on configuration."""
    runner = EvaluationRunner(config)
    return await runner.run_evaluation()
