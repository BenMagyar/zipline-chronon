package ai.chronon.online

import ai.chronon.api.submission.JobSubmitter

/** Capability mixed into a [[JobSubmitter]] that can submit model training directly.
  *
  * Orchestration pattern-matches this on the submitter it already owns. Submitters
  * that do not mix it in keep the existing Spark ModelNodeRunner path. The self-type
  * avoids adding a training method to every JobSubmitter.
  */
trait DirectTrainingSubmitter { self: JobSubmitter =>

  /** Returns the job id once submission is accepted, not once training has finished. */
  def submitTraining(request: TrainingRequest): String

  /** Submits training with a submission identity when the caller has one.
    * The compatibility default preserves existing direct submitters; implementations
    * with an idempotent backing store should override this overload.
    */
  def submitTraining(request: TrainingRequest, idempotencyKey: Option[String]): String =
    submitTraining(request, idempotencyKey, Map.empty)

  /** [[envVars]] is the node's resolved team env. Implementations that submit
    * through Ray should collect `RAY_`-prefixed keys, strip the prefix, and
    * inject them onto head/worker pods. The default ignores them.
    */
  def submitTraining(request: TrainingRequest, idempotencyKey: Option[String], envVars: Map[String, String]): String =
    submitTraining(request)
}
