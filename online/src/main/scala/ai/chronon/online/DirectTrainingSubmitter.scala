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
}
