package ai.chronon.online.test

import ai.chronon.api.{Builders, MetaData, Model, Source, TimeUnit, TrainingSpec, Window}
import ai.chronon.online.TrainingRequest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TrainingRequestSpec extends AnyFlatSpec with Matchers {

  private val Date = "2026-08-20"

  private def trainingSource(table: String = "project.dataset.labels"): Source =
    Builders.Source.events(query = Builders.Query(), table = table)

  private def model(trainingSpec: TrainingSpec = null): Model = {
    val result = new Model().setMetaData(new MetaData().setName("models.test_ctr_model").setVersion("1.0"))
    if (trainingSpec != null) result.setTrainingConf(trainingSpec)
    result
  }

  private def trainingSpec(source: Source = trainingSource(), window: Window = new Window()
                             .setLength(3)
                             .setTimeUnit(TimeUnit.DAYS)): TrainingSpec =
    new TrainingSpec().setTrainingDataSource(source).setTrainingDataWindow(window)

  "TrainingRequest.fromModel" should "copy the configured training source" in {
    val source = trainingSource("project.dataset.other_labels")
    val request = TrainingRequest.fromModel(model(trainingSpec(source = source)), Date)

    request.trainingSource shouldBe source
  }

  it should "copy the configured training data window" in {
    val window = new Window().setLength(7).setTimeUnit(TimeUnit.DAYS)
    val request = TrainingRequest.fromModel(model(trainingSpec(window = window)), Date)

    request.window shouldBe window
  }

  it should "preserve the original model and the supplied date" in {
    val original = model(trainingSpec())
    val request = TrainingRequest.fromModel(original, Date)

    request.model should be theSameInstanceAs original
    request.date shouldBe Date
  }

  it should "fail when the model has no training configuration" in {
    val exception = intercept[IllegalArgumentException](TrainingRequest.fromModel(model(), Date))

    exception.getMessage should include("trainingConf")
    exception.getMessage should include("models.test_ctr_model")
  }

  it should "fail when the model is null" in {
    val exception = intercept[IllegalArgumentException](TrainingRequest.fromModel(null, Date))

    exception.getMessage should include("model is required")
  }

  it should "fail when the date is missing or empty" in {
    Seq(null, "").foreach { date =>
      val exception = intercept[IllegalArgumentException](TrainingRequest.fromModel(model(trainingSpec()), date))
      exception.getMessage should include("training date is required")
    }
  }
}
