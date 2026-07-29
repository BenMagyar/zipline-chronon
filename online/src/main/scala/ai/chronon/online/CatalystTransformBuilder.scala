package ai.chronon.online

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, GeneratedClass}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeSet,
  BindReferences,
  Expression,
  Generator,
  GenericInternalRow,
  JoinedRow,
  Nondeterministic,
  Predicate,
  UnsafeProjection
}
import org.apache.spark.sql.execution.{
  BufferedRowIterator,
  FilterExec,
  GenerateExec,
  InputAdapter,
  LocalTableScanExec,
  ProjectExec,
  RDDScanExec,
  WholeStageCodegenExec
}
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  InputStream,
  ObjectInputStream,
  ObjectOutputStream,
  ObjectStreamClass
}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object CatalystTransformBuilder {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  type Transform = InternalRow => Seq[InternalRow]

  /** A Transform is not thread safe - generated iterators, projections and predicates all carry mutable row
    * buffers. A TransformFactory is: plan traversal, java source generation and compilation happen once when
    * the factory is built, and each invocation only allocates the mutable per-instance state.
    */
  type TransformFactory = () => Transform

  private class ClassLoaderObjectInputStream(in: InputStream, loader: ClassLoader) extends ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try Class.forName(desc.getName, false, loader)
      catch { case _: ClassNotFoundException => super.resolveClass(desc) }
  }

  /** Both codegen paths end the same way: a compiled GeneratedClass plus a references array mints instances.
    * The class is shareable, the references are not - objects codegen puts in there are not required to be
    * thread safe (LegacySimpleTimestampFormatter wraps a SimpleDateFormat). Spark gets away with sharing
    * because every task deserializes its own copy of the plan closure, so each instance here does the same.
    * `rebuild` covers the case where references cannot be serialized.
    */
  private def generatedInstances[T](clazz: GeneratedClass, references: Array[Any], rebuild: () => T): () => T =
    if (references.isEmpty) { () =>
      clazz.generate(references).asInstanceOf[T] // nothing shared, nothing to race on
    } else {
      Try {
        val bytes = new ByteArrayOutputStream()
        val out = new ObjectOutputStream(bytes)
        out.writeObject(references)
        out.close()
        val serialized = bytes.toByteArray
        val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)

        def copyReferences(): Array[Any] = {
          val in = new ClassLoaderObjectInputStream(new ByteArrayInputStream(serialized), loader)
          try in.readObject().asInstanceOf[Array[Any]]
          finally in.close()
        }

        copyReferences() // fail here rather than at request time
        () => clazz.generate(copyReferences()).asInstanceOf[T]
      }.getOrElse {
        logger.warn(
          "References are not serializable, rebuilding this stage per instance: " +
            references.map(r => if (r == null) "null" else r.getClass.getName).mkString(", "))
        rebuild
      }
    }

  /** Copies field by field into a fresh row so a downstream stage cannot observe the generated code
    * reusing its output buffer for the next row.
    */
  private def copyRow(row: InternalRow, output: Seq[Attribute]): InternalRow = {
    val safeRow = new GenericInternalRow(output.size)
    output.indices.foreach { i =>
      try safeRow.update(i, row.get(i, output(i).dataType))
      catch {
        case e: Exception => logger.error(s"Error copying field ${output(i).name}: ${e.getMessage}")
      }
    }
    safeRow
  }

  private def chain(childFactory: TransformFactory, stageFactory: TransformFactory): TransformFactory =
    () => {
      val childTransformer = childFactory()
      val stageTransformer = stageFactory()
      row => childTransformer(row).flatMap(stageTransformer)
    }

  private class IteratorWrapper[T] extends Iterator[T] {
    def put(elem: T): Unit = elemArr.enqueue(elem)

    override def hasNext: Boolean = elemArr.nonEmpty

    override def next(): T = elemArr.dequeue()

    private val elemArr: mutable.Queue[T] = mutable.Queue.empty[T]
  }

  /** Recursively builds a factory of transformation chains from a SparkPlan
    */
  def buildTransformFactory(plan: org.apache.spark.sql.execution.SparkPlan): TransformFactory = {
    logger.info(s"Building transform chain for plan: ${plan.getClass.getSimpleName}")

    // Helper function to inspect plan structures
    def describePlan(plan: org.apache.spark.sql.execution.SparkPlan, depth: Int = 0): String = {
      val indent = "  " * depth
      val childrenDesc = plan.children.map(c => describePlan(c, depth + 1)).mkString("\n")
      s"${indent}${plan.getClass.getSimpleName}: ${plan.output.map(_.name).mkString(", ")}\n${childrenDesc}"
    }

    // Log detailed plan structure for complex plans
    if (plan.children.size > 1 || plan.isInstanceOf[WholeStageCodegenExec]) {
      logger.info(s"Detailed plan structure:\n${describePlan(plan)}")
    }

    plan match {
      case whc: WholeStageCodegenExec =>
        logger.info(s"WholeStageCodegenExec child plan: ${whc.child}")

        // Check for tooManyFields issue and emit a more helpful diagnostic
        if (WholeStageCodegenExec.isTooManyFields(SQLConf.get, whc.child.schema)) {
          logger.warn("WholeStageCodegenExec has too many fields which may lead to code generation issues")
          logger.warn(s"Schema has ${whc.child.schema.size} fields, max is ${SQLConf.get.wholeStageMaxNumFields}")
        }

        // First check if the WholeStageCodegenExec has InputAdapter in its plan tree
        // If so, we need to handle the stages separately
        if (containsInputAdapter(whc)) {
          logger.info("WholeStageCodegenExec contains InputAdapter nodes - processing as cascading stages")

          // Process the child plan, which will handle the InputAdapter recursively
          // This is the critical step that implements proper cascading codegen
          val childFactory = buildTransformFactory(whc.child)

          // Return the child transformer directly - the cascading will happen
          // through the InputAdapter case which will process the next stage
          childFactory
        } else {
          // If no InputAdapter is found, this is a single WholeStageCodegenExec
          // that we can process with the extracted code
          try {
            logger.info("Processing WholeStageCodegenExec as a single stage")
            codegenStageFactory(whc)
          } catch {
            case e: Exception =>
              // If codegen fails, fall back to processing the child plans without codegen
              logger.warn(s"Failed to use WholeStageCodegenExec, falling back to child plan execution: ${e.getMessage}")
              logger.info("Building transform chain for child plan instead")

              // Recursively build a transform chain from the child plans
              buildTransformFactory(whc.child)
          }
        }

      case project: ProjectExec =>
        logger.info(s"Processing ProjectExec with expressions: ${project.projectList}")

        project.child match {
          // Special handling for direct RDD scans - no need to process through child
          case _: RDDScanExec | _: LocalTableScanExec =>
            // When the child is a simple scan, we can directly apply the projection
            projectFactory(project)

          // Special handling when child is InputAdapter
          case _: InputAdapter =>
            logger.info("ProjectExec has an InputAdapter child - using special handling")

            val childFactory = buildTransformFactory(project.child)
            val projectionFactory =
              generatedInstanceFactory(() => UnsafeProjection.create(project.projectList, project.child.output))

            () => {
              val childTransformer = childFactory()
              val proj = projectionFactory()
              // project each generated row independently, copying it out of the projection's reused buffer
              row => childTransformer(row).map(childRow => copyRow(proj(childRow), project.output))
            }

          // Special handling for WholeStageCodegenExec child - we need to be careful about schema alignment
          case whc: WholeStageCodegenExec =>
            try {
              // Try to use both the WholeStageCodegenExec and then the projection
              chain(buildTransformFactory(whc), projectFactory(project))
            } catch {
              case e: Exception =>
                logger.error(s"Error processing ProjectExec with WholeStageCodegenExec child: ", e)
                throw e
            }

          case _ =>
            // For complex children, we need to chain the transformations
            chain(buildTransformFactory(project.child), projectFactory(project))
        }

      case filter: FilterExec =>
        logger.info(s"Processing FilterExec with condition: ${filter.condition}")

        // For a filter, first process the child and then apply filter
        chain(buildTransformFactory(filter.child), predicateFactory(filter))

      case input: InputAdapter =>
        logger.info(
          s"Processing InputAdapter with child: ${input.child.getClass.getSimpleName}. " +
            s"This is a split point between codegen stages")

        // InputAdapter is a boundary between codegen regions
        // We need to recursively process its child, which might be another WholeStageCodegenExec
        val childFactory = buildTransformFactory(input.child)

        // Special handling when the child is a GenerateExec
        if (input.child.isInstanceOf[GenerateExec]) {
          logger.info("InputAdapter has a GenerateExec child - using special handling to ensure row memory isolation")

          () => {
            val childTransformer = childFactory()
            row => childTransformer(row).map(copyRow(_, input.output))
          }
        } else {
          // Standard handling for other cases
          childFactory
        }

      case ltse: LocalTableScanExec =>
        logger.info(s"Processing LocalTableScanExec with schema: ${ltse.schema}")

        // Input row is unused for LocalTableScanExec
        () => _ => ArrayBuffer(ltse.executeCollect(): _*).toSeq

      case rddse: RDDScanExec =>
        logger.info(s"Processing RDDScanExec with schema: ${rddse.schema}")

        val scanProjectionFactory = generatedInstanceFactory(() => UnsafeProjection.create(rddse.schema))

        () => {
          val unsafeProjection = scanProjectionFactory()
          row => Seq(unsafeProjection.apply(row))
        }

      case generateExec: GenerateExec =>
        logger.info(s"Processing GenerateExec with generator: ${generateExec.generator}")

        val childFactory = buildTransformFactory(generateExec.child)
        val genFactory = generateFactory(generateExec)
        val generateOutput = generateExec.output

        () => {
          val childTransformer = childFactory()
          val generateTransformer = genFactory()
          // the generator reuses its row objects across calls, so each generated row is copied out
          row => childTransformer(row).flatMap(generateTransformer(_).map(copyRow(_, generateOutput)))
        }

      case unsupported =>
        logger.warn(s"Unrecognized plan node: ${unsupported.getClass.getName}")
        throw new RuntimeException(s"Unrecognized stage in codegen: ${unsupported.getClass}")
    }
  }

  /** Extracts a transformation function from WholeStageCodegenExec
    * This method only handles the code generation part - the fallback to
    * child plans is handled in buildTransformFactory
    *
    * Source generation and janino compilation happen once here. Per instance we only pay `clazz.generate`,
    * which mirrors what spark itself does across the tasks of a stage - the compiled class and the references
    * array are shared, the BufferedRowIterator is not.
    */
  private def codegenStageFactory(whc: WholeStageCodegenExec): TransformFactory = {
    logger.info(s"Extracting codegen stage transformer for: ${whc}")

    // Generate and compile the code
    val (ctx, cleanedSource) = whc.doCodeGen()

    // Log a snippet of the generated code for debugging
    val codeSnippet = cleanedSource.body.split("\n").take(20).mkString("\n")
    logger.debug(s"Generated code snippet: \n$codeSnippet\n...")

    val (clazz, compilationTime) = CodeGenerator.compile(cleanedSource)
    logger.info(s"Compiled code in ${compilationTime}ms")

    val newBuffer = generatedInstances[BufferedRowIterator](
      clazz,
      ctx.references.toArray,
      () => {
        val (freshCtx, freshSource) = whc.doCodeGen()
        CodeGenerator.compile(freshSource)._1.generate(freshCtx.references.toArray).asInstanceOf[BufferedRowIterator]
      }
    )

    () => {
      val buffer = newBuffer()
      val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
      buffer.init(0, Array(iteratorWrapper))

      def codegenFunc(row: InternalRow): Seq[InternalRow] = {
        iteratorWrapper.put(row)
        val result = ArrayBuffer.empty[InternalRow]
        while (buffer.hasNext) {
          result.append(buffer.next())
        }
        result.toSeq
      }

      codegenFunc
    }
  }

  /** Spark's generated projections and predicates are inner classes of a GeneratedClass whose public
    * generate(references) mints a fresh instance - the same primitive whole stage codegen uses. Reusing it
    * skips the java source generation that UnsafeProjection.create/Predicate.create would redo per instance
    * (6ms vs 0.7us for a 30 expression projection). Falls back to rebuilding when the object is not codegen
    * backed, which happens on spark's interpreted fallback path.
    */
  private def generatedInstanceFactory[T <: AnyRef](build: () => T): () => T = {
    val prototype = build()

    val cloner = Try {
      def readField(name: String): Any = {
        val field = prototype.getClass.getDeclaredField(name)
        field.setAccessible(true)
        field.get(prototype)
      }

      val outer = readField("this$0").asInstanceOf[GeneratedClass]
      val references = readField("references").asInstanceOf[Array[Any]]
      val factory = generatedInstances[T](outer, references, build)
      require(factory().getClass == prototype.getClass, "clone produced a different class")

      factory
    }

    cloner.getOrElse {
      logger.info(s"${prototype.getClass.getName} is not codegen backed, rebuilding it per instance")
      build
    }
  }

  private def projectFactory(project: ProjectExec): TransformFactory = {
    // Use project.child.output as input schema instead of project.output
    // This ensures expressions like int32s#8 can be properly resolved
    val projectionFactory =
      generatedInstanceFactory(() => UnsafeProjection.create(project.projectList, project.child.output))

    () => {
      val unsafeProjection = projectionFactory()
      row => Seq(unsafeProjection.apply(row))
    }
  }

  private def predicateFactory(filter: FilterExec): TransformFactory = {
    val builtPredicateFactory = generatedInstanceFactory(() => Predicate.create(filter.condition, filter.child.output))

    () => {
      val predicate = builtPredicateFactory()
      predicate.initialize(0)
      val func = { row: InternalRow =>
        val passed = predicate.eval(row)
        if (passed) Seq(row) else Seq.empty
      }
      func
    }
  }

  private def generateFactory(generate: GenerateExec): TransformFactory = {
    logger.info(s"Extracting transformer for GenerateExec with generator: ${generate.generator}")

    val needsPruning = generate.child.outputSet != AttributeSet(generate.requiredChildOutput)

    // bound generators can be Nondeterministic - their rng state cannot be shared across instances. Binding
    // is cheap, no codegen is involved.
    () => {
      // Create a bound generator
      val boundGenerator = BindReferences
        .bindReference(
          generate.generator.asInstanceOf[Expression],
          generate.child.output
        )
        .asInstanceOf[Generator]

      // Initialize any nondeterministic expressions
      boundGenerator match {
        case n: Nondeterministic => n.initialize(0)
        case _                   => // No initialization needed
      }

      // Create a null row for outer join case
      val generatorNullRow = new GenericInternalRow(boundGenerator.elementSchema.length)

      lazy val pruneChildForResult: InternalRow => InternalRow = if (needsPruning) {
        UnsafeProjection.create(generate.requiredChildOutput, generate.child.output)
      } else {
        identity
      }

      // Return the transformer function
      row => {
        try {
          if (generate.requiredChildOutput.nonEmpty) {
            extractGenerateNonEmptyChildren(generate, boundGenerator, generatorNullRow, row, pruneChildForResult)
          } else {
            extractGenerateEmptyChildren(generate, boundGenerator, generatorNullRow, row)
          }
        } catch {
          case e: Exception =>
            logger.error(s"Error evaluating generator: ${e.getMessage}", e)
            throw e
        }
      }
    }
  }

  // No required child outputs, simpler case
  private def extractGenerateEmptyChildren(generate: GenerateExec,
                                           boundGenerator: Generator,
                                           generatorNullRow: GenericInternalRow,
                                           row: InternalRow) = {
    val generatedRows = boundGenerator.eval(row)

    if (generate.outer && generatedRows.isEmpty) {
      // Return a single null row for outer case
      Seq(generatorNullRow)
    } else {
      // Use the generated rows directly
      generatedRows.toSeq
    }
  }

  // If there are required child outputs, we need to join them with generated values
  private def extractGenerateNonEmptyChildren(generate: GenerateExec,
                                              boundGenerator: Generator,
                                              generatorNullRow: GenericInternalRow,
                                              row: InternalRow,
                                              pruneChildForResult: InternalRow => InternalRow) = {
    // Prune the child row if needed
    val prunedChildRow = pruneChildForResult(row)

    // Evaluate the generator against the input row
    val generatedRows = boundGenerator.eval(row)

    // handle the outer case if no rows were generated
    if (generate.outer && generatedRows.isEmpty) {
      val joined = new JoinedRow(prunedChildRow, generatorNullRow)
      Seq(joined)
    } else {
      val results = new ArrayBuffer[InternalRow](generatedRows.size)

      for (generatedRow <- generatedRows) {
        // Use JoinedRow to handle type conversions properly
        val joined = new JoinedRow(prunedChildRow, generatedRow)
        results += joined
      }

      results.toSeq
    }
  }

  /** Helper method to check if a plan tree contains any InputAdapter nodes
    * which indicate split points for WholeStageCodegenExec
    */
  private def containsInputAdapter(plan: org.apache.spark.sql.execution.SparkPlan): Boolean = {
    if (plan.isInstanceOf[InputAdapter]) {
      return true
    }
    plan.children.exists(containsInputAdapter)
  }

}
