/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api

import ai.chronon.api.DataType.toTDataType
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.observability.DriftSpec

// mostly used by tests to define confs easily
object Builders {

  object Selects {
    def apply(clauses: String*): Map[String, String] = {
      clauses.map { col => col -> col }.toMap
    }

    def exprs(clauses: (String, String)*): Map[String, String] = {
      clauses.map { case (col, expr) =>
        col -> expr
      }.toMap
    }
  }

  object Query {
    def apply(selects: Map[String, String] = null,
              wheres: Seq[String] = null,
              startPartition: String = null,
              endPartition: String = null,
              timeColumn: String = null,
              setups: Seq[String] = null,
              mutationTimeColumn: String = null,
              reversalColumn: String = null,
              partitionColumn: String = null): Query = {
      val result = new Query()
      if (selects != null)
        result.setSelects(selects.toJava)
      if (wheres != null)
        result.setWheres(wheres.toJava)
      result.setStartPartition(startPartition)
      result.setEndPartition(endPartition)
      result.setTimeColumn(timeColumn)
      if (setups != null)
        result.setSetups(setups.toJava)
      result.setMutationTimeColumn(mutationTimeColumn)
      result.setReversalColumn(reversalColumn)
      result.setPartitionColumn(partitionColumn)
      result
    }
  }

  object AggregationPart {
    def apply(operation: Operation,
              inputColumn: String,
              window: Window = WindowUtils.Unbounded,
              argMap: Map[String, String] = null,
              bucket: String = null): AggregationPart = {
      val result = new AggregationPart()
      result.setOperation(operation)
      result.setInputColumn(inputColumn)
      result.setWindow(window)
      if (argMap != null)
        result.setArgMap(argMap.toJava)
      if (bucket != null) {
        result.setBucket(bucket)
      }
      result
    }
  }

  object Aggregation {
    def apply(operation: Operation,
              inputColumn: String,
              windows: Seq[Window] = null,
              argMap: Map[String, String] = null,
              buckets: Seq[String] = null): Aggregation = {
      val result = new Aggregation()
      result.setOperation(operation)
      result.setInputColumn(inputColumn)
      if (argMap != null)
        result.setArgMap(argMap.toJava)
      if (windows != null)
        result.setWindows(windows.toJava)
      if (buckets != null)
        result.setBuckets(buckets.toJava)
      result
    }
  }

  object Source {
    def entities(query: Query,
                 snapshotTable: String,
                 mutationTable: String = null,
                 mutationTopic: String = null): Source = {
      val result = new EntitySource()
      result.setQuery(query)
      result.setSnapshotTable(snapshotTable)
      result.setMutationTable(mutationTable)
      result.setMutationTopic(mutationTopic)
      val source = new Source()
      source.setEntities(result)
      source
    }

    def events(query: Query, table: String, topic: String = null, isCumulative: Boolean = false): Source = {
      val result = new EventSource()
      result.setQuery(query)
      result.setTable(table)
      result.setTopic(topic)
      result.setIsCumulative(isCumulative)
      val source = new Source()
      source.setEvents(result)
      source
    }

    def joinSource(join: Join, query: Query): Source = {
      val joinSource = new JoinSource()
      joinSource.setJoin(join)
      joinSource.setQuery(query)
      val source = new Source()
      source.setJoinSource(joinSource)
      source
    }
  }

  object GroupBy {
    def apply(
        metaData: MetaData = null,
        sources: Seq[Source] = null,
        keyColumns: Seq[String] = null,
        aggregations: Seq[Aggregation] = null,
        accuracy: Accuracy = null,
        derivations: Seq[Derivation] = null,
        keyTransforms: Map[String, String] = null,
        setups: Seq[String] = null
    ): GroupBy = {
      val result = new GroupBy()
      result.setMetaData(metaData)
      if (sources != null)
        result.setSources(sources.toJava)
      if (keyColumns != null)
        result.setKeyColumns(keyColumns.toJava)
      if (aggregations != null)
        result.setAggregations(aggregations.toJava)
      if (accuracy != null)
        result.setAccuracy(accuracy)
      if (derivations != null)
        result.setDerivations(derivations.toJava)
      if (keyTransforms != null)
        result.setKeyTransforms(keyTransforms.toJava)
      if (setups != null)
        result.setSetups(setups.toJava)
      result
    }
  }

  object Join {
    def apply(metaData: MetaData = null,
              left: Source = null,
              joinParts: Seq[JoinPart] = null,
              externalParts: Seq[ExternalPart] = null,
              bootstrapParts: Seq[BootstrapPart] = null,
              rowIds: Seq[String] = null,
              derivations: Seq[Derivation] = null,
              skewKeys: Map[String, Seq[String]] = null,
              useLongNames: Boolean = true): Join = {
      val result = new Join()
      result.setMetaData(metaData)
      result.setLeft(left)
      result.setUseLongNames(useLongNames)
      if (joinParts != null)
        result.setJoinParts(joinParts.toJava)
      if (externalParts != null)
        result.setOnlineExternalParts(externalParts.toJava)
      if (bootstrapParts != null)
        result.setBootstrapParts(bootstrapParts.toJava)
      if (rowIds != null)
        result.setRowIds(rowIds.toJava)
      if (derivations != null)
        result.setDerivations(derivations.toJava)
      if (skewKeys != null)
        result.setSkewKeys(skewKeys.mapValues(_.toJava).toMap.toJava)
      result
    }
  }

  object ExternalSource {
    def apply(metadata: MetaData, keySchema: DataType, valueSchema: DataType): ExternalSource = {
      val result = new ExternalSource()
      result.setMetadata(metadata)
      result.setKeySchema(toTDataType(keySchema))
      result.setValueSchema(toTDataType(valueSchema))
      result
    }
  }

  object ContextualSource {
    def apply(fields: Array[StructField]): ExternalSource = {
      val result = new ExternalSource
      result.setMetadata(MetaData(name = Constants.ContextualSourceName))
      result.setKeySchema(toTDataType(StructType(Constants.ContextualSourceKeys, fields)))
      result.setValueSchema(toTDataType(StructType(Constants.ContextualSourceValues, fields)))
    }
  }

  object ExternalPart {
    def apply(
        externalSource: ExternalSource,
        keyMapping: Map[String, String] = null,
        prefix: String = null
    ): ExternalPart = {
      val result = new ExternalPart()
      result.setSource(externalSource)
      if (keyMapping != null)
        result.setKeyMapping(keyMapping.toJava)
      result.setPrefix(prefix)
      result
    }
  }

  object JoinPart {
    def apply(
        groupBy: GroupBy = null,
        keyMapping: Map[String, String] = null,
        prefix: String = null
    ): ai.chronon.api.JoinPart = {
      val result = new ai.chronon.api.JoinPart()
      result.setGroupBy(groupBy)
      if (keyMapping != null)
        result.setKeyMapping(keyMapping.toJava)
      result.setPrefix(prefix)
      result.setUseLongNames(true)
      result
    }
  }

  object MetaData {
    def apply(
        name: String = null,
        online: Boolean = false,
        production: Boolean = false,
        customJson: String = null,
        namespace: String = null,
        team: String = null,
        samplePercent: Double = 100,
        consistencySamplePercent: Double = 5,
        tableProperties: Map[String, String] = Map.empty,
        historicalBackfill: Boolean = true,
        driftSpec: DriftSpec = null,
        additionalOutputPartitionColumns: Seq[String] = Seq.empty,
        executionInfo: ExecutionInfo = null,
        version: Int = 0
    ): MetaData = {
      val result = new MetaData()
      result.setName(name)
      result.setOnline(online)
      result.setProduction(production)
      result.setCustomJson(customJson)
      result.setOutputNamespace(namespace)
      result.setVersion(version.toString)

      val effectiveTeam = (Option(team), Option(name)) match {
        case (Some(t), _) => t
        case (_, Some(n)) => n.split("\\.").headOption.getOrElse("chronon")
        case _            => "chronon"
      }

      result.setTeam(effectiveTeam)

      if (samplePercent > 0)
        result.setSamplePercent(samplePercent)
      if (consistencySamplePercent > 0)
        result.setConsistencySamplePercent(consistencySamplePercent)
      if (tableProperties.nonEmpty)
        result.setTableProperties(tableProperties.toJava)
      if (driftSpec != null)
        result.setDriftSpec(driftSpec)

      if (executionInfo != null) {
        result.setExecutionInfo(executionInfo.setHistoricalBackfill(historicalBackfill))
      } else {
        result.setExecutionInfo(
          new ExecutionInfo()
            .setHistoricalBackfill(historicalBackfill))
      }

      if (additionalOutputPartitionColumns.nonEmpty) {
        result.setAdditionalOutputPartitionColumns(additionalOutputPartitionColumns.toJava)
      }

      result
    }
  }

  object StagingQuery {
    def apply(
        query: String = null,
        metaData: MetaData = null,
        startPartition: String = null,
        setups: Seq[String] = null,
        engineType: EngineType = EngineType.SPARK,
        tableDependencies: Seq[TableDependency] = null
    ): StagingQuery = {
      val stagingQuery = new StagingQuery()
      stagingQuery.setQuery(query)
      stagingQuery.setMetaData(metaData)
      stagingQuery.setStartPartition(startPartition)
      stagingQuery.setEngineType(engineType)
      if (tableDependencies != null) stagingQuery.setTableDependencies(tableDependencies.toJava)
      if (setups != null) stagingQuery.setSetups(setups.toJava)
      stagingQuery
    }
  }

  object BootstrapPart {
    def apply(
        query: Query = null,
        table: String = null,
        keyColumns: Seq[String] = null,
        metaData: MetaData = null
    ): BootstrapPart = {
      val bootstrapPart = new BootstrapPart()
      bootstrapPart.setQuery(query)
      bootstrapPart.setTable(table)
      Option(keyColumns)
        .map(_.toSeq.toJava)
        .foreach(bootstrapPart.setKeyColumns)
      bootstrapPart.setMetaData(metaData)
      bootstrapPart
    }
  }

  object Derivation {
    def apply(
        name: String = null,
        expression: String = null
    ): Derivation = {
      val derivation = new Derivation()
      if (name != null) {
        derivation.setName(name)
      }
      if (derivation != null) {
        derivation.setExpression(expression)
      }
      derivation
    }
  }

  object TableDependency {
    def apply(
        table: String,
        startOffsetDays: Int = 0,
        endOffsetDays: Int = 0,
        startCutoff: String = null,
        endCutoff: String = null
    ): TableDependency = {
      val dep = new TableDependency()
      val tableInfo = new TableInfo()
      tableInfo.setTable(table)
      dep.setTableInfo(tableInfo)

      dep.setStartOffset(new Window(startOffsetDays, TimeUnit.DAYS))
      dep.setEndOffset(new Window(endOffsetDays, TimeUnit.DAYS))
      dep.setStartCutOff(startCutoff)
      dep.setEndCutOff(endCutoff)

      dep
    }
  }

  // Alias for shorter syntax in WorkflowPlan builder
  object Dep {
    def apply(
        table: String,
        startOffsetDays: Int = 0,
        endOffsetDays: Int = 0,
        startCutoff: String = null,
        endCutoff: String = null
    ): TableDependency = TableDependency(table, startOffsetDays, endOffsetDays, startCutoff, endCutoff)
  }

  object NodeMeta {
    def apply(
        outputTable: String,
        stepSize: Int = 1,
        deps: Seq[TableDependency] = Seq.empty,
        continuous: Boolean = false,
        retries: Int = 3
    ): ai.chronon.planner.NodeMeta = {
      val nodeMeta = new ai.chronon.planner.NodeMeta()
      val nodeHash = s"hash_${outputTable.replace(".", "_")}"

      nodeMeta.setName(outputTable)
      nodeMeta.setHash(nodeHash)
      nodeMeta.setContinuous(continuous)
      nodeMeta.setStepSize(stepSize)
      nodeMeta.setOutputTable(outputTable)
      nodeMeta.setRetries(retries)
      nodeMeta.setDeps(deps.toJava)
      nodeMeta
    }
  }

  object WorkflowPlan {
    def apply(
        nodes: Map[String, ai.chronon.planner.NodeMeta],
        terminalNode: String = null
    ): ai.chronon.planner.WorkflowPlan = {
      val plan = new ai.chronon.planner.WorkflowPlan()

      // Set node names from the map keys
      val nodeMetaList = nodes.map { case (nodeName, nodeMeta) =>
        nodeMeta.setName(nodeName)
        nodeMeta
      }.toList

      plan.setNodes(nodeMetaList.toJava)

      val effectiveTerminalNode = Option(terminalNode).getOrElse {
        // If no terminal node specified, use the last node in the map
        nodes.keys.lastOption.getOrElse("")
      }
      plan.setTerminalNode(effectiveTerminalNode)

      plan
    }
  }

  // Shorter aliases for better readability
  def node(
      outputTable: String,
      stepSize: Int = 1,
      deps: Seq[TableDependency] = Seq.empty,
      continuous: Boolean = false,
      retries: Int = 3
  ): ai.chronon.planner.NodeMeta = NodeMeta(outputTable, stepSize, deps, continuous, retries)

  def dep(
      table: String,
      startOffsetDays: Int = 0,
      endOffsetDays: Int = 0,
      startCutoff: String = null,
      endCutoff: String = null
  ): TableDependency = Dep(table, startOffsetDays, endOffsetDays, startCutoff, endCutoff)

  object Model {
    def apply(
        metaData: MetaData = null,
        inferenceSpec: InferenceSpec = null,
        inputMapping: Map[String, String] = null,
        outputMapping: Map[String, String] = null,
        valueSchema: TDataType = null,
        trainingSpec: TrainingSpec = null
    ): Model = {
      val result = new Model()
      if (metaData != null)
        result.setMetaData(metaData)
      if (inferenceSpec != null)
        result.setInferenceSpec(inferenceSpec)
      if (inputMapping != null)
        result.setInputMapping(inputMapping.toJava)
      if (outputMapping != null)
        result.setOutputMapping(outputMapping.toJava)
      if (valueSchema != null)
        result.setValueSchema(valueSchema)
      if (trainingSpec != null)
        result.setTrainingConf(trainingSpec)
      result
    }
  }

  object InferenceSpec {
    def apply(
        modelBackend: ModelBackend = ModelBackend.VertexAI,
        modelBackendParams: Map[String, String] = null,
        resourceConfig: ResourceConfig = null
    ): InferenceSpec = {
      val result = new InferenceSpec()
      result.setModelBackend(modelBackend)
      if (modelBackendParams != null)
        result.setModelBackendParams(modelBackendParams.toJava)
      if (resourceConfig != null)
        result.setResourceConfig(resourceConfig)
      result
    }
  }

  object TrainingSpec {
    def apply(
        trainingDataSource: Source = null,
        trainingDataWindow: Window = null
    ): TrainingSpec = {
      val result = new TrainingSpec()
      if (trainingDataSource != null)
        result.setTrainingDataSource(trainingDataSource)
      if (trainingDataWindow != null)
        result.setTrainingDataWindow(trainingDataWindow)
      result
    }
  }

  object ModelTransforms {
    def apply(
        sources: Seq[Source] = null,
        models: Seq[Model] = null,
        passthroughFields: Seq[String] = null,
        metaData: MetaData = null,
        keySchema: TDataType = null
    ): ModelTransforms = {
      val result = new ModelTransforms()
      if (sources != null)
        result.setSources(sources.toJava)
      if (models != null)
        result.setModels(models.toJava)
      if (passthroughFields != null)
        result.setPassthroughFields(passthroughFields.toJava)
      if (metaData != null)
        result.setMetaData(metaData)
      if (keySchema != null)
        result.setKeySchema(keySchema)
      result
    }
  }

  // Helper for creating schema types commonly used in testing
  def structSchema(name: String, fields: (String, DataType)*): TDataType = {
    val structFields = fields.map { case (fieldName, dataType) =>
      new DataField().setName(fieldName).setDataType(toTDataType(dataType))
    }

    new TDataType(DataKind.STRUCT)
      .setName(name)
      .setParams(structFields.toList.toJava)
  }

}
