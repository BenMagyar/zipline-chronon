/*
 *    Copyright (C) 2026 The Chronon Authors.
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

package ai.chronon.spark.catalog

import io.delta.kernel.{ScanBuilder, Snapshot}
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.FileStatus

private[catalog] object DeltaKernelCompat {
  def partitionColumnNames(snapshot: Snapshot, engine: Engine): java.util.List[String] =
    snapshot.getPartitionColumnNames(engine)

  def schema(snapshot: Snapshot, engine: Engine): StructType = snapshot.getSchema(engine)

  def checkpoints(snapshot: SnapshotImpl): java.util.List[FileStatus] = snapshot.getLogSegment.checkpoints

  def scanBuilder(snapshot: Snapshot, engine: Engine): ScanBuilder = snapshot.getScanBuilder(engine)
}
