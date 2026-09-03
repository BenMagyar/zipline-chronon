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

package ai.chronon.flink

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder}

private[flink] object SparkEncoderCompat {
  def serializer[T](encoder: Encoder[T]): ExpressionEncoder.Serializer[T] =
    encoder match {
      case expressionEncoder: ExpressionEncoder[T] => expressionEncoder.createSerializer()
      case agnosticEncoder: AgnosticEncoder[T]     => ExpressionEncoder(agnosticEncoder).createSerializer()
    }
}
