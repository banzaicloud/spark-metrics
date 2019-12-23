/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.banzaicloud.spark.metrics

import com.codahale.metrics.MetricRegistry

import scala.util.Try
import scala.util.matching.Regex

/**
  * Exporter that converts DropWizard type metrics to Prometheus metrics while applies a regexp to change the name of
  * the metrics
  * @param metricsNameCaptureRegex the regexp to capture metrics name parts to replace, e.g. `(\w+)-(\w+)`
  * @param replacement the replacement string to replace the captured part of the metrics name with, e.g. `\${1}/\${2}`
  * @param registry the metrics registry that holds all the DropWizard metrics
  */
class DropwizardExportsWithMetricNameTransform(registry: MetricRegistry,
                                               metricsNameCaptureRegex: Regex,
                                               replacement: String,
                                               toLowerCase: Boolean = false,
) extends DropwizardExports(registry) {

  /**
    * Applies #metricsNameCaptureRegex
    * regular expression to metric name to capture the parts that will be replaced by the expression specified
    * in #replacement
    */
  override def transformMetricsName(originMetricName: String): String = {
    Try {
      val metricNameByReplacement = metricsNameCaptureRegex
        .replaceAllIn(originMetricName, replacement)

      if (toLowerCase) {
        metricNameByReplacement.toLowerCase
      } else metricNameByReplacement
    }.getOrElse(originMetricName)
  }
}
