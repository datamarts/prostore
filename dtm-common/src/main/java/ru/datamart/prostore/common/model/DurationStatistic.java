/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.prostore.common.model;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;

/**
 * Класс накопления статистики
 */
public class DurationStatistic {
  private String operation;
  private Long sum = 0L;
  private Long max = MIN_VALUE;
  private Long min = MAX_VALUE;
  private Long count = 0L;

  public DurationStatistic(String operation) {
    this.operation = operation;
  }

  public void add(Long duration) {
    this.sum += duration;
    count++;
    max = (max < duration) ? duration : max;
    min = (min > duration) ? duration : min;
  }

  private Float avg() {
    return count == 0L ? 0.0F : sum.floatValue() / count;
  }

  @Override
  public String toString() {
    return String.format("%s;%d;%d;%d;%d;%f", operation, count, sum, max, min, avg());
  }
}
