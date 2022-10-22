/*
 * Copyright 2011-2022 Lime Mojito Pty Ltd
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
 *
 */

package com.limemojito.trading.dukascopy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.time.temporal.TemporalAdjusters.*;
import static java.util.stream.Collectors.groupingBy;

/**
 * Paths follow the shape EURUSD/2017/00/03/19h_ticks.bi5
 */
@Component
@Slf4j
public class DukascopyPathGenerator {

    private final NumberFormat format00 = new DecimalFormat("00");

    public List<List<String>> generatePathsGroupedByDay(String symbol, LocalDate start, LocalDate end) {
        final List<String> paths = generatePaths(symbol, start, end);
        final int dayPathLength = 17;
        Map<String, List<String>> groupedByDay = paths.stream()
                                                      .collect(groupingBy(path -> path.substring(0, dayPathLength)));
        final List<List<String>> grouped = groupedByDay.keySet()
                                                       .stream()
                                                       .sorted()
                                                       .map(groupedByDay::get)
                                                       .collect(Collectors.toList());
        log.debug("Generated {} day groups", groupedByDay.size());
        return grouped;
    }

    public List<String> generatePaths(String symbol, LocalDate start, LocalDate end) {
        final List<String> paths = new LinkedList<>();
        if (start.getYear() == end.getYear()) {
            computePartialYear(paths, symbol, start, end);
        } else {
            computePartialYear(paths, symbol, start, start.with(lastDayOfYear()));
            if (end.getYear() - start.getYear() - 1 > 0) {
                for (int y = start.getYear() + 1; y <= end.getYear() - 1; y++) {
                    final LocalDate newStart = LocalDate.of(y, 1, 1);
                    final LocalDate newEnd = LocalDate.of(y, 12, 31);
                    computePartialYear(paths, symbol, newStart, newEnd);
                }
            }
            computePartialYear(paths, symbol, end.with(firstDayOfYear()), end);
        }
        log.debug("Generated {} paths", paths.size());
        return paths;
    }

    private void computePartialYear(List<String> paths, String symbol, LocalDate startInYear, LocalDate endInYear) {
        if (startInYear.getMonthValue() == endInYear.getMonthValue()) {
            computePartialMonth(paths, symbol, startInYear, endInYear);
        } else {
            computePartialMonth(paths, symbol, startInYear, startInYear.with(lastDayOfMonth()));
            for (int m = startInYear.getMonthValue() + 1; m <= endInYear.getMonthValue() - 1; m++) {
                final LocalDate startOfMonth = LocalDate.of(startInYear.getYear(), m, 1);
                final LocalDate endOfMonth = startOfMonth.with(lastDayOfMonth());
                computePartialMonth(paths, symbol, startOfMonth, endOfMonth);
            }
            computePartialMonth(paths, symbol, endInYear.with(firstDayOfMonth()), endInYear);
        }
    }

    private void computePartialMonth(List<String> paths, String symbol, LocalDate startInMonth, LocalDate endInMonth) {
        for (int d = startInMonth.getDayOfMonth(); d <= endInMonth.getDayOfMonth(); d++) {
            computeDay(paths, symbol, LocalDate.of(startInMonth.getYear(), startInMonth.getMonthValue(), d));
        }
    }

    private void computeDay(List<String> paths, String symbol, LocalDate day) {
        final int hoursInDay = 24;
        for (int h = 0; h < hoursInDay; h++) {
            paths.add(dukascopyPath(symbol, day.getYear(), day.getMonthValue(), day.getDayOfMonth(), h));
        }
    }

    private String dukascopyPath(String symbol, int year, int month, int day, int hour) {
        return format("%s/%d/%s/%s/%sh_ticks.bi5",
                      symbol,
                      year,
                      format00.format(month - 1),
                      format00.format(day),
                      format00.format(hour));
    }
}
