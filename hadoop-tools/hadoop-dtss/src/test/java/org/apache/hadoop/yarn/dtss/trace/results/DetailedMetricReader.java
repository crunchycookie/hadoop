package org.apache.hadoop.yarn.dtss.trace.results;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.Iterator;

public final class DetailedMetricReader implements Closeable {
  private final CSVParser parser;
  private final Iterator<CSVRecord> iterator;

  public DetailedMetricReader(
      final String fpath, final char delimiter, final boolean hasHeader
  ) throws IOException {
    final InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(fpath));
    switch (delimiter) {
      case '\t':
        parser = CSVFormat.TDF.parse(inputStreamReader);
        break;
      case ',':
        parser = CSVFormat.DEFAULT.parse(inputStreamReader);
        break;
      default:
        throw new IllegalArgumentException("Invalid delimiter for CSV reader.");
    }

    iterator = parser.iterator();

    if (hasHeader) {
      // Skip the header
      iterator.next();
    }
  }

  public boolean hasNextRecord() {
    return iterator.hasNext();
  }

  public CSVRecord readRecord() {
    return iterator.next();
  }

  @Override
  public void close() throws IOException {
    if (parser != null && !parser.isClosed()) {
      parser.close();
    }
  }
}
