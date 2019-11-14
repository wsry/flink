/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.orc;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.flink.orc.OrcRowInputFormatTest.getPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Test for {@link OrcColumnarRowSplitReader}.
 */
public class OrcColumnarRowSplitReaderTest {

	private static final int BATCH_SIZE = 10;

	private static final Path TEST_FILE_FLAT = new Path(getPath("test-data-flat.orc"));
	private static final DataType[] TEST_SCHEMA_FLAT = new DataType[] {
			DataTypes.INT(),
			DataTypes.STRING(),
			DataTypes.STRING(),
			DataTypes.STRING(),
			DataTypes.INT(),
			DataTypes.STRING(),
			DataTypes.INT(),
			DataTypes.INT(),
			DataTypes.INT()
	};

	private static final Path TEST_FILE_DECIMAL = new Path(getPath("test-data-decimal.orc"));
	private static final DataType[] TEST_SCHEMA_DECIMAL = new DataType[] {DataTypes.DECIMAL(10, 5)};

	@Test
	public void testReadFileInSplits() throws IOException {
		FileInputSplit[] splits = createSplits(TEST_FILE_FLAT, 4);

		long cnt = 0;
		long totalF0 = 0;
		// read all splits
		for (FileInputSplit split : splits) {

			try (OrcColumnarRowSplitReader reader = createReader(
					new int[] {0, 1},
					TEST_SCHEMA_FLAT,
					new ArrayList<>(),
					new HashMap<>(),
					split)) {
				// read and count all rows
				while (!reader.reachedEnd()) {
					BaseRow row = reader.nextRecord(null);
					Assert.assertFalse(row.isNullAt(0));
					Assert.assertFalse(row.isNullAt(1));
					totalF0 += row.getInt(0);
					Assert.assertNotNull(row.getString(1).toString());
					cnt++;
				}
			}
		}
		// check that all rows have been read
		assertEquals(1920800, cnt);
		assertEquals(1844737280400L, totalF0);
	}

	@Test
	public void testReadDecimalTypeFile() throws IOException {
		FileInputSplit[] splits = createSplits(TEST_FILE_DECIMAL, 1);

		try (OrcColumnarRowSplitReader reader = createReader(
				new int[] {0},
				TEST_SCHEMA_DECIMAL,
				new ArrayList<>(),
				new HashMap<>(),
				splits[0])) {
			assertFalse(reader.reachedEnd());
			BaseRow row = reader.nextRecord(null);

			// validate first row
			assertNotNull(row);
			assertEquals(1, row.getArity());
			assertEquals(Decimal.castFrom(-1000.5d, 10, 5), row.getDecimal(0, 10, 5));

			// check correct number of rows
			long cnt = 1;
			long nullCount = 0;
			while (!reader.reachedEnd()) {
				row = reader.nextRecord(null);
				if (!row.isNullAt(0)) {
					assertNotNull(row.getDecimal(0, 10, 5));
				} else {
					nullCount++;
				}
				cnt++;
			}
			assertEquals(6000, cnt);
			assertEquals(2000, nullCount);
		}
	}

	@Test
	public void testReadFileWithSelectFields() throws IOException {
		FileInputSplit[] splits = createSplits(TEST_FILE_FLAT, 4);

		long cnt = 0;
		long totalF0 = 0;

		Map<String, Object> partSpec = new HashMap<>();
		partSpec.put("f1", 1);
		partSpec.put("f3", 3L);
		partSpec.put("f5", "f5");
		partSpec.put("f8", BigDecimal.valueOf(5.333));
		partSpec.put("f13", "f13");

		// read all splits
		for (FileInputSplit split : splits) {
			try (OrcColumnarRowSplitReader reader = createReader(
					new int[] {8, 1, 3, 0, 5, 2},
					new DataType[] {
							/* 0 */ DataTypes.INT(),
							/* 1 */ DataTypes.INT(), // part-1
							/* 2 */ DataTypes.STRING(),
							/* 3 */ DataTypes.BIGINT(), // part-2
							/* 4 */ DataTypes.STRING(),
							/* 5 */ DataTypes.STRING(), // part-3
							/* 6 */ DataTypes.STRING(),
							/* 7 */ DataTypes.INT(),
							/* 8 */ DataTypes.DECIMAL(10, 5), // part-4
							/* 9 */ DataTypes.STRING(),
							/* 11*/ DataTypes.INT(),
							/* 12*/ DataTypes.INT(),
							/* 13*/ DataTypes.STRING(), // part-5
							/* 14*/ DataTypes.INT()
					},
					Arrays.asList("f1", "f3", "f5", "f8", "f13"),
					partSpec,
					split)) {
				// read and count all rows
				while (!reader.reachedEnd()) {
					BaseRow row = reader.nextRecord(null);

					// data values
					Assert.assertFalse(row.isNullAt(3));
					Assert.assertFalse(row.isNullAt(5));
					totalF0 += row.getInt(3);
					Assert.assertNotNull(row.getString(5).toString());

					// part values
					Assert.assertFalse(row.isNullAt(0));
					Assert.assertFalse(row.isNullAt(1));
					Assert.assertFalse(row.isNullAt(2));
					Assert.assertFalse(row.isNullAt(4));
					Assert.assertEquals(Decimal.castFrom(5.333, 10, 5), row.getDecimal(0, 10, 5));
					Assert.assertEquals(1, row.getInt(1));
					Assert.assertEquals(3, row.getLong(2));
					Assert.assertEquals("f5", row.getString(4).toString());
					cnt++;
				}
			}
		}
		// check that all rows have been read
		assertEquals(1920800, cnt);
		assertEquals(1844737280400L, totalF0);
	}

	@Test
	public void testReadFileWithPartitionValues() throws IOException {
		FileInputSplit[] splits = createSplits(TEST_FILE_FLAT, 4);

		long cnt = 0;
		long totalF0 = 0;
		// read all splits
		for (FileInputSplit split : splits) {

			try (OrcColumnarRowSplitReader reader = createReader(
					new int[] {2, 0, 1},
					TEST_SCHEMA_FLAT,
					new ArrayList<>(),
					new HashMap<>(),
					split)) {
				// read and count all rows
				while (!reader.reachedEnd()) {
					BaseRow row = reader.nextRecord(null);
					Assert.assertFalse(row.isNullAt(0));
					Assert.assertFalse(row.isNullAt(1));
					Assert.assertFalse(row.isNullAt(2));
					Assert.assertNotNull(row.getString(0).toString());
					totalF0 += row.getInt(1);
					Assert.assertNotNull(row.getString(2).toString());
					cnt++;
				}
			}
		}
		// check that all rows have been read
		assertEquals(1920800, cnt);
		assertEquals(1844737280400L, totalF0);
	}

	private OrcColumnarRowSplitReader createReader(
			int[] selectedFields,
			DataType[] fullTypes,
			List<String> partitionKeys,
			Map<String, Object> partitionSpec,
			FileInputSplit split) throws IOException {
		return new OrcColumnarRowSplitReader(
				new Configuration(),
				selectedFields,
				IntStream.range(0, fullTypes.length)
						.mapToObj(i -> "f" + i).toArray(String[]::new),
				fullTypes,
				partitionKeys,
				partitionSpec,
				new ArrayList<>(),
				BATCH_SIZE,
				split.getPath(),
				split.getStart(),
				split.getLength());
	}

	private static FileInputSplit[] createSplits(Path path, int minNumSplits) throws IOException {
		return new DummyFileInputFormat(path).createInputSplits(minNumSplits);
	}

	private static class DummyFileInputFormat extends FileInputFormat<Row> {

		private static final long serialVersionUID = 1L;

		private DummyFileInputFormat(Path path) {
			super(path);
		}

		@Override
		public boolean reachedEnd() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Row nextRecord(Row reuse) {
			throw new UnsupportedOperationException();
		}
	}
}
