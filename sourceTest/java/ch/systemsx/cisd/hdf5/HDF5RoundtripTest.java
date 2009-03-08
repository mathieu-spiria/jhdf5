/*
 * Copyright 2007 ETH Zuerich, CISD
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.systemsx.cisd.hdf5;

import static ch.systemsx.cisd.hdf5.HDF5CompoundMemberMapping.mapping;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.HDFNativeData;
import ncsa.hdf.hdf5lib.exceptions.HDF5DatatypeInterfaceException;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import ncsa.hdf.hdf5lib.exceptions.HDF5SymbolTableException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import ch.systemsx.cisd.common.array.MDArray;
import ch.systemsx.cisd.common.array.MDFloatArray;
import ch.systemsx.cisd.common.logging.LogInitializer;
import ch.systemsx.cisd.common.utilities.OSUtilities;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation.StorageLayout;

/**
 * Test cases for {@link HDF5Writer} and {@link HDF5Reader}, doing "round-trips" to the HDF5 disk
 * format and back.
 * 
 * @author Bernd Rinn
 */
public class HDF5RoundtripTest
{

    private static final File rootDirectory = new File("targets", "unit-test-wd");

    private static final File workingDirectory = new File(rootDirectory, "hdf5-roundtrip-wd");

    @BeforeSuite
    public void init()
    {
        LogInitializer.init();
        workingDirectory.mkdirs();
        assertTrue(workingDirectory.isDirectory());
        workingDirectory.deleteOnExit();
        rootDirectory.deleteOnExit();
    }

    @Override
    protected void finalize() throws Throwable
    {
        // Delete the working directory
        if (workingDirectory.exists() && workingDirectory.canWrite())
        {
            workingDirectory.delete();
        }
        // Delete root directory
        if (rootDirectory.exists() && rootDirectory.canWrite())
        {
            rootDirectory.delete();
        }

        super.finalize();
    }

    public static void main(String[] args) throws Throwable
    {
        // Print OS Version
        System.out.println("Platform: " + OSUtilities.getComputerPlatform());
        HDF5RoundtripTest test = new HDF5RoundtripTest();

        test.init();

        // Print Library Version
        final int[] libversion = new int[3];
        H5.H5get_libversion(libversion);
        System.out.println("HDF5 Version: " + libversion[0] + "." + libversion[1] + "."
                + libversion[2]);

        // Tests
        test.testCreateSomeDeepGroup();
        test.testGetGroupMembersIteratively();
        test.testScalarValues();
        test.testDataSets();
        test.testAccessClosedReaderWriter();
        test.testDataSetsNonExtendable();
        test.testOverwriteContiguousDataSet();
        test.testStringArray();
        test.testStringCompression();
        test.testStringArrayCompression();
        test.testReadMDFloatArray();
        test.testReadToFloatMDArray();
        test.testIterateOverFloatArrayInNaturalBlocks(10, 99);
        test.testIterateOverFloatArrayInNaturalBlocks(10, 100);
        test.testIterateOverFloatArrayInNaturalBlocks(10, 101);
        test.testReadToFloatMDArrayBlockWithOffset();
        test.testIterateOverMDFloatArrayInNaturalBlocks(new int[]
            { 2, 2 }, new long[]
            { 4, 3 }, new float[]
            { 0f, 2f, 6f, 8f }, new int[][]
            {
                { 2, 2 },
                { 2, 1 },
                { 2, 2 },
                { 2, 1 } });
        test.testIterateOverMDFloatArrayInNaturalBlocks(new int[]
            { 2, 2 }, new long[]
            { 4, 4 }, new float[]
            { 0f, 2f, 8f, 10f }, new int[][]
            {
                { 2, 2 },
                { 2, 2 },
                { 2, 2 },
                { 2, 2 } });
        test.testIterateOverMDFloatArrayInNaturalBlocks(new int[]
            { 2, 2 }, new long[]
            { 4, 5 }, new float[]
            { 0f, 2f, 4f, 10f, 12f, 14f }, new int[][]
            {
                { 2, 2 },
                { 2, 2 },
                { 2, 1 },
                { 2, 2 },
                { 2, 2 },
                { 2, 1 } });
        test.testIterateOverMDFloatArrayInNaturalBlocks(new int[]
            { 3, 2 }, new long[]
            { 5, 4 }, new float[]
            { 0f, 2f, 12f, 14f }, new int[][]
            {
                { 3, 2 },
                { 3, 2 },
                { 2, 2 },
                { 2, 2 } });
        test.testIterateOverMDFloatArrayInNaturalBlocks(new int[]
            { 2, 2 }, new long[]
            { 5, 4 }, new float[]
            { 0f, 2f, 8f, 10f, 16f, 18f }, new int[][]
            {
                { 2, 2 },
                { 2, 2 },
                { 2, 2 },
                { 2, 2 },
                { 1, 2 },
                { 1, 2 } });
        test.testMDFloatArrayBlockWise();
        test.testMDFloatArrayBlockWiseWithMemoryOffset();
        test.testCompressedDataSet();
        test.testFloatVectorLength1();
        test.testFloatMatrixLength1();
        test.testOneRowFloatMatrix();
        test.testEmptyVectorDataSets();
        test.testEmptyVectorDataSetsContiguous();
        test.testEmptyVectorDataSetsCompact();
        test.testEmptyMatrixDataSets();
        test.testEmptyMatrixDataSetsContiguous();
        test.testOverwriteVectorIncreaseSize();
        test.testOverwriteMatrixIncreaseSize();
        test.testOverwriteStringVectorDecreaseSize();
        test.testAttributes();
        test.testCreateDataTypes();
        test.testGroups();
        test.testSoftLink();
        test.testBrokenSoftLink();
        test.testNullOnGetSymbolicLinkTargetForNoLink();
        test.testUpdateSoftLink();
        test.testExternalLink();
        test.testEnum();
        test.testEnumArray();
        test.testEnumArrayFromIntArray();
        test.testEnumArray16BitFromIntArray();
        test.testOpaqueType();
        test.testCompound();
        test.testCompoundOverflow();
        test.testBitFieldCompound();
        test.testCompoundArray();
        test.testCompoundArrayBlockWise();
        test.testCompoundMDArray();
        test.testCompoundMDArrayBlockWise();
        test.testConfusedCompound();
        test.testGetGroupMemberInformation();
        try
        {
            test.testGetLinkInformationFailed();
            System.err.println("testGetObjectTypeFailed(): failure not detected.");
        } catch (HDF5JavaException ex)
        {
            // Expected
        }
        test.testGetObjectType();
        test.testHardLink();
        test.testNullOnGetSymbolicLinkTargetForNoLink();
        test.testReadByteArrayDataSetBlockWise();
        test.testWriteByteArrayDataSetBlockWise();
        test.testWriteByteMatrixDataSetBlockWise();
        test.testWriteByteArrayDataSetBlockWiseMismatch();
        test.testWriteByteMatrixDataSetBlockWiseMismatch();
        test.testReadFloatMatrixDataSetBlockWise();
        test.testWriteFloatMatrixDataSetBlockWise();
        test.testExtendCompactDataset();
        test.testExtendContiguousDataset();
        test.testTimestamps();
        test.testTimestampArray();
        test.testTimestampArrayChunked();
        test.testTimeDurations();
        test.testSmallTimeDurations();
        test.testTimeDurationArrayChunked();
        test.testNumericConversion();
        test.finalize();
    }

    @Test
    public void testCreateSomeDeepGroup()
    {
        final File datasetFile = new File(workingDirectory, "deepGroup.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String groupName = "/some/deep/and/non/existing/group";
        writer.createGroup(groupName);
        assertTrue(writer.isGroup(groupName));
        writer.close();
    }

    @Test
    public void testGetGroupMembersIteratively()
    {
        final File datasetFile = new File(workingDirectory, "writereadwriteread.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String groupName = "/test/group/";
        final String dset1Name = "dset1";
        final String dset1Path = groupName + dset1Name;
        final float[] dset1 = new float[]
            { 1.3f, 2.4f, 3.6f };
        writer.writeFloatArray(dset1Path, dset1);
        final List<String> members1 = writer.getGroupMembers(groupName);
        assertEquals(1, members1.size());
        assertEquals(dset1Name, members1.get(0));
        final String dset2Name = "dset2";
        final String dset2Path = groupName + dset2Name;
        final int[] dset2 = new int[]
            { 1, 2, 3 };
        writer.writeIntArray(dset2Path, dset2);
        final Set<String> members2 = new HashSet<String>(writer.getGroupMembers(groupName));
        assertEquals(2, members2.size());
        assertTrue(members2.contains(dset1Name));
        assertTrue(members2.contains(dset2Name));
        writer.close();
    }

    @Test
    public void testScalarValues()
    {
        final File datasetFile = new File(workingDirectory, "values.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String booleanDatasetName = "/boolean";
        writer.writeBoolean(booleanDatasetName, true);
        final String byteDatasetName = "/byte";
        writer.writeByte(byteDatasetName, (byte) 17);
        final String shortDatasetName = "/short";
        writer.writeShort(shortDatasetName, (short) 1000);
        final String intDatasetName = "/int";
        writer.writeInt(intDatasetName, 1000000);
        final String longDatasetName = "/long";
        writer.writeLong(longDatasetName, 10000000000L);
        final String floatDatasetName = "/float";
        writer.writeFloat(floatDatasetName, 0.001f);
        final String doubleDatasetName = "/double";
        writer.writeDouble(doubleDatasetName, 1.0E100);
        final String stringDatasetName = "/string";
        writer.writeString(stringDatasetName, "some string");
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(reader.readBoolean(booleanDatasetName));
        assertEquals(17, reader.readByte(byteDatasetName));
        assertEquals(1000, reader.readShort(shortDatasetName));
        assertEquals(1000000, reader.readInt(intDatasetName));
        assertEquals(10000000000L, reader.readLong(longDatasetName));
        assertEquals(0.001f, reader.readFloat(floatDatasetName));
        assertEquals(1.0E100, reader.readDouble(doubleDatasetName));
        assertEquals("some string", reader.readString(stringDatasetName));
        reader.close();
    }

    @Test
    public void testReadMDFloatArray()
    {
        final File datasetFile = new File(workingDirectory, "mdArray.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/floatMatrix";
        final MDFloatArray arrayWritten = new MDFloatArray(new int[]
            { 3, 2, 4 });
        int count = 0;
        for (int i = 0; i < arrayWritten.size(0); ++i)
        {
            for (int j = 0; j < arrayWritten.size(1); ++j)
            {
                for (int k = 0; k < arrayWritten.size(2); ++k)
                {
                    arrayWritten.set(++count, new int[]
                        { i, j, k });
                }
            }
        }
        writer.writeFloatMDArray(floatDatasetName, arrayWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final MDFloatArray arrayRead = reader.readFloatMDArray(floatDatasetName);
        reader.close();
        assertEquals(arrayWritten, arrayRead);

    }

    @Test
    public void testBooleanArray()
    {
        final File datasetFile = new File(workingDirectory, "booleanArray.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String booleanDatasetName = "/booleanArray";
        final String longArrayDataSetName = "/longArray";
        final BitSet arrayWritten = new BitSet();
        arrayWritten.set(32);
        writer.writeBitField(booleanDatasetName, arrayWritten);
        writer.writeLongArray(longArrayDataSetName, BitSetConversionUtils
                .toStorageForm(arrayWritten));
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final BitSet arrayRead = reader.readBitField(booleanDatasetName);
        try
        {
            reader.readBitField(longArrayDataSetName);
            fail("Failed to detect type mismatch.");
        } catch (HDF5DatatypeInterfaceException ex)
        {
            // Expected, as the types do not match.
        }
        assertEquals(arrayWritten, arrayRead);
        final HDF5DataSetInformation info = reader.getDataSetInformation(booleanDatasetName);
        assertEquals(HDF5DataClass.BITFIELD, info.getTypeInformation().getDataClass());
        assertChunkSizes(info, HDF5Utils.MIN_CHUNK_SIZE);
        reader.close();
    }

    private void assertChunkSizes(final HDF5DataSetInformation info,
            final long... expectedChunkSize)
    {
        assertEquals(StorageLayout.CHUNKED, info.getStorageLayout());
        final int[] chunkSize = info.tryGetChunkSizes();
        assertNotNull(chunkSize);
        assertEquals(expectedChunkSize.length, chunkSize.length);
        for (int i = 0; i < expectedChunkSize.length; ++i)
        {
            assertEquals(Integer.toString(i), expectedChunkSize[i], chunkSize[i]);
        }
    }

    @Test
    public void testMDFloatArrayBlockWise()
    {
        final File datasetFile = new File(workingDirectory, "mdArrayBlockWise.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/floatMatrix";
        final long[] shape = new long[]
            { 10, 10, 10 };
        final int[] blockShape = new int[]
            { 5, 5, 5 };
        writer.createFloatMDArray(floatDatasetName, shape, blockShape);
        final float[] flatArray = new float[MDArray.getLength(blockShape)];
        for (int i = 0; i < flatArray.length; ++i)
        {
            flatArray[i] = i;
        }
        final MDFloatArray arrayBlockWritten = new MDFloatArray(flatArray, blockShape);
        for (int i = 0; i < 2; ++i)
        {
            for (int j = 0; j < 2; ++j)
            {
                for (int k = 0; k < 2; ++k)
                {
                    writer.writeFloatMDArrayBlock(floatDatasetName, arrayBlockWritten, new long[]
                        { i, j, k });
                }
            }
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        for (int i = 0; i < 2; ++i)
        {
            for (int j = 0; j < 2; ++j)
            {
                for (int k = 0; k < 2; ++k)
                {
                    final MDFloatArray arrayRead =
                            reader.readFloatMDArrayBlock(floatDatasetName, blockShape, new long[]
                                { i, j, k });
                    assertEquals(arrayBlockWritten, arrayRead);
                }
            }
        }
        reader.close();

    }

    @Test
    public void testMDFloatArrayBlockWiseWithMemoryOffset()
    {
        final File datasetFile = new File(workingDirectory, "mdArrayBlockWiseWithMemoryOffset.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/floatMatrix";
        final long[] shape = new long[]
            { 10, 10 };
        writer.createFloatMDArray(floatDatasetName, shape, MDArray.toInt(shape));
        final float[] flatArray = new float[MDArray.getLength(shape)];
        for (int i = 0; i < flatArray.length; ++i)
        {
            flatArray[i] = i;
        }
        final MDFloatArray arrayBlockWritten = new MDFloatArray(flatArray, shape);
        writer.writeFloatMDArrayBlockWithOffset(floatDatasetName, arrayBlockWritten, new int[]
            { 2, 2 }, new long[]
            { 0, 0 }, new int[]
            { 1, 3 });
        writer.writeFloatMDArrayBlockWithOffset(floatDatasetName, arrayBlockWritten, new int[]
            { 2, 2 }, new long[]
            { 2, 2 }, new int[]
            { 5, 1 });
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final float[][] matrixRead = reader.readFloatMatrix(floatDatasetName);
        reader.close();
        assertEquals(13f, matrixRead[0][0]);
        assertEquals(14f, matrixRead[0][1]);
        assertEquals(23f, matrixRead[1][0]);
        assertEquals(24f, matrixRead[1][1]);
        assertEquals(51f, matrixRead[2][2]);
        assertEquals(52f, matrixRead[2][3]);
        assertEquals(61f, matrixRead[3][2]);
        assertEquals(62f, matrixRead[3][3]);
        for (int i = 0; i < 10; ++i)
        {
            for (int j = 0; j < 10; ++j)
            {
                if ((i < 2 && j < 2) || (i > 1 && i < 4 && j > 1 && j < 4))
                {
                    continue;
                }
                assertEquals("(" + i + "," + j + "}", 0f, matrixRead[i][j]);
            }
        }
    }

    @Test
    public void testDataSets()
    {
        final File datasetFile = new File(workingDirectory, "datasets.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/Group1/floats";
        final float[] floatDataWritten = new float[]
            { 2.8f, 8.2f, -3.1f, 0.0f, 10000.0f };
        writer.writeFloatArray(floatDatasetName, floatDataWritten);
        final long[] longDataWritten = new long[]
            { 10, -1000000, 1, 0, 100000000000L };
        final String longDatasetName = "/Group2/longs";
        writer.writeLongArray(longDatasetName, longDataWritten);
        final byte[] byteDataWritten = new byte[]
            { 0, -1, 1, -128, 127 };
        final String byteDatasetName = "/Group2/bytes";
        writer.writeByteArray(byteDatasetName, byteDataWritten, true);
        final short[] shortDataWritten = new short[]
            { 0, -1, 1, -128, 127 };
        final String shortDatasetName = "/Group2/shorts";
        writer.writeShortArray(shortDatasetName, shortDataWritten, true);
        final String stringDataWritten = "Some Random String";
        final String stringDatasetName = "/Group3/strings";
        final String stringDatasetName2 = "/Group4/strings";
        writer.writeString(stringDatasetName, stringDataWritten);
        writer.writeStringVariableLength(stringDatasetName2, stringDataWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final float[] floatDataRead = reader.readFloatArray(floatDatasetName);
        assertTrue(Arrays.equals(floatDataWritten, floatDataRead));
        final long[] longDataRead = reader.readLongArray(longDatasetName);
        assertTrue(Arrays.equals(longDataWritten, longDataRead));
        final byte[] byteDataRead = reader.readByteArray(byteDatasetName);
        assertTrue(Arrays.equals(byteDataWritten, byteDataRead));
        final short[] shortDataRead = reader.readShortArray(shortDatasetName);
        assertTrue(Arrays.equals(shortDataWritten, shortDataRead));
        final String stringDataRead = reader.readString(stringDatasetName);
        assertEquals(stringDataWritten, stringDataRead);
        reader.close();
    }

    @Test
    public void testAccessClosedReaderWriter()
    {
        final File datasetFile = new File(workingDirectory, "datasetsNonExtendable.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        writer.close();
        try
        {
            writer.writeBoolean("dataSet", true);
        } catch (HDF5JavaException ex)
        {
            assertEquals(String.format("HDF5 file '%s' is closed.", datasetFile.getAbsolutePath()),
                    ex.getMessage());
        }
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        reader.close();
        try
        {
            reader.readBoolean("dataSet");
        } catch (HDF5JavaException ex)
        {
            assertEquals(String.format("HDF5 file '%s' is closed.", datasetFile.getAbsolutePath()),
                    ex.getMessage());
        }
    }

    @Test
    public void testDataSetsNonExtendable()
    {
        final File datasetFile = new File(workingDirectory, "datasetsNonExtendable.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer =
                new HDF5WriterConfig(datasetFile).dontUseExtendableDataTypes().writer();
        final String floatDatasetName = "/Group1/floats";
        final float[] floatDataWritten = new float[]
            { 2.8f, 8.2f, -3.1f, 0.0f, 10000.0f };
        writer.writeFloatArray(floatDatasetName, floatDataWritten);
        final String compressedFloatDatasetName = "/Group1/floatsCompressed";
        writer.writeFloatArray(compressedFloatDatasetName, floatDataWritten, true);
        final long[] longDataWritten = new long[]
            { 10, -1000000, 1, 0, 100000000000L };
        final String longDatasetName = "/Group2/longs";
        writer.writeLongArray(longDatasetName, longDataWritten);
        final long[] longDataWrittenAboveCompactThreshold = new long[128];
        for (int i = 0; i < longDataWrittenAboveCompactThreshold.length; ++i)
        {
            longDataWrittenAboveCompactThreshold[i] = i;
        }
        final String longDatasetNameAboveCompactThreshold = "/Group2/longsContiguous";
        writer.writeLongArray(longDatasetNameAboveCompactThreshold,
                longDataWrittenAboveCompactThreshold);
        final String longDatasetNameAboveCompactThresholdCompress = "/Group2/longsChunked";
        writer.writeLongArray(longDatasetNameAboveCompactThresholdCompress,
                longDataWrittenAboveCompactThreshold, true);
        final byte[] byteDataWritten = new byte[]
            { 0, -1, 1, -128, 127 };
        final String byteDatasetName = "/Group2/bytes";
        writer.writeByteArray(byteDatasetName, byteDataWritten, true);
        final String stringDataWritten = "Some Random String";
        final String stringDatasetName = "/Group3/strings";
        final String stringDatasetName2 = "/Group4/strings";
        writer.writeString(stringDatasetName, stringDataWritten);
        writer.writeStringVariableLength(stringDatasetName2, stringDataWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final float[] floatDataRead = reader.readFloatArray(floatDatasetName);
        HDF5DataSetInformation info = reader.getDataSetInformation(floatDatasetName);
        assertEquals(StorageLayout.COMPACT, info.getStorageLayout());
        assertNull(info.tryGetChunkSizes());
        assertTrue(Arrays.equals(floatDataWritten, floatDataRead));
        final long[] compressedLongDataRead =
                reader.readLongArray(longDatasetNameAboveCompactThresholdCompress);
        info = reader.getDataSetInformation(longDatasetNameAboveCompactThresholdCompress);
        assertChunkSizes(info, longDataWrittenAboveCompactThreshold.length);
        assertTrue(Arrays.equals(longDataWrittenAboveCompactThreshold, compressedLongDataRead));
        final long[] longDataRead = reader.readLongArray(longDatasetName);
        info = reader.getDataSetInformation(longDatasetName);
        assertEquals(StorageLayout.COMPACT, info.getStorageLayout());
        assertNull(info.tryGetChunkSizes());
        assertTrue(Arrays.equals(longDataWritten, longDataRead));
        final long[] longDataReadAboveCompactThreshold =
                reader.readLongArray(longDatasetNameAboveCompactThreshold);
        info = reader.getDataSetInformation(longDatasetNameAboveCompactThreshold);
        assertEquals(StorageLayout.CONTIGUOUS, info.getStorageLayout());
        assertNull(info.tryGetChunkSizes());
        assertTrue(Arrays.equals(longDataWrittenAboveCompactThreshold,
                longDataReadAboveCompactThreshold));
        final byte[] byteDataRead = reader.readByteArray(byteDatasetName);
        assertTrue(Arrays.equals(byteDataWritten, byteDataRead));
        final String stringDataRead = reader.readString(stringDatasetName);
        assertEquals(stringDataWritten, stringDataRead);
        reader.close();
    }

    @Test
    public void testOverwriteContiguousDataSet()
    {
        // Test for a bug in 1.8.1 and 1.8.2 when overwriting contiguous data sets and thereby
        // changing its size.
        // We have some workaround code in HDF5Writer.getDataSetId(), this is why this test runs
        // green. As new versions of HDF5 become available, one can try to comment out the
        // workaround code and see whether this test still runs red.
        final File datasetFile = new File(workingDirectory, "overwriteContiguousDataSet.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final String dsName = "longArray";
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).dontUseExtendableDataTypes().writer();
        // Creating the group is part of the "bug magic".
        writer.createGroup("group");
        final long[] arrayWritten1 = new long[1000];
        for (int i = 0; i < arrayWritten1.length; ++i)
        {
            arrayWritten1[i] = i;
        }
        writer.writeLongArray(dsName, arrayWritten1);
        writer.close();
        writer = new HDF5WriterConfig(datasetFile).writer();
        final long[] arrayWritten2 = new long[5];
        for (int i = 0; i < arrayWritten1.length; ++i)
        {
            arrayWritten1[i] = i * i;
        }
        writer.writeLongArray(dsName, arrayWritten2);
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final long[] arrayRead = reader.readLongArray(dsName);
        assertTrue(Arrays.equals(arrayWritten2, arrayRead));
        reader.close();
    }

    @Test
    public void testExtendCompactDataset()
    {
        final File datasetFile = new File(workingDirectory, "extendCompact.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final String dsName = "ds";
        long[] data = new long[]
            { 1, 2, 3 };
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        // Set maxdims of 5 so that we can overwrite the data set with a larger one later on.
        writer.createLongArrayCompact(dsName, 5);
        writer.writeLongArray(dsName, data);
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(Arrays.equals(data, reader.readLongArray(dsName)));
        reader.close();
        // Now write a larger data set and see whether it is correctly extended.
        writer = new HDF5WriterConfig(datasetFile).useLatestFileFormat().writer();
        data = new long[]
            { 17, 42, 1, 2, 3 };
        writer.writeLongArray(dsName, data);
        writer.close();
        reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(Arrays.equals(data, reader.readLongArray(dsName)));
        reader.close();
    }

    @Test
    public void testExtendChunkedDataset()
    {
        final File datasetFile = new File(workingDirectory, "extendChunked.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final String dsName = "ds";
        long[] data = new long[]
            { 1, 2, 3, 4, 5 };
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        writer.createLongArray(dsName, 5, 3);
        writer.writeLongArray(dsName, data);
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(Arrays.equals(data, reader.readLongArray(dsName)));
        reader.close();
        // Now write a larger data set and see whether the data set is correctly extended.
        writer = new HDF5WriterConfig(datasetFile).writer();
        data = new long[]
            { 17, 42, 1, 2, 3, 101, -5 };
        writer.writeLongArray(dsName, data);
        writer.close();
        reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(Arrays.equals(data, reader.readLongArray(dsName)));
        reader.close();
    }

    @Test
    public void testExtendContiguousDataset()
    {
        final File datasetFile = new File(workingDirectory, "extendContiguous.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final String dsName = "ds";
        long[] data = new long[]
            { 1, 2, 3 };
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).dontUseExtendableDataTypes().writer();
        // Set maxdims such that COMPACT_LAYOUT_THRESHOLD (int bytes!) is exceeded so that we get a
        // contiguous data set.
        writer.createLongArray(dsName, 128, 1);
        writer.writeLongArray(dsName, data);
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(Arrays.equals(data, reader.readLongArray(dsName)));
        reader.close();
        // Now write a larger data set and see whether the data set is correctly extended.
        writer = new HDF5WriterConfig(datasetFile).useLatestFileFormat().writer();
        data = new long[]
            { 17, 42, 1, 2, 3 };
        writer.writeLongArray(dsName, data);
        writer.close();
        reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(Arrays.equals(data, reader.readLongArray(dsName)));
        reader.close();
    }

    @Test
    public void testSpacesInDataSetName()
    {
        final File datasetFile = new File(workingDirectory, "datasetsWithSpaces.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "Float Dataset";
        final float[] floatDataWritten = new float[]
            { 2.8f, 8.2f, -3.1f, 0.0f, 10000.0f };
        writer.writeFloatArray(floatDatasetName, floatDataWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final float[] floatDataRead = reader.readFloatArray(floatDatasetName);
        assertTrue(Arrays.equals(floatDataWritten, floatDataRead));
        reader.close();
    }

    @Test
    public void testReadFloatMatrixDataSetBlockWise()
    {
        final File datasetFile = new File(workingDirectory, "readFloatMatrixBlockWise.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final float[][] floatMatrix = new float[10][10];
        for (int i = 0; i < floatMatrix.length; ++i)
        {
            for (int j = 0; j < floatMatrix[i].length; ++j)
            {
                floatMatrix[i][j] = i * j;
            }
        }
        writer.writeFloatMatrix(dsName, floatMatrix);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final int blockSize = 5;
        for (int i = 0; i < 2; ++i)
        {
            for (int j = 0; j < 2; ++j)
            {
                final float[][] floatMatrixBlockRead =
                        reader.readFloatMatrixBlock(dsName, blockSize, blockSize, i, j);
                assertEquals(blockSize, floatMatrixBlockRead.length);
                assertEquals(blockSize, floatMatrixBlockRead[0].length);
                final float[][] floatMatrixBlockExpected = new float[blockSize][];
                for (int k = 0; k < blockSize; ++k)
                {
                    final float[] rowExpected = new float[blockSize];
                    System.arraycopy(floatMatrix[i * blockSize + k], blockSize * j, rowExpected, 0,
                            blockSize);
                    floatMatrixBlockExpected[k] = rowExpected;
                }
                assertMatrixEquals(floatMatrixBlockExpected, floatMatrixBlockRead);
            }
        }
        reader.close();
    }

    @Test
    public void testWriteFloatMatrixDataSetBlockWise()
    {
        final File datasetFile = new File(workingDirectory, "writeFloatMatrixBlockWise.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final float[][] floatMatrixBlockWritten = new float[5][5];
        for (int i = 0; i < floatMatrixBlockWritten.length; ++i)
        {
            for (int j = 0; j < floatMatrixBlockWritten[i].length; ++j)
            {
                floatMatrixBlockWritten[i][j] = i * j;
            }
        }
        final int blockSize = 5;
        writer.createFloatMatrix(dsName, 2 * blockSize, 2 * blockSize, blockSize, blockSize);
        for (int i = 0; i < 2; ++i)
        {
            for (int j = 0; j < 2; ++j)
            {
                writer.writeFloatMatrixBlock(dsName, floatMatrixBlockWritten, i, j);
            }
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        for (int i = 0; i < 2; ++i)
        {
            for (int j = 0; j < 2; ++j)
            {
                final float[][] floatMatrixBlockRead =
                        reader.readFloatMatrixBlock(dsName, blockSize, blockSize, i, j);
                assertMatrixEquals(floatMatrixBlockWritten, floatMatrixBlockRead);
            }
        }
        reader.close();
    }

    @Test
    public void testWriteFloatMatrixDataSetBlockWiseWithOffset()
    {
        final File datasetFile =
                new File(workingDirectory, "writeFloatMatrixBlockWiseWithOffset.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final float[][] floatMatrixBlockWritten = new float[5][5];
        int count = 0;
        for (int i = 0; i < floatMatrixBlockWritten.length; ++i)
        {
            for (int j = 0; j < floatMatrixBlockWritten[i].length; ++j)
            {
                floatMatrixBlockWritten[i][j] = ++count;
            }
        }
        final int blockSize = 5;
        final int offsetX = 2;
        final int offsetY = 3;
        writer.createFloatMatrix(dsName, 2 * blockSize, 2 * blockSize, blockSize, blockSize);
        writer.writeFloatMatrixBlockWithOffset(dsName, floatMatrixBlockWritten, 5, 5, offsetX,
                offsetY);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final float[][] floatMatrixBlockRead =
                reader.readFloatMatrixBlockWithOffset(dsName, blockSize, blockSize, offsetX,
                        offsetY);
        assertMatrixEquals(floatMatrixBlockWritten, floatMatrixBlockRead);
        final float[][] floatMatrixRead = reader.readFloatMatrix(dsName);
        // Subtract the non-zero block.
        for (int i = 0; i < floatMatrixBlockWritten.length; ++i)
        {
            for (int j = 0; j < floatMatrixBlockWritten[i].length; ++j)
            {
                floatMatrixRead[offsetX + i][offsetY + j] -= floatMatrixBlockWritten[i][j];
            }
        }
        for (int i = 0; i < floatMatrixRead.length; ++i)
        {
            for (int j = 0; j < floatMatrixRead[i].length; ++j)
            {
                assertEquals(i + ":" + j, 0.0f, floatMatrixRead[i][j]);
            }
        }
        reader.close();
    }

    @Test
    public void testReadByteArrayDataSetBlockWise()
    {
        final File datasetFile = new File(workingDirectory, "readByteArrayBlockWise.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final byte[] byteArray = new byte[100];
        for (int i = 0; i < byteArray.length; ++i)
        {
            byteArray[i] = (byte) (100 + i);
        }
        writer.writeByteArray(dsName, byteArray);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final int blockSize = 10;
        for (int i = 0; i < 10; ++i)
        {
            final byte[] byteArrayBlockRead = reader.readByteArrayBlock(dsName, blockSize, i);
            assertEquals(blockSize, byteArrayBlockRead.length);
            final byte[] byteArrayBlockExpected = new byte[blockSize];
            System.arraycopy(byteArray, blockSize * i, byteArrayBlockExpected, 0, blockSize);
            assertTrue("Block " + i, Arrays.equals(byteArrayBlockExpected, byteArrayBlockRead));
        }
        reader.close();
    }

    @Test
    public void testWriteByteArrayDataSetBlockWise()
    {
        final File datasetFile = new File(workingDirectory, "writeByteArrayBlockWise.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final int size = 100;
        final int blockSize = 10;
        final int numberOfBlocks = 10;
        writer.createByteArray(dsName, size, blockSize, true);
        final byte[] block = new byte[blockSize];
        for (int i = 0; i < numberOfBlocks; ++i)
        {
            Arrays.fill(block, (byte) i);
            writer.writeByteArrayBlock(dsName, block, i);
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final byte[] byteArrayRead = reader.readAsByteArray(dsName);
        reader.close();
        assertEquals(size, byteArrayRead.length);
        for (int i = 0; i < byteArrayRead.length; ++i)
        {
            assertEquals("Byte " + i, (i / blockSize), byteArrayRead[i]);
        }
    }

    @Test
    public void testWriteByteArrayDataSetBlockWiseMismatch()
    {
        final File datasetFile = new File(workingDirectory, "writeByteArrayBlockWise.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final int size = 99;
        final int blockSize = 10;
        final int numberOfBlocks = 10;
        writer.createByteArray(dsName, size, blockSize, true);
        final byte[] block = new byte[blockSize];
        for (int i = 0; i < numberOfBlocks; ++i)
        {
            Arrays.fill(block, (byte) i);
            if (blockSize * (i + 1) > size)
            {
                final int ofs = blockSize * i;
                writer.writeByteArrayBlockWithOffset(dsName, block, size - ofs, ofs);
            } else
            {
                writer.writeByteArrayBlock(dsName, block, i);
            }
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final byte[] byteArrayRead = reader.readByteArray(dsName);
        reader.close();
        assertEquals(size, byteArrayRead.length);
        for (int i = 0; i < byteArrayRead.length; ++i)
        {
            assertEquals("Byte " + i, (i / blockSize), byteArrayRead[i]);
        }
    }

    @Test
    public void testWriteOpaqueByteArrayDataSetBlockWise()
    {
        final File datasetFile = new File(workingDirectory, "writeOpaqueByteArrayBlockWise.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final int size = 100;
        final int blockSize = 10;
        final int numberOfBlocks = 10;
        final HDF5OpaqueType opaqueDataType =
                writer.createOpaqueByteArray(dsName, "TAG", size, blockSize, true);
        final byte[] block = new byte[blockSize];
        for (int i = 0; i < numberOfBlocks; ++i)
        {
            Arrays.fill(block, (byte) i);
            writer.writeOpaqueByteArrayBlock(dsName, opaqueDataType, block, i);
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final byte[] byteArrayRead = reader.readAsByteArray(dsName);
        reader.close();
        assertEquals(size, byteArrayRead.length);
        for (int i = 0; i < byteArrayRead.length; ++i)
        {
            assertEquals("Byte " + i, (i / blockSize), byteArrayRead[i]);
        }
    }

    @Test
    public void testWriteOpaqueByteArrayDataSetBlockWiseMismatch()
    {
        final File datasetFile = new File(workingDirectory, "writeOpaqueByteArrayBlockWise.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final int size = 99;
        final int blockSize = 10;
        final int numberOfBlocks = 10;
        final HDF5OpaqueType opaqueDataType =
                writer.createOpaqueByteArray(dsName, "TAG", size, blockSize, true);
        final byte[] block = new byte[blockSize];
        for (int i = 0; i < numberOfBlocks; ++i)
        {
            Arrays.fill(block, (byte) i);
            if (blockSize * (i + 1) > size)
            {
                final int ofs = blockSize * i;
                writer.writeOpaqueByteArrayBlockWithOffset(dsName, opaqueDataType, block, size
                        - ofs, ofs);
            } else
            {
                writer.writeOpaqueByteArrayBlock(dsName, opaqueDataType, block, i);
            }
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final byte[] byteArrayRead = reader.readAsByteArray(dsName);
        reader.close();
        assertEquals(size, byteArrayRead.length);
        for (int i = 0; i < byteArrayRead.length; ++i)
        {
            assertEquals("Byte " + i, (i / blockSize), byteArrayRead[i]);
        }
    }

    @Test
    public void testWriteByteMatrixDataSetBlockWise()
    {
        final File datasetFile = new File(workingDirectory, "writeByteMatrixBlockWise.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final int sizeX = 100;
        final int sizeY = 10;
        final int blockSizeX = 10;
        final int blockSizeY = 5;
        final int numberOfBlocksX = 10;
        final int numberOfBlocksY = 2;
        writer.createByteMatrix(dsName, sizeX, sizeY, blockSizeX, blockSizeY, true);
        final byte[][] block = new byte[blockSizeX][blockSizeY];
        for (int i = 0; i < numberOfBlocksX; ++i)
        {
            for (int j = 0; j < numberOfBlocksY; ++j)
            {
                for (int k = 0; k < blockSizeX; ++k)
                {
                    Arrays.fill(block[k], (byte) (i + j));
                }
                writer.writeByteMatrixBlock(dsName, block, i, j);
            }
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final byte[][] byteMatrixRead = reader.readByteMatrix(dsName);
        reader.close();
        assertEquals(sizeX, byteMatrixRead.length);
        for (int i = 0; i < byteMatrixRead.length; ++i)
        {
            for (int j = 0; j < byteMatrixRead[i].length; ++j)
            {
                assertEquals("Byte (" + i + "," + j + ")", (i / blockSizeX + j / blockSizeY),
                        byteMatrixRead[i][j]);
            }
        }
    }

    @Test
    public void testWriteByteMatrixDataSetBlockWiseMismatch()
    {
        final File datasetFile = new File(workingDirectory, "writeByteMatrixBlockWiseMismatch.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final int sizeX = 99;
        final int sizeY = 12;
        final int blockSizeX = 10;
        final int blockSizeY = 5;
        final int numberOfBlocksX = 10;
        final int numberOfBlocksY = 3;
        writer.createByteMatrix(dsName, sizeX, sizeY, blockSizeX, blockSizeY, true);
        final byte[][] block = new byte[blockSizeX][blockSizeY];
        for (int i = 0; i < numberOfBlocksX; ++i)
        {
            for (int j = 0; j < numberOfBlocksY; ++j)
            {
                for (int k = 0; k < blockSizeX; ++k)
                {
                    Arrays.fill(block[k], (byte) (i + j));
                }
                writer.writeByteMatrixBlockWithOffset(dsName, block, Math.min(blockSizeX, sizeX - i
                        * blockSizeX), Math.min(blockSizeY, sizeY - j * blockSizeY),
                        i * blockSizeX, j * blockSizeY);
            }
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final byte[][] byteMatrixRead = reader.readByteMatrix(dsName);
        reader.close();
        assertEquals(sizeX, byteMatrixRead.length);
        for (int i = 0; i < byteMatrixRead.length; ++i)
        {
            for (int j = 0; j < byteMatrixRead[i].length; ++j)
            {
                assertEquals("Byte (" + i + "," + j + ")", (i / blockSizeX + j / blockSizeY),
                        byteMatrixRead[i][j]);
            }
        }
    }

    @Test
    public void testReadToFloatMDArray()
    {
        final File datasetFile = new File(workingDirectory, "readToFloatMDArray.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final MDFloatArray arrayWritten = new MDFloatArray(new float[]
            { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, new int[]
            { 3, 3 });
        writer.writeFloatMDArray(dsName, arrayWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final MDFloatArray arrayRead = new MDFloatArray(new int[]
            { 10, 10 });
        final int memOfsX = 2;
        final int memOfsY = 3;
        reader.readToFloatMDArrayWithOffset(dsName, arrayRead, new int[]
            { memOfsX, memOfsY });
        reader.close();
        final boolean[][] isSet = new boolean[10][10];
        for (int i = 0; i < arrayWritten.size(0); ++i)
        {
            for (int j = 0; j < arrayWritten.size(1); ++j)
            {
                isSet[memOfsX + i][memOfsY + j] = true;
                assertEquals("(" + i + "," + j + ")", arrayWritten.get(i, j), arrayRead.get(memOfsX
                        + i, memOfsY + j));
            }
        }
        for (int i = 0; i < arrayRead.size(0); ++i)
        {
            for (int j = 0; j < arrayRead.size(1); ++j)
            {
                if (isSet[i][j] == false)
                {
                    assertEquals("(" + i + "," + j + ")", 0f, arrayRead.get(i, j));
                }
            }
        }
    }

    @SuppressWarnings("unused")
    @DataProvider
    private Object[][] provideSizes()
    {
        return new Object[][]
            {
                { 10, 99 },
                { 10, 100 },
                { 10, 101 } };
    }

    @Test(dataProvider = "provideSizes")
    public void testIterateOverFloatArrayInNaturalBlocks(int blockSize, int dataSetSize)
    {
        final File datasetFile =
                new File(workingDirectory, "iterateOverFloatArrayInNaturalBlocks.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final float[] arrayWritten = new float[dataSetSize];
        for (int i = 0; i < dataSetSize; ++i)
        {
            arrayWritten[i] = i;
        }
        writer.createFloatArray(dsName, dataSetSize, blockSize);
        writer.writeFloatArray(dsName, arrayWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        int i = 0;
        for (HDF5DataBlock<float[]> block : reader.getFloatArrayNaturalBlocks(dsName))
        {
            assertEquals(i, block.getIndex());
            assertEquals(blockSize * i, block.getOffset());
            final float[] arrayReadBlock = block.getData();
            if (blockSize * (i + 1) > dataSetSize)
            {
                assertEquals(dataSetSize - i * blockSize, arrayReadBlock.length);
            } else
            {
                assertEquals(blockSize, arrayReadBlock.length);
            }
            final float[] arrayWrittenBlock = new float[arrayReadBlock.length];
            System.arraycopy(arrayWritten, (int) block.getOffset(), arrayWrittenBlock, 0,
                    arrayWrittenBlock.length);
            assertTrue(Arrays.equals(arrayWrittenBlock, arrayReadBlock));
            ++i;
        }
        assertEquals(dataSetSize / blockSize + (dataSetSize % blockSize != 0 ? 1 : 0), i);
        reader.close();
    }

    @Test
    public void testReadToFloatMDArrayBlockWithOffset()
    {
        final File datasetFile = new File(workingDirectory, "readToFloatMDArrayBlockWithOffset.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final MDFloatArray arrayWritten = new MDFloatArray(new float[]
            { 1, 2, 3, 4, 5, 6, 7, 8, 9 }, new int[]
            { 3, 3 });
        writer.writeFloatMDArray(dsName, arrayWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final MDFloatArray arrayRead = new MDFloatArray(new int[]
            { 10, 10 });
        final int memOfsX = 2;
        final int memOfsY = 3;
        final int diskOfsX = 1;
        final int diskOfsY = 0;
        final int blockSizeX = 2;
        final int blockSizeY = 2;
        reader.readToFloatMDArrayBlockWithOffset(dsName, arrayRead, new int[]
            { blockSizeX, blockSizeY }, new long[]
            { diskOfsX, diskOfsY }, new int[]
            { memOfsX, memOfsY });
        reader.close();
        final boolean[][] isSet = new boolean[10][10];
        for (int i = 0; i < blockSizeX; ++i)
        {
            for (int j = 0; j < blockSizeY; ++j)
            {
                isSet[memOfsX + i][memOfsY + j] = true;
                assertEquals("(" + i + "," + j + ")", arrayWritten.get(diskOfsX + i, diskOfsY + j),
                        arrayRead.get(memOfsX + i, memOfsY + j));
            }
        }
        for (int i = 0; i < arrayRead.size(0); ++i)
        {
            for (int j = 0; j < arrayRead.size(1); ++j)
            {
                if (isSet[i][j] == false)
                {
                    assertEquals("(" + i + "," + j + ")", 0f, arrayRead.get(i, j));
                }
            }
        }
    }

    @SuppressWarnings("unused")
    @DataProvider
    private Object[][] provideMDSizes()
    {
        return new Object[][]
            {
                { new int[]
                    { 2, 2 }, new long[]
                    { 4, 3 }, new float[]
                    { 0f, 2f, 6f, 8f }, new int[][]
                    {
                        { 2, 2 },
                        { 2, 1 },
                        { 2, 2 },
                        { 2, 1 } } },
                { new int[]
                    { 2, 2 }, new long[]
                    { 4, 4 }, new float[]
                    { 0f, 2f, 8f, 10f }, new int[][]
                    {
                        { 2, 2 },
                        { 2, 2 },
                        { 2, 2 },
                        { 2, 2 } } },
                { new int[]
                    { 2, 2 }, new long[]
                    { 4, 5 }, new float[]
                    { 0f, 2f, 4f, 10f, 12f, 14f }, new int[][]
                    {
                        { 2, 2 },
                        { 2, 2 },
                        { 2, 1 },
                        { 2, 2 },
                        { 2, 2 },
                        { 2, 1 } } },
                { new int[]
                    { 3, 2 }, new long[]
                    { 5, 4 }, new float[]
                    { 0f, 2f, 12f, 14f }, new int[][]
                    {
                        { 3, 2 },
                        { 3, 2 },
                        { 2, 2 },
                        { 2, 2 } } },
                { new int[]
                    { 2, 2 }, new long[]
                    { 5, 4 }, new float[]
                    { 0f, 2f, 8f, 10f, 16f, 18f }, new int[][]
                    {
                        { 2, 2 },
                        { 2, 2 },
                        { 2, 2 },
                        { 2, 2 },
                        { 1, 2 },
                        { 1, 2 } } }, };
    }

    @Test(dataProvider = "provideMDSizes")
    public void testIterateOverMDFloatArrayInNaturalBlocks(int[] blockSize, long[] dataSetSize,
            float[] firstNumberPerIteration, int[][] blockSizePerIteration)
    {
        final File datasetFile =
                new File(workingDirectory, "iterateOverMDFloatArrayInNaturalBlocks.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "ds";
        final float[] flattenedArray = new float[getNumberOfElements(dataSetSize)];
        for (int i = 0; i < flattenedArray.length; ++i)
        {
            flattenedArray[i] = i;
        }
        final MDFloatArray arrayWritten = new MDFloatArray(flattenedArray, dataSetSize);
        writer.createFloatMDArray(dsName, dataSetSize, blockSize);
        writer.writeFloatMDArray(dsName, arrayWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        int i = 0;
        for (HDF5MDDataBlock<MDFloatArray> block : reader.getFloatMDArrayNaturalBlocks(dsName))
        {
            assertEquals(firstNumberPerIteration[i], block.getData().get(0, 0));
            assertTrue(Arrays.equals(block.getData().dimensions(), blockSizePerIteration[i]));
            ++i;
        }
        assertEquals(firstNumberPerIteration.length, i);
        reader.close();
    }

    private static int getNumberOfElements(long[] size)
    {
        int elements = 1;
        for (long dim : size)
        {
            elements *= dim;
        }
        return elements;
    }

    @Test
    public void testStringArray()
    {
        final File stringArrayFile = new File(workingDirectory, "stringArray.h5");
        stringArrayFile.delete();
        assertFalse(stringArrayFile.exists());
        stringArrayFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(stringArrayFile).writer();
        final String[] data = new String[]
            { "abc", "ABCxxx", "xyz" };
        final String dataSetName = "/aStringArray";
        writer.writeStringArray(dataSetName, data);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(stringArrayFile).reader();
        final String[] dataStored = reader.readStringArray(dataSetName);
        assertTrue(Arrays.equals(data, dataStored));
        reader.close();
    }

    @Test
    public void testStringCompression()
    {
        final File compressedStringFile = new File(workingDirectory, "compressedStrings.h5");
        compressedStringFile.delete();
        assertFalse(compressedStringFile.exists());
        compressedStringFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(compressedStringFile).writer();
        final int size = 100000;
        final String dataSetName = "/hopefullyCompressedString";
        final String longMonotonousString = StringUtils.repeat("a", size);
        writer.writeString(dataSetName, longMonotonousString, true);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(compressedStringFile).reader();
        final String longMonotonousStringStored = reader.readString(dataSetName);
        assertEquals(longMonotonousString, longMonotonousStringStored);
        reader.close();
        assertTrue(Long.toString(compressedStringFile.length()),
                compressedStringFile.length() < size / 10);
    }

    @Test
    public void testStringArrayCompression()
    {
        final File compressedStringArrayFile =
                new File(workingDirectory, "compressedStringArray.h5");
        compressedStringArrayFile.delete();
        assertFalse(compressedStringArrayFile.exists());
        compressedStringArrayFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(compressedStringArrayFile).writer();
        final int size = 100000;
        final String longMonotonousString = StringUtils.repeat("a", size);
        final String[] data = new String[]
            { longMonotonousString, longMonotonousString, longMonotonousString };
        final String dataSetName = "/aHopeFullyCompressedStringArray";
        writer.writeStringArray(dataSetName, data, size, true);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(compressedStringArrayFile).reader();
        final String[] dataStored = reader.readStringArray(dataSetName);
        assertTrue(Arrays.equals(data, dataStored));
        reader.close();
        assertTrue(Long.toString(compressedStringArrayFile.length()), compressedStringArrayFile
                .length() < 3 * size / 10);
    }

    private void assertMatrixEquals(final float[][] floatMatrixWritten,
            final float[][] floatMatrixRead)
    {
        assertEquals(floatMatrixWritten.length, floatMatrixRead.length);
        for (int i = 0; i < floatMatrixWritten.length; ++i)
        {
            assertEquals(floatMatrixWritten[i].length, floatMatrixRead[i].length);
            for (int j = 0; j < floatMatrixWritten[i].length; ++j)
            {
                assertEquals(i + ":" + j, floatMatrixWritten[i][j], floatMatrixRead[i][j]);
            }
        }
    }

    @Test
    public void testCompressedDataSet()
    {
        final File datasetFile = new File(workingDirectory, "compressed.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String stringDatasetName = "/compressed";
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < 10000; ++i)
        {
            b.append("easyToCompress");
        }
        writer.writeByteArray(stringDatasetName, b.toString().getBytes(), true);
        writer.close();
    }

    @Test
    public void testFloatVectorLength1()
    {
        final File datasetFile = new File(workingDirectory, "singleFloatVector.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/singleFloat";
        final float[] floatDataWritten = new float[]
            { 1.0f };
        writer.writeFloatArray(floatDatasetName, floatDataWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        reader.hasAttribute(floatDatasetName, "flag");
        final float[] floatDataRead = reader.readFloatArray(floatDatasetName);
        assertTrue(Arrays.equals(floatDataWritten, floatDataRead));
        reader.close();
    }

    @Test
    public void testFloatMatrixLength1()
    {
        final File datasetFile = new File(workingDirectory, "singleFloatMatrix.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/singleFloat";
        final float[][] floatDataWritten = new float[][]
            {
                { 1.0f } };
        writer.writeFloatMatrix(floatDatasetName, floatDataWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final float[][] floatDataRead = reader.readFloatMatrix(floatDatasetName);
        assertTrue(equals(floatDataWritten, floatDataRead));
        reader.close();
    }

    @Test
    public void testOneRowFloatMatrix()
    {
        final File datasetFile = new File(workingDirectory, "oneRowFloatMatrix.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/singleFloat";
        final float[][] floatDataWritten = new float[][]
            {
                { 1.0f, 2.0f } };
        writer.writeFloatMatrix(floatDatasetName, floatDataWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final float[][] floatDataRead = reader.readFloatMatrix(floatDatasetName);
        assertTrue(equals(floatDataWritten, floatDataRead));
        reader.close();
    }

    private static boolean equals(float[][] a, float[][] a2)
    {
        if (a == a2)
        {
            return true;
        }
        if (a == null || a2 == null)
        {
            return false;
        }

        int rows = a.length;
        if (a2.length != rows)
        {
            return false;
        }

        for (int i = 0; i < rows; i++)
        {
            int columns = a[i].length;
            if (a2[i].length != columns)
            {
                return false;
            }
            for (int j = 0; j < columns; j++)
            {
                if (Float.floatToIntBits(a[i][j]) != Float.floatToIntBits(a2[i][j]))
                {
                    return false;
                }
            }
        }

        return true;
    }

    @Test
    public void testEmptyVectorDataSets()
    {
        final File datasetFile = new File(workingDirectory, "emptyVectorDatasets.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/float";
        writer.writeFloatArray(floatDatasetName, new float[0]);
        final String doubleDatasetName = "/double";
        writer.writeDoubleArray(doubleDatasetName, new double[0]);
        final String byteDatasetName = "byte";
        writer.writeByteArray(byteDatasetName, new byte[0]);
        final String shortDatasetName = "/short";
        writer.writeShortArray(shortDatasetName, new short[0]);
        final String intDatasetName = "/int";
        writer.writeIntArray(intDatasetName, new int[0]);
        final String longDatasetName = "/long";
        writer.writeLongArray(longDatasetName, new long[0]);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertEquals(HDF5ObjectType.DATASET, reader.getObjectType(floatDatasetName));
        assertTrue(reader.readFloatArray(floatDatasetName).length == 0);
        assertTrue(reader.readDoubleArray(doubleDatasetName).length == 0);
        assertTrue(reader.readByteArray(byteDatasetName).length == 0);
        assertTrue(reader.readShortArray(shortDatasetName).length == 0);
        assertTrue(reader.readIntArray(intDatasetName).length == 0);
        assertTrue(reader.readLongArray(longDatasetName).length == 0);
        reader.close();
    }

    @Test
    public void testEmptyVectorDataSetsContiguous()
    {
        final File datasetFile = new File(workingDirectory, "emptyVectorDatasetsContiguous.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer =
                new HDF5WriterConfig(datasetFile).dontUseExtendableDataTypes().writer();
        final String floatDatasetName = "/float";
        writer.writeFloatArray(floatDatasetName, new float[0]);
        final String doubleDatasetName = "/double";
        writer.writeDoubleArray(doubleDatasetName, new double[0]);
        final String byteDatasetName = "byte";
        writer.writeByteArray(byteDatasetName, new byte[0]);
        final String shortDatasetName = "/short";
        writer.writeShortArray(shortDatasetName, new short[0]);
        final String intDatasetName = "/int";
        writer.writeIntArray(intDatasetName, new int[0]);
        final String longDatasetName = "/long";
        writer.writeLongArray(longDatasetName, new long[0]);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertEquals(HDF5ObjectType.DATASET, reader.getObjectType(floatDatasetName));
        assertTrue(reader.readFloatArray(floatDatasetName).length == 0);
        assertTrue(reader.readDoubleArray(doubleDatasetName).length == 0);
        assertTrue(reader.readByteArray(byteDatasetName).length == 0);
        assertTrue(reader.readShortArray(shortDatasetName).length == 0);
        assertTrue(reader.readIntArray(intDatasetName).length == 0);
        assertTrue(reader.readLongArray(longDatasetName).length == 0);
        reader.close();
    }

    @Test
    public void testEmptyVectorDataSetsCompact()
    {
        final File datasetFile = new File(workingDirectory, "emptyVectorDatasetsCompact.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/float";
        writer.writeFloatArrayCompact(floatDatasetName, new float[0]);
        final String doubleDatasetName = "/double";
        writer.writeDoubleArrayCompact(doubleDatasetName, new double[0]);
        final String byteDatasetName = "byte";
        writer.writeByteArrayCompact(byteDatasetName, new byte[0]);
        final String shortDatasetName = "/short";
        writer.writeShortArrayCompact(shortDatasetName, new short[0]);
        final String intDatasetName = "/int";
        writer.writeIntArrayCompact(intDatasetName, new int[0]);
        final String longDatasetName = "/long";
        writer.writeLongArrayCompact(longDatasetName, new long[0]);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertEquals(HDF5ObjectType.DATASET, reader.getObjectType(floatDatasetName));
        assertTrue(reader.readFloatArray(floatDatasetName).length == 0);
        assertTrue(reader.readDoubleArray(doubleDatasetName).length == 0);
        assertTrue(reader.readByteArray(byteDatasetName).length == 0);
        assertTrue(reader.readShortArray(shortDatasetName).length == 0);
        assertTrue(reader.readIntArray(intDatasetName).length == 0);
        assertTrue(reader.readLongArray(longDatasetName).length == 0);
        reader.close();
    }

    @Test
    public void testEmptyMatrixDataSets()
    {
        final File datasetFile = new File(workingDirectory, "emptyMatrixDatasets.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String floatDatasetName = "/float";
        writer.writeFloatMatrix(floatDatasetName, new float[0][0]);
        final String doubleDatasetName = "/double";
        writer.writeDoubleMatrix(doubleDatasetName, new double[1][0]);
        final String byteDatasetName = "byte";
        writer.writeByteMatrix(byteDatasetName, new byte[2][0]);
        final String shortDatasetName = "/short";
        writer.writeShortMatrix(shortDatasetName, new short[3][0]);
        final String intDatasetName = "/int";
        writer.writeIntMatrix(intDatasetName, new int[4][0]);
        final String longDatasetName = "/long";
        writer.writeLongMatrix(longDatasetName, new long[5][0]);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(isEmpty(reader.readFloatMatrix(floatDatasetName)));
        assertTrue(isEmpty(reader.readDoubleMatrix(doubleDatasetName)));
        assertTrue(isEmpty(reader.readByteMatrix(byteDatasetName)));
        assertTrue(isEmpty(reader.readShortMatrix(shortDatasetName)));
        assertTrue(isEmpty(reader.readIntMatrix(intDatasetName)));
        assertTrue(isEmpty(reader.readLongMatrix(longDatasetName)));
        reader.close();
    }

    @Test
    public void testEmptyMatrixDataSetsContiguous()
    {
        final File datasetFile = new File(workingDirectory, "emptyMatrixDatasetsContiguous.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer =
                new HDF5WriterConfig(datasetFile).dontUseExtendableDataTypes().writer();
        final String floatDatasetName = "/float";
        writer.writeFloatMatrix(floatDatasetName, new float[0][0]);
        final String doubleDatasetName = "/double";
        writer.writeDoubleMatrix(doubleDatasetName, new double[1][0]);
        final String byteDatasetName = "byte";
        writer.writeByteMatrix(byteDatasetName, new byte[2][0]);
        final String shortDatasetName = "/short";
        writer.writeShortMatrix(shortDatasetName, new short[3][0]);
        final String intDatasetName = "/int";
        writer.writeIntMatrix(intDatasetName, new int[4][0]);
        final String longDatasetName = "/long";
        writer.writeLongMatrix(longDatasetName, new long[5][0]);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(isEmpty(reader.readFloatMatrix(floatDatasetName)));
        assertTrue(isEmpty(reader.readDoubleMatrix(doubleDatasetName)));
        assertTrue(isEmpty(reader.readByteMatrix(byteDatasetName)));
        assertTrue(isEmpty(reader.readShortMatrix(shortDatasetName)));
        assertTrue(isEmpty(reader.readIntMatrix(intDatasetName)));
        assertTrue(isEmpty(reader.readLongMatrix(longDatasetName)));
        reader.close();
    }

    @Test
    public void testOverwriteVectorIncreaseSize()
    {
        final File datasetFile = new File(workingDirectory, "resizableVector.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "/vector";
        final float[] firstVector = new float[]
            { 1f, 2f, 3f };
        writer.writeFloatArray(dsName, firstVector);
        writer.close();
        writer = new HDF5WriterConfig(datasetFile).writer();
        final float[] secondVector = new float[]
            { 1f, 2f, 3f, 4f };
        writer.writeFloatArray(dsName, secondVector);
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final float[] vectorRead = reader.readFloatArray(dsName);
        reader.close();
        assertTrue(Arrays.equals(secondVector, vectorRead));
    }

    @Test
    public void testOverwriteWithEmptyVector()
    {
        final File datasetFile = new File(workingDirectory, "overwriteVector1.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "/vector";
        final byte[] firstVector = new byte[]
            { 1, 2, 3 };
        writer.writeByteArray(dsName, firstVector);
        writer.close();
        writer = new HDF5WriterConfig(datasetFile).writer();
        final byte[] emptyVector = new byte[0];
        writer.writeByteArray(dsName, emptyVector);
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final byte[] vectorRead = reader.readByteArray(dsName);
        reader.close();
        assertTrue(Arrays.equals(emptyVector, vectorRead));
    }

    @Test
    public void testOverwriteEmptyVectorWithNonEmptyVector()
    {
        final File datasetFile = new File(workingDirectory, "overwriteVector2.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "/vector";
        final byte[] emptyVector = new byte[0];
        writer.writeByteArray(dsName, emptyVector);
        writer.close();
        writer = new HDF5WriterConfig(datasetFile).writer();
        final byte[] nonEmptyVector = new byte[]
            { 1 };
        writer.writeByteArray(dsName, nonEmptyVector);
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final byte[] vectorRead = reader.readByteArray(dsName);
        reader.close();
        assertTrue(Arrays.equals(nonEmptyVector, vectorRead));
    }

    @Test
    public void testDeleteVector()
    {
        final File datasetFile = new File(workingDirectory, "deleteVector.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        try
        {
            final String dsName = "/vector";
            final byte[] firstVector = new byte[]
                { 1, 2, 3 };
            writer.writeByteArray(dsName, firstVector);
            writer.close();
            writer = new HDF5WriterConfig(datasetFile).writer();
            writer.delete(dsName.substring(1));
        } finally
        {
            writer.close();
        }
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        try
        {
            final List<String> members = reader.getAllGroupMembers("/");
            assertEquals(1, members.size());
            assertEquals("__DATA_TYPES__", members.get(0));
        } finally
        {
            reader.close();
        }
    }

    @Test
    public void testDeleteGroup()
    {
        final File datasetFile = new File(workingDirectory, "deleteGroup.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        try
        {
            final String groupName = "/group";
            final String dsName = groupName + "/vector";
            final byte[] firstVector = new byte[]
                { 1, 2, 3 };
            writer.writeByteArray(dsName, firstVector);
            writer.close();
            writer = new HDF5WriterConfig(datasetFile).writer();
            writer.delete(groupName);
        } finally
        {
            writer.close();
        }
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        try
        {
            final List<String> members = reader.getAllGroupMembers("/");
            assertEquals(1, members.size());
            assertEquals("__DATA_TYPES__", members.get(0));
            assertEquals(0, reader.getGroupMembers("/").size());
        } finally
        {
            reader.close();
        }
    }

    @Test
    public void testOverwriteWithEmptyString()
    {
        final File datasetFile = new File(workingDirectory, "overwriteString.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "/string";
        writer.writeString(dsName, "non-empty");
        writer.close();
        writer = new HDF5WriterConfig(datasetFile).writer();
        writer.writeString(dsName, "");
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final String stringRead = reader.readString(dsName);
        reader.close();
        assertEquals("", stringRead);
    }

    @Test
    public void testOverwriteMatrixIncreaseSize()
    {
        final File datasetFile = new File(workingDirectory, "resizableMatrix.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "/matrix";
        final float[][] firstMatrix = new float[][]
            {
                { 1f, 2f, 3f },
                { 4f, 5f, 6f } };
        writer.writeFloatMatrix(dsName, firstMatrix);
        writer.close();
        writer = new HDF5WriterConfig(datasetFile).writer();
        final float[][] secondMatrix = new float[][]
            {
                { 1f, 2f, 3f, 4f },
                { 5f, 6f, 7f, 8f },
                { 9f, 10f, 11f, 12f } };
        writer.writeFloatMatrix(dsName, secondMatrix);
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final float[][] matrixRead = reader.readFloatMatrix(dsName);
        reader.close();
        assertMatrixEquals(secondMatrix, matrixRead);
    }

    @Test
    public void testOverwriteStringVectorDecreaseSize()
    {
        final File datasetFile = new File(workingDirectory, "resizableStringVector.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String dsName = "/vector";
        final String[] firstVector = new String[]
            { "a", "b", "c" };
        writer.writeStringArray(dsName, firstVector);
        writer.close();
        writer = new HDF5WriterConfig(datasetFile).writer();
        final String[] secondVector = new String[]
            { "a", "b" };
        writer.writeStringArray(dsName, secondVector);
        writer.close();
        HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final String[] vectorRead = reader.readStringArray(dsName);
        reader.close();
        assertTrue(Arrays.equals(secondVector, vectorRead));
    }

    private static boolean isEmpty(Object matrix)
    {
        Object maybeAnArray = matrix;
        do
        {
            if (Array.getLength(maybeAnArray) == 0)
            {
                return true;
            }
            maybeAnArray = Array.get(maybeAnArray, 0);
        } while (maybeAnArray.getClass().isArray());
        return false;
    }

    @Test
    public void testTimestamps()
    {
        final File datasetFile = new File(workingDirectory, "timestamps.h5");
        final String timeStampDS = "prehistoric";
        final long timestampValue = 10000L;
        final String noTimestampDS = "notatimestamp";
        final long someLong = 173756123L;
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        writer.writeTimeStamp(timeStampDS, timestampValue);
        writer.writeLong(noTimestampDS, someLong);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertEquals(HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH, reader
                .tryGetTypeVariant(timeStampDS));
        final HDF5DataSetInformation info = reader.getDataSetInformation(timeStampDS);
        assertTrue(info.isScalar());
        assertEquals(StorageLayout.COMPACT, info.getStorageLayout());
        assertNull(info.tryGetChunkSizes());
        assertEquals(HDF5DataClass.INTEGER, info.getTypeInformation().getDataClass());
        assertTrue(info.isTimeStamp());
        assertFalse(info.isTimeDuration());
        assertEquals(HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH, info
                .tryGetTypeVariant());
        assertEquals(timestampValue, reader.readTimeStamp(timeStampDS));
        assertEquals(timestampValue, reader.readDate(timeStampDS).getTime());
        try
        {
            reader.readTimeStamp(noTimestampDS);
            fail("Failed to detect non-timestamp value.");
        } catch (HDF5JavaException ex)
        {
            if (ex.getMessage().contains("not a time stamp") == false)
            {
                throw ex;
            }
            // That is what we expect.
        }
        reader.close();
    }

    @Test
    public void testTimestampArray()
    {
        final File datasetFile = new File(workingDirectory, "timestampArray.h5");
        final String timeSeriesDS = "/some/timeseries";
        final long[] timeSeries = new long[10];
        for (int i = 0; i < timeSeries.length; ++i)
        {
            timeSeries[i] = i * 10000L;
        }
        final long[] notATimeSeries = new long[100];
        final String noTimeseriesDS = "nota/timeseries";
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        writer.writeTimeStampArray(timeSeriesDS, timeSeries);
        writer.writeLongArray(noTimeseriesDS, notATimeSeries);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final HDF5DataSetInformation info = reader.getDataSetInformation(timeSeriesDS);
        assertEquals(HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH, info
                .tryGetTypeVariant());
        assertChunkSizes(info, 10);
        assertTrue(Arrays.equals(timeSeries, reader.readTimeStampArray(timeSeriesDS)));
        final Date[] datesRead = reader.readDateArray(timeSeriesDS);
        final long[] timeStampsRead = new long[datesRead.length];
        for (int i = 0; i < timeStampsRead.length; ++i)
        {
            timeStampsRead[i] = datesRead[i].getTime();
        }
        assertTrue(Arrays.equals(timeSeries, timeStampsRead));
        try
        {
            reader.readTimeStampArray(noTimeseriesDS);
            fail("Failed to detect non-timestamp array.");
        } catch (HDF5JavaException ex)
        {
            if (ex.getMessage().contains("not a time stamp") == false)
            {
                throw ex;
            }
            // That is what we expect.
        }
        reader.close();
    }

    @Test
    public void testTimestampArrayChunked()
    {
        final File datasetFile = new File(workingDirectory, "timestampArrayChunked.h5");
        final String timeSeriesDS = "/some/timeseries";
        final long[] timeSeries = new long[10];
        for (int i = 0; i < timeSeries.length; ++i)
        {
            timeSeries[i] = i * 10000L;
        }
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        writer.createTimeStampArray(timeSeriesDS, 100, 10, true);
        for (int i = 0; i < 10; ++i)
        {
            writer.writeTimeStampArrayBlock(timeSeriesDS, timeSeries, i);
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final HDF5DataSetInformation info = reader.getDataSetInformation(timeSeriesDS);
        assertEquals(HDF5DataTypeVariant.TIMESTAMP_MILLISECONDS_SINCE_START_OF_THE_EPOCH, info
                .tryGetTypeVariant());
        assertChunkSizes(info, 10);
        for (int i = 0; i < 10; ++i)
        {
            assertTrue(Arrays.equals(timeSeries, reader
                    .readTimeStampArrayBlock(timeSeriesDS, 10, i)));
        }
        reader.close();
    }

    @Test
    public void testTimeDurations()
    {
        final File datasetFile = new File(workingDirectory, "timedurations.h5");
        final String timeDurationDS = "someDuration";
        final long timeDurationInSeconds = 10000L;
        final long timeDurationInMilliSeconds = 10000L * 1000L;
        final long timeDurationInHoursRounded = 3L;
        final String noTimestampDS = "notatimeduration";
        final long someLong = 173756123L;
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        writer.writeTimeDuration(timeDurationDS, timeDurationInSeconds, HDF5TimeUnit.SECONDS);
        writer.writeLong(noTimestampDS, someLong);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final HDF5DataSetInformation info = reader.getDataSetInformation(timeDurationDS);
        assertTrue(info.isScalar());
        assertEquals(StorageLayout.COMPACT, info.getStorageLayout());
        assertNull(info.tryGetChunkSizes());
        assertEquals(HDF5DataClass.INTEGER, info.getTypeInformation().getDataClass());
        assertTrue(info.isTimeDuration());
        assertFalse(info.isTimeStamp());
        assertEquals(HDF5TimeUnit.SECONDS, reader.tryGetTimeUnit(timeDurationDS));
        assertEquals(HDF5DataTypeVariant.TIME_DURATION_SECONDS, info.tryGetTypeVariant());
        assertEquals(HDF5TimeUnit.SECONDS, info.tryGetTimeUnit());
        assertEquals(timeDurationInSeconds, reader.readTimeDuration(timeDurationDS,
                HDF5TimeUnit.SECONDS));
        assertEquals(timeDurationInMilliSeconds, reader.readTimeDuration(timeDurationDS,
                HDF5TimeUnit.MILLISECONDS));
        assertEquals(timeDurationInHoursRounded, reader.readTimeDuration(timeDurationDS,
                HDF5TimeUnit.HOURS));
        try
        {
            reader.readTimeDuration(noTimestampDS, HDF5TimeUnit.HOURS);
            fail("Failed to detect non-timeduration value.");
        } catch (HDF5JavaException ex)
        {
            if (ex.getMessage().contains("not a time duration") == false)
            {
                throw ex;
            }
            // That is what we expect.
        }
        reader.close();
    }

    @Test
    public void testSmallTimeDurations()
    {
        final File datasetFile = new File(workingDirectory, "smalltimedurations.h5");
        final String timeDurationDS = "someDuration";
        final short timeDurationInSeconds = 10000;
        final long timeDurationInMilliSeconds = 10000L * 1000L;
        final long timeDurationInHoursRounded = 3L;
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        writer.writeShort(timeDurationDS, timeDurationInSeconds);
        writer.addTypeVariant(timeDurationDS, HDF5TimeUnit.SECONDS.getTypeVariant());
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final HDF5DataSetInformation info = reader.getDataSetInformation(timeDurationDS);
        assertTrue(info.isScalar());
        assertEquals(StorageLayout.COMPACT, info.getStorageLayout());
        assertNull(info.tryGetChunkSizes());
        assertEquals(HDF5DataClass.INTEGER, info.getTypeInformation().getDataClass());
        assertEquals(HDFNativeData.SHORT_SIZE, info.getTypeInformation().getElementSize());
        assertTrue(info.isTimeDuration());
        assertFalse(info.isTimeStamp());
        assertEquals(HDF5TimeUnit.SECONDS, reader.tryGetTimeUnit(timeDurationDS));
        assertEquals(HDF5DataTypeVariant.TIME_DURATION_SECONDS, info.tryGetTypeVariant());
        assertEquals(HDF5TimeUnit.SECONDS, info.tryGetTimeUnit());
        assertEquals(timeDurationInSeconds, reader.readTimeDuration(timeDurationDS,
                HDF5TimeUnit.SECONDS));
        assertEquals(timeDurationInMilliSeconds, reader.readTimeDuration(timeDurationDS,
                HDF5TimeUnit.MILLISECONDS));
        assertEquals(timeDurationInHoursRounded, reader.readTimeDuration(timeDurationDS,
                HDF5TimeUnit.HOURS));
        reader.close();
    }

    @Test
    public void testTimeDurationArrayChunked()
    {
        final File datasetFile = new File(workingDirectory, "timeDurationArrayChunked.h5");
        final String timeDurationSeriesDS = "/some/timeseries";
        final long[] timeDurationSeries = new long[10];
        for (int i = 0; i < timeDurationSeries.length; ++i)
        {
            timeDurationSeries[i] = i * 10000L;
        }
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        writer.createTimeDurationArray(timeDurationSeriesDS, 100, 10, HDF5TimeUnit.MILLISECONDS,
                true);
        for (int i = 0; i < 10; ++i)
        {
            writer.writeTimeDurationArrayBlock(timeDurationSeriesDS, timeDurationSeries, i,
                    HDF5TimeUnit.MILLISECONDS);
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        final HDF5DataSetInformation info = reader.getDataSetInformation(timeDurationSeriesDS);
        assertEquals(HDF5DataTypeVariant.TIME_DURATION_MILLISECONDS, info.tryGetTypeVariant());
        assertChunkSizes(info, 10);
        HDF5Reader.convertTimeDurations(HDF5TimeUnit.MICROSECONDS, HDF5TimeUnit.MILLISECONDS,
                timeDurationSeries);
        for (int i = 0; i < 10; ++i)
        {
            assertTrue(Arrays.equals(timeDurationSeries, reader.readTimeDurationArrayBlock(
                    timeDurationSeriesDS, 10, i, HDF5TimeUnit.MICROSECONDS)));
        }
        reader.close();
    }

    @Test
    public void testAttributes()
    {
        final File datasetFile = new File(workingDirectory, "attributes.h5");
        datasetFile.delete();
        assertFalse(datasetFile.exists());
        datasetFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(datasetFile).writer();
        final String datasetName = "SomeDataSet";
        writer.writeIntArray(datasetName, new int[0]);
        final String booleanAttributeName = "Boolean Attribute";
        final boolean booleanAttributeValueWritten = true;
        writer.addBooleanAttribute(datasetName, booleanAttributeName, booleanAttributeValueWritten);
        assertTrue(writer.hasAttribute(datasetName, booleanAttributeName));
        final String integerAttributeName = "Integer Attribute";
        final int integerAttributeValueWritten = 17;
        writer.addIntAttribute(datasetName, integerAttributeName, integerAttributeValueWritten);
        final String stringAttributeName = "String Attribute";
        final String stringAttributeValueWritten = "Some String Value";
        writer.addStringAttribute(datasetName, stringAttributeName, stringAttributeValueWritten);
        final String enumAttributeName = "Enum Attribute";
        final HDF5EnumerationType enumType = writer.getEnumType("MyEnum", new String[]
            { "ONE", "TWO", "THREE" }, false);
        final HDF5EnumerationValue enumAttributeValueWritten =
                new HDF5EnumerationValue(enumType, "TWO");
        writer.addEnumAttribute(datasetName, enumAttributeName, enumAttributeValueWritten);
        final String volatileAttributeName = "Some Volatile Attribute";
        writer.addIntAttribute(datasetName, volatileAttributeName, 21);
        writer.deleteAttribute(datasetName, volatileAttributeName);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(datasetFile).reader();
        assertTrue(reader.hasAttribute(datasetName, booleanAttributeName));
        final boolean booleanAttributeValueRead =
                reader.getBooleanAttribute(datasetName, booleanAttributeName);
        assertEquals(booleanAttributeValueWritten, booleanAttributeValueRead);
        final int integerAttributeValueRead =
                reader.getIntegerAttribute(datasetName, integerAttributeName);
        assertEquals(integerAttributeValueWritten, integerAttributeValueRead);
        HDF5DataTypeInformation info =
                reader.getAttributeInformation(datasetName, integerAttributeName);
        assertEquals(HDF5DataClass.INTEGER, info.getDataClass());
        assertEquals(4, info.getElementSize());
        final String stringAttributeValueRead =
                reader.getStringAttribute(datasetName, stringAttributeName);
        assertEquals(stringAttributeValueWritten, stringAttributeValueRead);
        final HDF5EnumerationValue enumAttributeValueRead =
                reader.getEnumAttribute(datasetName, enumAttributeName);
        assertEquals(enumAttributeValueWritten.getValue(), enumAttributeValueRead.getValue());
        assertFalse(reader.hasAttribute(datasetName, volatileAttributeName));
        reader.close();
    }

    @Test
    public void testCreateDataTypes()
    {
        final File file = new File(workingDirectory, "types.h5");
        final String enumName = "TestEnum";
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        try
        {
            final List<String> initialDataTypes = writer.getGroupMembers(HDF5Utils.DATATYPE_GROUP);

            writer.getEnumType(enumName, new String[]
                { "ONE", "TWO", "THREE" }, false);
            final Set<String> dataTypes =
                    new HashSet<String>(writer.getGroupMembers(HDF5Utils.DATATYPE_GROUP));
            assertEquals(initialDataTypes.size() + 1, dataTypes.size());
            assertTrue(dataTypes.contains(HDF5Utils.ENUM_PREFIX + enumName));
        } finally
        {
            writer.close();
        }
    }

    @Test
    public void testGroups()
    {
        final File groupFile = new File(workingDirectory, "groups.h5");
        groupFile.delete();
        assertFalse(groupFile.exists());
        groupFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(groupFile).writer();
        final String groupName1 = "/group";
        final String groupName2 = "/group2";
        final String groupName4 = "/dataSetGroup";
        final String groupName5 = "/group5";
        final String dataSetName = groupName4 + "/dataset";
        writer.createGroup(groupName1);
        writer.createGroup(groupName2);
        writer.writeByteArray(dataSetName, new byte[]
            { 1 });
        assertTrue(writer.isGroup(groupName1));
        assertTrue(writer.isGroup(groupName2));
        assertTrue(writer.isGroup(groupName4));
        assertFalse(writer.isGroup(dataSetName));
        assertFalse(writer.isGroup(groupName5));
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(groupFile).reader();
        assertTrue(reader.isGroup(groupName1));
        assertEquals(HDF5ObjectType.GROUP, reader.getObjectType(groupName1));
        assertTrue(reader.isGroup(groupName4));
        assertEquals(HDF5ObjectType.GROUP, reader.getObjectType(groupName4));
        assertFalse(reader.isGroup(dataSetName));
        reader.close();
    }

    @Test
    public void testGetObjectType()
    {
        final File file = new File(workingDirectory, "typeInfo.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        writer.writeBoolean("/some/flag", false);
        assertEquals(HDF5ObjectType.GROUP, writer.getObjectType("/some"));
        assertEquals(HDF5ObjectType.DATASET, writer.getObjectType("/some/flag"));
        assertFalse(writer.exists("non_existent"));
        assertEquals(HDF5ObjectType.NONEXISTENT, writer.getObjectType("non_existent"));
        writer.close();
    }

    @Test(expectedExceptions = HDF5JavaException.class)
    public void testGetLinkInformationFailed()
    {
        final File file = new File(workingDirectory, "linkInfo.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        try
        {
            assertFalse(writer.exists("non_existent"));
            writer.getLinkInformation("non_existent").checkExists();
        } finally
        {
            writer.close();
        }
    }

    @Test
    public void testGetDataSetInformation()
    {
        final File file = new File(workingDirectory, "dsInfo.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        writer.writeShortMatrix("ds", new short[][]
            {
                { (short) 1, (short) 2, (short) 3 },
                { (short) 4, (short) 5, (short) 6 } });
        final String s = "this is a string";
        writer.writeString("stringDS", s);
        writer.writeStringVariableLength("stringDSVL", s);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        final HDF5DataSetInformation info = reader.getDataSetInformation("ds");
        assertEquals(HDF5DataClass.INTEGER, info.getTypeInformation().getDataClass());
        assertEquals(2, info.getTypeInformation().getElementSize());
        assertEquals(2, info.getRank());
        assertFalse(info.isScalar());
        assertEquals(2, info.getDimensions()[0]);
        assertEquals(3, info.getDimensions()[1]);
        assertChunkSizes(info, HDF5Utils.MIN_CHUNK_SIZE, HDF5Utils.MIN_CHUNK_SIZE);
        final HDF5DataSetInformation stringInfo = reader.getDataSetInformation("stringDS");
        assertEquals(HDF5DataClass.STRING, stringInfo.getTypeInformation().getDataClass());
        assertEquals(s.length() + 1, stringInfo.getTypeInformation().getElementSize());
        assertEquals(1, stringInfo.getDimensions().length);
        assertEquals(1, stringInfo.getDimensions()[0]);
        assertEquals(1, stringInfo.getMaxDimensions().length);
        assertEquals(1, stringInfo.getMaxDimensions()[0]);
        assertEquals(StorageLayout.COMPACT, stringInfo.getStorageLayout());
        assertNull(stringInfo.tryGetChunkSizes());
        final HDF5DataSetInformation stringInfoVL = reader.getDataSetInformation("stringDSVL");
        assertEquals(HDF5DataClass.STRING, stringInfoVL.getTypeInformation().getDataClass());
        assertEquals(1, stringInfoVL.getTypeInformation().getElementSize());
        assertEquals(1, stringInfoVL.getDimensions().length);
        assertEquals(StorageLayout.VARIABLE_LENGTH, stringInfoVL.getStorageLayout());
        assertNull(stringInfoVL.tryGetChunkSizes());
        assertEquals(HDF5Constants.H5T_VARIABLE, stringInfoVL.getDimensions()[0]);
        assertEquals(1, stringInfoVL.getMaxDimensions().length);
        assertEquals(HDF5Constants.H5T_VARIABLE, stringInfoVL.getMaxDimensions()[0]);
        reader.close();
    }

    @Test(expectedExceptions = HDF5SymbolTableException.class)
    public void testGetDataSetInformationFailed()
    {
        final File file = new File(workingDirectory, "dsInfo.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        try
        {
            assertFalse(writer.exists("non_existent"));
            writer.getDataSetInformation("non_existent");
        } finally
        {
            writer.close();
        }
    }

    @Test
    public void testGetGroupMemberInformation()
    {
        final File groupFile = new File(workingDirectory, "groupMemberInformation.h5");
        groupFile.delete();
        assertFalse(groupFile.exists());
        groupFile.deleteOnExit();
        final String groupName1 = "/group";
        final String groupName2 = "/dataSetGroup";
        final String dataSetName = groupName2 + "/dataset";
        final String dataSetName2 = "ds2";
        final String linkName = "/link";
        final HDF5Writer writer = new HDF5WriterConfig(groupFile).writer();
        try
        {
            writer.createGroup(groupName1);
            writer.writeByteArray(dataSetName, new byte[]
                { 1 });
            writer.writeString(dataSetName2, "abc");
            writer.createSoftLink(dataSetName2, linkName);
        } finally
        {
            writer.close();
        }
        final HDF5Reader reader = new HDF5ReaderConfig(groupFile).reader();
        final Map<String, HDF5LinkInformation> map = new HashMap<String, HDF5LinkInformation>();
        for (HDF5LinkInformation info : reader.getAllGroupMemberInformation("/", false))
        {
            map.put(info.getPath(), info);
        }
        HDF5LinkInformation info;
        assertEquals(5, map.size());
        info = map.get(groupName1);
        assertNotNull(info);
        assertTrue(info.exists());
        assertEquals(HDF5ObjectType.GROUP, info.getType());
        assertNull(info.tryGetSymbolicLinkTarget());
        info = map.get(groupName2);
        assertNotNull(info);
        assertTrue(info.exists());
        assertEquals(HDF5ObjectType.GROUP, info.getType());
        assertNull(info.tryGetSymbolicLinkTarget());
        info = map.get("/" + dataSetName2);
        assertNotNull(info);
        assertTrue(info.exists());
        assertEquals(HDF5ObjectType.DATASET, info.getType());
        assertNull(info.tryGetSymbolicLinkTarget());
        info = map.get(linkName);
        assertNotNull(info);
        assertTrue(info.exists());
        assertEquals(HDF5ObjectType.SOFT_LINK, info.getType());
        assertNull(info.tryGetSymbolicLinkTarget());

        map.clear();
        for (HDF5LinkInformation info2 : reader.getGroupMemberInformation("/", true))
        {
            map.put(info2.getPath(), info2);
        }
        assertEquals(4, map.size());
        info = map.get(groupName1);
        assertNotNull(info);
        assertTrue(info.exists());
        assertEquals(HDF5ObjectType.GROUP, info.getType());
        assertNull(info.tryGetSymbolicLinkTarget());
        info = map.get(groupName2);
        assertNotNull(info);
        assertTrue(info.exists());
        assertEquals(HDF5ObjectType.GROUP, info.getType());
        assertNull(info.tryGetSymbolicLinkTarget());
        info = map.get("/" + dataSetName2);
        assertNotNull(info);
        assertTrue(info.exists());
        assertEquals(HDF5ObjectType.DATASET, info.getType());
        assertNull(info.tryGetSymbolicLinkTarget());
        info = map.get(linkName);
        assertNotNull(info);
        assertTrue(info.exists());
        assertEquals(HDF5ObjectType.SOFT_LINK, info.getType());
        assertEquals(dataSetName2, info.tryGetSymbolicLinkTarget());

        reader.close();
    }

    @Test
    public void testHardLink()
    {
        final File linkFile = new File(workingDirectory, "hardLink.h5");
        linkFile.delete();
        assertFalse(linkFile.exists());
        linkFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(linkFile).writer();
        final String str = "BlaBlub";
        writer.writeString("/data/set", str);
        writer.createHardLink("/data/set", "/data/link");
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(linkFile).reader();
        assertEquals(HDF5ObjectType.DATASET, reader.getObjectType("/data/link"));
        assertEquals(str, reader.readString("/data/link"));
        reader.close();
    }

    @Test
    public void testSoftLink()
    {
        final File linkFile = new File(workingDirectory, "softLink.h5");
        linkFile.delete();
        assertFalse(linkFile.exists());
        linkFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(linkFile).writer();
        writer.writeBoolean("/data/set", true);
        writer.createSoftLink("/data/set", "/data/link");
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(linkFile).reader();
        assertEquals(HDF5ObjectType.SOFT_LINK, reader.getObjectType("/data/link"));
        assertEquals("/data/set", reader.getLinkInformation("/data/link")
                .tryGetSymbolicLinkTarget());
        reader.close();
    }

    @Test
    public void testUpdateSoftLink()
    {
        final File file = new File(workingDirectory, "updateSoftLink.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        final long now = System.currentTimeMillis();
        final String dataSetName1 = "creationTime1";
        final String dataSetName2 = "creationTime2";
        final String linkName = "time";
        writer.writeTimeStamp(dataSetName1, now);
        writer.writeTimeStamp(dataSetName2, now);
        writer.createSoftLink(dataSetName1, linkName);
        writer.createOrUpdateSoftLink(dataSetName2, linkName);
        try
        {
            writer.createOrUpdateSoftLink(dataSetName1, dataSetName2);
        } catch (HDF5LibraryException ex)
        {
            assertEquals(HDF5Constants.H5E_EXISTS, ex.getMinorErrorNumber());
        }
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        assertEquals(dataSetName2, reader.getLinkInformation(linkName).tryGetSymbolicLinkTarget());
        reader.close();
    }

    @Test
    public void testBrokenSoftLink()
    {
        final File linkFile = new File(workingDirectory, "brokenSoftLink.h5");
        linkFile.delete();
        assertFalse(linkFile.exists());
        linkFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(linkFile).writer();
        writer.createSoftLink("/does/not/exist", "/linkToNowhere");
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(linkFile).reader();
        assertEquals(HDF5ObjectType.SOFT_LINK, reader.getObjectType("/linkToNowhere"));
        assertEquals("/does/not/exist", reader.getLinkInformation("/linkToNowhere")
                .tryGetSymbolicLinkTarget());
        reader.close();
    }

    @Test
    public void testNullOnGetSymbolicLinkTargetForNoLink()
    {
        final File noLinkFile = new File(workingDirectory, "noLink.h5");
        noLinkFile.delete();
        assertFalse(noLinkFile.exists());
        noLinkFile.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(noLinkFile).writer();
        writer.writeBoolean("/data/set", true);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(noLinkFile).reader();
        try
        {
            assertNull(reader.getLinkInformation("/data/set").tryGetSymbolicLinkTarget());
        } finally
        {
            reader.close();
        }
    }

    @Test
    public void testExternalLink()
    {
        final File fileToLinkTo = new File(workingDirectory, "fileToLinkTo.h5");
        fileToLinkTo.delete();
        assertFalse(fileToLinkTo.exists());
        fileToLinkTo.deleteOnExit();
        final HDF5Writer writer1 = new HDF5WriterConfig(fileToLinkTo).writer();
        final String dataSetName = "/data/set";
        final String dataSetValue = "Some data set value...";
        writer1.writeString(dataSetName, dataSetValue);
        writer1.close();
        final File linkFile = new File(workingDirectory, "externalLink.h5");
        linkFile.delete();
        assertFalse(linkFile.exists());
        linkFile.deleteOnExit();
        final HDF5Writer writer2 = new HDF5WriterConfig(linkFile).useLatestFileFormat().writer();
        final String linkName = "/data/link";
        writer2.createExternalLink(fileToLinkTo.getPath(), dataSetName, linkName);
        writer2.close();
        final HDF5Reader reader = new HDF5ReaderConfig(linkFile).reader();
        assertEquals(HDF5ObjectType.EXTERNAL_LINK, reader.getObjectType(linkName));
        assertEquals(dataSetValue, reader.readString(linkName));
        final String expectedLink =
                OSUtilities.isWindows() ? "EXTERNAL::targets\\unit-test-wd\\hdf5-roundtrip-wd\\fileToLinkTo.h5::/data/set"
                        : "EXTERNAL::targets/unit-test-wd/hdf5-roundtrip-wd/fileToLinkTo.h5::/data/set";
        assertEquals(expectedLink, reader.getLinkInformation(linkName).tryGetSymbolicLinkTarget());
        reader.close();
    }

    @Test
    public void testEnum()
    {
        final File file = new File(workingDirectory, "enum.h5");
        final String enumTypeName = "testEnumType";
        final String dsName = "/testEnum";
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5EnumerationType type = writer.getEnumType(enumTypeName, new String[]
            { "ONE", "TWO", "THREE" }, false);
        writer.writeEnum(dsName, new HDF5EnumerationValue(type, "THREE"));
        // That is wrong, but we disable the check, so no exception should be thrown.
        writer.getEnumType(enumTypeName, new String[]
            { "THREE", "ONE", "TWO" }, false);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        type = reader.getEnumType(enumTypeName);
        final String valueStr = reader.readEnumAsString(dsName);
        assertEquals("THREE", valueStr);
        final HDF5EnumerationValue value = reader.readEnum(dsName);
        assertEquals("THREE", value.getValue());
        final String expectedDataTypePath =
                HDF5Utils.createDataTypePath(HDF5Utils.ENUM_PREFIX, enumTypeName);
        assertEquals(expectedDataTypePath, reader.tryGetDataTypePath(value.getType()));
        assertEquals(expectedDataTypePath, reader.tryGetDataTypePath(dsName));
        type = reader.getEnumTypeForObject(dsName);
        assertEquals("THREE", reader.readEnum(dsName, type).getValue());
        reader.close();
        final HDF5Writer writer2 = new HDF5WriterConfig(file).writer();
        type = writer2.getEnumType(enumTypeName, new String[]
            { "ONE", "TWO", "THREE" }, true);
        assertEquals("THREE", writer2.readEnum(dsName, type).getValue());
        writer2.close();
    }

    @Test
    public void testEnum16()
    {
        final File file = new File(workingDirectory, "enum16bit.h5");
        final String enumTypeName = "testEnumType16";
        final String dsName = "/testEnum";
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5EnumerationType type = createEnum16Bit(writer, enumTypeName);
        writer.writeEnum(dsName, new HDF5EnumerationValue(type, "17"));
        final String[] confusedValues = new String[type.getValueArray().length];
        System.arraycopy(confusedValues, 0, confusedValues, 1, confusedValues.length - 1);
        confusedValues[0] = "XXX";
        // This is wrong, but we disabled the check.
        writer.getEnumType(enumTypeName, confusedValues, false);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        type = reader.getEnumType(enumTypeName);
        final String valueStr = reader.readEnumAsString(dsName);
        assertEquals("17", valueStr);
        final HDF5EnumerationValue value = reader.readEnum(dsName);
        assertEquals("17", value.getValue());
        type = reader.getEnumTypeForObject(dsName);
        assertEquals("17", reader.readEnum(dsName, type).getValue());
        reader.close();
        final HDF5Writer writer2 = new HDF5WriterConfig(file).writer();
        type = writer2.getEnumType(enumTypeName, type.getValueArray(), true);
        assertEquals("17", writer2.readEnum(dsName, type).getValue());
        // That is wrong, but we disable the check, so no exception should be thrown.
        writer2.close();
    }

    @Test(expectedExceptions = HDF5JavaException.class)
    public void testConfusedEnum()
    {
        final File file = new File(workingDirectory, "confusedEnum.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5EnumerationType type = writer.getEnumType("testEnum", new String[]
            { "ONE", "TWO", "THREE" }, false);
        writer.writeEnum("/testEnum", new HDF5EnumerationValue(type, 2));
        writer.close();
        try
        {
            writer = new HDF5WriterConfig(file).writer();
            writer.getEnumType("testEnum", new String[]
                { "THREE", "ONE", "TWO" }, true);
        } finally
        {
            writer.close();
        }
    }

    @Test
    public void testEnumArray()
    {
        final File file = new File(workingDirectory, "enumArray.h5");
        final String enumTypeName = "testEnum";
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5EnumerationType enumType = writer.getEnumType(enumTypeName, new String[]
            { "ONE", "TWO", "THREE" }, false);
        HDF5EnumerationValueArray arrayWritten =
                new HDF5EnumerationValueArray(enumType, new String[]
                    { "TWO", "ONE", "THREE" });
        writer.writeEnumArray("/testEnum", arrayWritten, false);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        final HDF5EnumerationValueArray arrayRead = reader.readEnumArray("/testEnum");
        enumType = reader.getEnumTypeForObject("/testEnum");
        final HDF5EnumerationValueArray arrayRead2 = reader.readEnumArray("/testEnum", enumType);
        final String[] stringArrayRead = reader.readEnumArrayAsString("/testEnum");
        assertEquals(arrayWritten.getLength(), stringArrayRead.length);
        assertEquals(arrayWritten.getLength(), arrayRead.getLength());
        assertEquals(arrayWritten.getLength(), arrayRead2.getLength());
        for (int i = 0; i < stringArrayRead.length; ++i)
        {
            assertEquals("Index " + i, arrayWritten.getValue(i), arrayRead.getValue(i));
            assertEquals("Index " + i, arrayWritten.getValue(i), arrayRead2.getValue(i));
            assertEquals("Index " + i, arrayWritten.getValue(i), stringArrayRead[i]);
        }
        reader.close();
    }

    @Test
    public void testEnumArray16BitFromIntArray()
    {
        final File file = new File(workingDirectory, "enumArray16BitFromIntArray.h5");
        final String enumTypeName = "testEnum";
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        final HDF5EnumerationType enumType = createEnum16Bit(writer, enumTypeName);
        final int[] arrayWritten = new int[]
            { 8, 16, 722, 913, 333 };
        writer.writeEnumArray("/testEnum", new HDF5EnumerationValueArray(enumType, arrayWritten),
                false);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        final String[] stringArrayRead = reader.readEnumArrayAsString("/testEnum");
        assertEquals(arrayWritten.length, stringArrayRead.length);
        for (int i = 0; i < stringArrayRead.length; ++i)
        {
            assertEquals("Index " + i, enumType.getValues().get(arrayWritten[i]),
                    stringArrayRead[i]);
        }
        reader.close();
    }

    @Test
    public void testOpaqueType()
    {
        final File file = new File(workingDirectory, "opaqueType.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final String opaqueDataSetName = "/opaque/ds";
        final String byteArrayDataSetName = "/bytearr/ds";
        final String opaqueTag = "my opaque type";
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        final byte[] byteArrayWritten = new byte[]
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        writer.writeByteArray(byteArrayDataSetName, byteArrayWritten);
        writer.writeOpaqueByteArray(opaqueDataSetName, opaqueTag, byteArrayWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        HDF5DataSetInformation info = reader.getDataSetInformation(byteArrayDataSetName);
        assertEquals(HDF5DataClass.INTEGER, info.getTypeInformation().getDataClass());
        assertChunkSizes(info, byteArrayWritten.length);
        info = reader.getDataSetInformation(opaqueDataSetName);
        assertEquals(HDF5DataClass.OPAQUE, info.getTypeInformation().getDataClass());
        assertChunkSizes(info, byteArrayWritten.length);
        assertEquals(opaqueTag, reader.tryGetOpaqueTag(opaqueDataSetName));
        assertNull(reader.tryGetOpaqueTag(byteArrayDataSetName));
        final byte[] byteArrayRead = reader.readAsByteArray(byteArrayDataSetName);
        assertTrue(Arrays.equals(byteArrayWritten, byteArrayRead));
        final byte[] byteArrayReadOpaque = reader.readAsByteArray(opaqueDataSetName);
        assertTrue(Arrays.equals(byteArrayWritten, byteArrayReadOpaque));
        reader.close();
    }

    private HDF5EnumerationType createEnum16Bit(final HDF5Writer writer, final String enumTypeName)
    {
        final String[] enumValues = new String[1024];
        for (int i = 0; i < enumValues.length; ++i)
        {
            enumValues[i] = Integer.toString(i);
        }
        final HDF5EnumerationType enumType = writer.getEnumType(enumTypeName, enumValues, false);
        return enumType;
    }

    @Test
    public void testEnumArrayFromIntArray()
    {
        final File file = new File(workingDirectory, "enumArrayFromIntArray.h5");
        final String enumTypeName = "testEnum";
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        final HDF5EnumerationType enumType = writer.getEnumType(enumTypeName, new String[]
            { "ONE", "TWO", "THREE" }, false);
        final int[] arrayWritten =
                new int[]
                    { enumType.tryGetIndexForValue("TWO").byteValue(),
                            enumType.tryGetIndexForValue("ONE").byteValue(),
                            enumType.tryGetIndexForValue("THREE").byteValue() };
        writer.writeEnumArray("/testEnum", new HDF5EnumerationValueArray(enumType, arrayWritten),
                false);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        final String[] stringArrayRead = reader.readEnumArrayAsString("/testEnum");
        assertEquals(arrayWritten.length, stringArrayRead.length);
        for (int i = 0; i < stringArrayRead.length; ++i)
        {
            assertEquals("Index " + i, enumType.getValues().get(arrayWritten[i]),
                    stringArrayRead[i]);
        }
        reader.close();
    }

    static class Record
    {
        int a;

        float b;

        long l;

        double c;

        short d;

        boolean e;

        String f;

        HDF5EnumerationValue g;

        int[] ar;

        float[] br;

        long[] lr;

        double[] cr;

        short[] dr;

        byte[] er;

        Record(int a, float b, long l, double c, short d, boolean e, String f,
                HDF5EnumerationValue g, int[] ar, float[] br, long[] lr, double[] cr, short[] dr,
                byte[] er)
        {
            this.a = a;
            this.b = b;
            this.l = l;
            this.c = c;
            this.d = d;
            this.e = e;
            this.f = f;
            this.g = g;
            this.ar = ar;
            this.br = br;
            this.lr = lr;
            this.cr = cr;
            this.dr = dr;
            this.er = er;
        }

        Record()
        {
        }

        static HDF5CompoundMemberInformation[] getMemberInfo(HDF5EnumerationType enumType)
        {
            return HDF5CompoundMemberInformation.create(Record.class, getShuffledMapping(enumType));
        }

        static HDF5CompoundType<Record> getHDF5Type(HDF5Reader reader)
        {
            final HDF5EnumerationType enumType = reader.getEnumType("someEnumType", new String[]
                { "1", "Two", "THREE" });
            return reader.getCompoundType(null, Record.class, getMapping(enumType));
        }

        private static HDF5CompoundMemberMapping[] getMapping(HDF5EnumerationType enumType)
        {
            return new HDF5CompoundMemberMapping[]
                { mapping("a"), mapping("b"), mapping("l"), mapping("c"), mapping("d"),
                        mapping("e"), mapping("f", 3), mapping("g", enumType), mapping("ar", 3),
                        mapping("br", 2), mapping("lr", 3), mapping("cr", 1), mapping("dr", 2),
                        mapping("er", 4) };
        }

        private static HDF5CompoundMemberMapping[] getShuffledMapping(HDF5EnumerationType enumType)
        {
            return new HDF5CompoundMemberMapping[]
                { mapping("er", 4), mapping("e"), mapping("b"), mapping("br", 2),
                        mapping("g", enumType), mapping("lr", 3), mapping("c"), mapping("ar", 3),
                        mapping("a"), mapping("d"), mapping("cr", 1), mapping("f", 3),
                        mapping("dr", 2), mapping("l") };
        }

        //
        // Object
        //

        @Override
        public int hashCode()
        {
            final HashCodeBuilder builder = new HashCodeBuilder();
            builder.append(a);
            builder.append(b);
            builder.append(c);
            builder.append(d);
            builder.append(e);
            builder.append(f);
            builder.append(g);
            builder.append(ar);
            builder.append(br);
            builder.append(cr);
            builder.append(dr);
            builder.append(er);
            return builder.toHashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null || obj instanceof Record == false)
            {
                return false;
            }
            final Record that = (Record) obj;
            final EqualsBuilder builder = new EqualsBuilder();
            builder.append(a, that.a);
            builder.append(b, that.b);
            builder.append(c, that.c);
            builder.append(d, that.d);
            builder.append(e, that.e);
            builder.append(f, that.f);
            builder.append(g, that.g);
            builder.append(ar, that.ar);
            builder.append(br, that.br);
            builder.append(cr, that.cr);
            builder.append(dr, that.dr);
            builder.append(er, that.er);
            return builder.isEquals();
        }

        @Override
        public String toString()
        {
            final ToStringBuilder builder = new ToStringBuilder(this);
            builder.append(a);
            builder.append(b);
            builder.append(c);
            builder.append(d);
            builder.append(e);
            builder.append(f);
            builder.append(g);
            builder.append(ar);
            builder.append(br);
            builder.append(cr);
            builder.append(dr);
            builder.append(er);
            return builder.toString();
        }

    }

    @Test
    public void testCompound()
    {
        final File file = new File(workingDirectory, "compound.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5CompoundType<Record> compoundType = Record.getHDF5Type(writer);
        HDF5EnumerationType enumType = writer.getEnumType("someEnumType");
        final Record recordWritten =
                new Record(1, 2.0f, 100000000L, 3.0, (short) 4, true, "one",
                        new HDF5EnumerationValue(enumType, "THREE"), new int[]
                            { 1, 2, 3 }, new float[]
                            { 8.0f, -17.0f }, new long[]
                            { -10, -11, -12 }, new double[]
                            { 3.14159 }, new short[]
                            { 1000, 2000 }, new byte[]
                            { 11, 12, 13, 14 });
        writer.writeCompound("/testCompound", compoundType, recordWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        final HDF5CompoundMemberInformation[] memMemberInfo =
                Record.getMemberInfo(reader.getEnumType("someEnumType"));
        final HDF5CompoundMemberInformation[] diskMemberInfo =
                reader.getCompoundDataSetInformation("/testCompound");
        assertTrue(Arrays.equals(memMemberInfo, diskMemberInfo));
        compoundType = Record.getHDF5Type(reader);
        final Record recordRead = reader.readCompound("/testCompound", Record.getHDF5Type(reader));
        assertEquals(recordWritten, recordRead);
        reader.close();
    }

    @Test
    public void testCompoundOverflow()
    {
        final File file = new File(workingDirectory, "compoundOverflow.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5CompoundType<Record> compoundType = Record.getHDF5Type(writer);
        HDF5EnumerationType enumType = writer.getEnumType("someEnumType");
        final Record recordWritten =
                new Record(1, 2.0f, 100000000L, 3.0, (short) 4, true, "one",
                        new HDF5EnumerationValue(enumType, "THREE"), new int[]
                            { 1, 2, 3 }, new float[]
                            { 8.0f, -17.0f }, new long[]
                            { -10, -11, -12 }, new double[]
                            { 3.14159 }, new short[]
                            { 1000, 2000 }, new byte[]
                            { 11, 12, 13, 14, 0, 0, 0 });
        try
        {
            writer.writeCompound("/testCompound", compoundType, recordWritten);
            fail("Failed to detect overflow.");
        } catch (HDF5JavaException ex)
        {
            if (ex.getMessage().contains("must not exceed 4 bytes") == false)
            {
                throw ex;
            }
            // Expected.
        } finally
        {
            writer.close();
        }
    }

    static class BitFieldRecord
    {
        BitSet bs;

        BitFieldRecord(BitSet bs)
        {
            this.bs = bs;
        }

        BitFieldRecord()
        {
        }

        static HDF5CompoundMemberInformation[] getMemberInfo()
        {
            return HDF5CompoundMemberInformation.create(BitFieldRecord.class, mapping("bs", 40));
        }

        static HDF5CompoundType<BitFieldRecord> getHDF5Type(HDF5Reader reader)
        {
            return reader.getCompoundType(BitFieldRecord.class, mapping("bs", 40));
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof BitFieldRecord == false)
            {
                return false;
            }
            final BitFieldRecord that = (BitFieldRecord) obj;
            return this.bs.equals(that.bs);
        }

        @Override
        public int hashCode()
        {
            return bs.hashCode();
        }
    }

    @Test
    public void testBitFieldCompound()
    {
        final File file = new File(workingDirectory, "compoundWithBitField.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5CompoundType<BitFieldRecord> compoundType = BitFieldRecord.getHDF5Type(writer);
        final BitSet bs = new BitSet();
        bs.set(39);
        final BitFieldRecord recordWritten = new BitFieldRecord(bs);
        writer.writeCompound("/testCompound", compoundType, recordWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        final HDF5CompoundMemberInformation[] memMemberInfo = BitFieldRecord.getMemberInfo();
        final HDF5CompoundMemberInformation[] diskMemberInfo =
                reader.getCompoundDataSetInformation("/testCompound");
        assertTrue(Arrays.equals(memMemberInfo, diskMemberInfo));
        compoundType = BitFieldRecord.getHDF5Type(reader);
        final BitFieldRecord recordRead = reader.readCompound("/testCompound", compoundType);
        assertEquals(recordWritten, recordRead);
        reader.close();
    }

    @Test
    public void testCompoundArray()
    {
        final File file = new File(workingDirectory, "compoundVector.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5CompoundType<Record> compoundType = Record.getHDF5Type(writer);
        HDF5EnumerationType enumType = writer.getEnumType("someEnumType", new String[]
            { "1", "Two", "THREE" }, false);
        Record[] arrayWritten =
                new Record[]
                    {
                            new Record(1, 2.0f, 100000000L, 3.0, (short) 4, true, "one",
                                    new HDF5EnumerationValue(enumType, "THREE"), new int[]
                                        { 1, 2, 3 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(2, 3.0f, 100000000L, 4.0, (short) 5, false, "two",
                                    new HDF5EnumerationValue(enumType, "1"), new int[]
                                        { 4, 5, 6 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }), };
        writer.writeCompoundArrayCompact("/testCompound", compoundType, arrayWritten, false);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        compoundType = Record.getHDF5Type(reader);
        Record[] arrayRead = reader.readCompoundArray("/testCompound", Record.getHDF5Type(reader));
        Record firstElementRead = reader.readCompound("/testCompound", Record.getHDF5Type(reader));
        assertEquals(arrayRead[0], firstElementRead);
        for (int i = 0; i < arrayRead.length; ++i)
        {
            assertEquals("" + i, arrayWritten[i], arrayRead[i]);
        }
        reader.close();
    }

    @Test
    public void testCompoundArrayBlockWise()
    {
        final File file = new File(workingDirectory, "compoundVectorBlockWise.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5CompoundType<Record> compoundType = Record.getHDF5Type(writer);
        HDF5EnumerationType enumType = writer.getEnumType("someEnumType");
        writer.createCompoundArray("/testCompound", compoundType, 6, 3, false);
        Record[] arrayWritten1 =
                new Record[]
                    {
                            new Record(1, 2.0f, 100000000L, 3.0, (short) 4, true, "one",
                                    new HDF5EnumerationValue(enumType, "THREE"), new int[]
                                        { 1, 2, 3 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(2, 3.0f, 100000000L, 4.0, (short) 5, false, "two",
                                    new HDF5EnumerationValue(enumType, "1"), new int[]
                                        { 4, 5, 6 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(3, 3.0f, 100000000L, 5.0, (short) 6, true, "two",
                                    new HDF5EnumerationValue(enumType, "Two"), new int[]
                                        { -1, -2, -3 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }), };
        Record[] arrayWritten2 =
                new Record[]
                    {
                            new Record(4, 4.0f, 100000000L, 6.0, (short) 7, false, "two",
                                    new HDF5EnumerationValue(enumType, "Two"), new int[]
                                        { 100, 200, 300 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(5, 5.0f, 100000000L, 7.0, (short) 8, true, "two",
                                    new HDF5EnumerationValue(enumType, "THREE"), new int[]
                                        { 400, 500, 600 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(6, 6.0f, 100000000L, 8.0, (short) 9, false, "x",
                                    new HDF5EnumerationValue(enumType, "1"), new int[]
                                        { -100, -200, -300 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }), };
        writer.writeCompoundArrayBlock("/testCompound", compoundType, arrayWritten1, 0);
        writer.writeCompoundArrayBlock("/testCompound", compoundType, arrayWritten2, 1);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        compoundType = Record.getHDF5Type(reader);
        Record[] arrayRead = reader.readCompoundArrayBlock("/testCompound", compoundType, 3, 0);
        for (int i = 0; i < arrayRead.length; ++i)
        {
            assertEquals("" + i, arrayWritten1[i], arrayRead[i]);
        }
        arrayRead = reader.readCompoundArrayBlock("/testCompound", compoundType, 3, 1);
        for (int i = 0; i < arrayRead.length; ++i)
        {
            assertEquals("" + i, arrayWritten2[i], arrayRead[i]);
        }
        arrayRead = reader.readCompoundArrayBlockWithOffset("/testCompound", compoundType, 3, 1);
        for (int i = 1; i < arrayRead.length; ++i)
        {
            assertEquals("" + i, arrayWritten1[i], arrayRead[i - 1]);
        }
        assertEquals("" + (arrayRead.length - 1), arrayWritten2[0], arrayRead[arrayRead.length - 1]);
        reader.close();
    }

    @Test
    public void testCompoundMDArray()
    {
        final File file = new File(workingDirectory, "compoundMDArray.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5CompoundType<Record> compoundType = Record.getHDF5Type(writer);
        HDF5EnumerationType enumType = writer.getEnumType("someEnumType");
        final Record[] arrayWritten =
                new Record[]
                    {
                            new Record(1, 2.0f, 100000000L, 3.0, (short) 4, true, "one",
                                    new HDF5EnumerationValue(enumType, "THREE"), new int[]
                                        { 1, 2, 3 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(2, 3.0f, 100000000L, 4.0, (short) 5, false, "two",
                                    new HDF5EnumerationValue(enumType, "1"), new int[]
                                        { 4, 5, 6 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(3, 3.0f, 100000000L, 5.0, (short) 6, true, "two",
                                    new HDF5EnumerationValue(enumType, "Two"), new int[]
                                        { 7, 8, 9 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(4, 4.0f, 100000000L, 6.0, (short) 7, false, "two",
                                    new HDF5EnumerationValue(enumType, "Two"), new int[]
                                        { 10, 11, 12 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }), };
        final MDArray<Record> mdArrayWritten = new MDArray<Record>(arrayWritten, new int[]
            { 2, 2 });
        writer.writeCompoundMDArray("/testCompound", compoundType, mdArrayWritten, false);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        compoundType = Record.getHDF5Type(reader);
        final MDArray<Record> mdArrayRead =
                reader.readCompoundMDArray("/testCompound", compoundType);
        assertEquals(mdArrayWritten, mdArrayRead);
        reader.close();
    }

    @Test
    public void testCompoundMDArrayBlockWise()
    {
        final File file = new File(workingDirectory, "compoundMDArrayBlockWise.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5CompoundType<Record> compoundType = Record.getHDF5Type(writer);
        HDF5EnumerationType enumType = writer.getEnumType("someEnumType");
        writer.createCompoundMDArray("/testCompound", compoundType, new long[]
            { 2, 2 }, new int[]
            { 2, 1 }, false);
        final Record[] arrayWritten1 =
                new Record[]
                    {
                            new Record(1, 2.0f, 100000000L, 3.0, (short) 4, true, "one",
                                    new HDF5EnumerationValue(enumType, "THREE"), new int[]
                                        { 1, 2, 3 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(2, 3.0f, 100000000L, 4.0, (short) 5, false, "two",
                                    new HDF5EnumerationValue(enumType, "1"), new int[]
                                        { 2, 3, 4 }, new float[]
                                        { 8.1f, -17.1f }, new long[]
                                        { -10, -13, -12 }, new double[]
                                        { 3.1415 }, new short[]
                                        { 1000, 2001 }, new byte[]
                                        { 11, 12, 13, 17 }), };
        final Record[] arrayWritten2 =
                new Record[]
                    {
                            new Record(3, 3.0f, 100000000L, 5.0, (short) 6, true, "two",
                                    new HDF5EnumerationValue(enumType, "Two"), new int[]
                                        { 3, 4, 5 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }),
                            new Record(4, 4.0f, 100000000L, 6.0, (short) 7, false, "two",
                                    new HDF5EnumerationValue(enumType, "Two"), new int[]
                                        { 4, 5, 6 }, new float[]
                                        { 8.0f, -17.0f }, new long[]
                                        { -10, -11, -12 }, new double[]
                                        { 3.14159 }, new short[]
                                        { 1000, 2000 }, new byte[]
                                        { 11, 12, 13, 14 }), };
        final MDArray<Record> mdArrayWritten1 = new MDArray<Record>(arrayWritten1, new int[]
            { 2, 1 });
        final MDArray<Record> mdArrayWritten2 = new MDArray<Record>(arrayWritten2, new int[]
            { 2, 1 });
        writer.writeCompoundMDArrayBlock("/testCompound", compoundType, mdArrayWritten1, new long[]
            { 0, 0 });
        writer.writeCompoundMDArrayBlock("/testCompound", compoundType, mdArrayWritten2, new long[]
            { 0, 1 });
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        compoundType = Record.getHDF5Type(reader);
        final MDArray<Record> mdArrayRead1 =
                reader.readCompoundMDArrayBlock("/testCompound", compoundType, new int[]
                    { 2, 1 }, new long[]
                    { 0, 0 });
        final MDArray<Record> mdArrayRead2 =
                reader.readCompoundMDArrayBlock("/testCompound", compoundType, new int[]
                    { 2, 1 }, new long[]
                    { 0, 1 });
        assertEquals(mdArrayWritten1, mdArrayRead1);
        assertEquals(mdArrayWritten2, mdArrayRead2);
        reader.close();
    }

    static class RecordA
    {
        int a;

        double b;

        RecordA(int a, float b)
        {
            this.a = a;
            this.b = b;
        }

        RecordA()
        {
        }

        static HDF5CompoundType<RecordA> getHDF5Type(HDF5Reader reader)
        {
            return reader.getCompoundType(RecordA.class, mapping("a"), mapping("b"));
        }
    }

    static class RecordB
    {
        float a;

        long b;

        RecordB(float a, int b)
        {
            this.a = a;
            this.b = b;
        }

        RecordB()
        {
        }

        static HDF5CompoundType<RecordB> getHDF5Type(HDF5Reader reader)
        {
            return reader.getCompoundType(RecordB.class, mapping("a"), mapping("b"));
        }
    }

    @Test
    public void testConfusedCompound()
    {
        // Sparc tries conversion which doesn't work reliably on this platform and may even SEGFAULT
        if (OSUtilities.getComputerPlatform().startsWith("sparc"))
        {
            return;
        }
        final File file = new File(workingDirectory, "confusedCompound.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        HDF5CompoundType<RecordA> compoundTypeInt = RecordA.getHDF5Type(writer);
        final RecordA recordWritten = new RecordA(17, 42.0f);
        writer.writeCompound("/testCompound", compoundTypeInt, recordWritten);
        writer.close();
        final HDF5Reader reader = new HDF5ReaderConfig(file).reader();
        HDF5CompoundType<RecordB> compoundTypeFloat = RecordB.getHDF5Type(reader);
        final RecordB recordRead = reader.readCompound("/testCompound", compoundTypeFloat);
        assertTrue("written: " + recordWritten.a + ", read: " + recordRead.a,
                recordWritten.a == recordRead.a);
        assertTrue("written: " + recordWritten.b + ", read: " + recordRead.b,
                recordWritten.b == recordRead.b);
        reader.close();
    }

    @Test
    public void testNumericConversion()
    {
        final File file = new File(workingDirectory, "numericConversions.h5");
        file.delete();
        assertFalse(file.exists());
        file.deleteOnExit();
        final HDF5Writer writer = new HDF5WriterConfig(file).writer();
        writer.writeFloat("pi", 3.14159f);
        writer.writeFloat("INFINITY", Float.POSITIVE_INFINITY);
        writer.writeDouble("DINFINITY", Double.NEGATIVE_INFINITY);
        writer.writeDouble("verySmallFloat", 1e-100);
        writer.writeDouble("veryLargeFloat", 1e+100);
        writer.addDoubleAttribute("pi", "eps", 1e-5);
        writer.writeLong("smallInteger", 17L);
        writer.writeLong("largeInteger", Long.MAX_VALUE);
        writer.close();
        final HDF5ReaderConfig config = new HDF5ReaderConfig(file).performNumericConversions();
        // If this platform doesn't support numeric conversions, the test would fail.
        if (config.platformSupportsNumericConversions() == false)
        {
            return;
        }
        final HDF5Reader reader = config.reader();
        assertEquals(3.14159, reader.readDouble("pi"), 1e-5);
        assertEquals(3, reader.readInt("pi"));
        assertEquals(1e-5f, reader.getFloatAttribute("pi", "eps"), 1e-9);
        assertEquals(17, reader.readByte("smallInteger"));
        assertEquals(0.0f, reader.readFloat("verySmallFloat"));
        assertEquals(Double.POSITIVE_INFINITY, reader.readDouble("INFINITY"));
        try
        {
            reader.readInt("largeInteger");
            fail("Failed to detect overflow");
        } catch (HDF5DatatypeInterfaceException ex)
        {
            assertEquals(HDF5Constants.H5E_CANTCONVERT, ex.getMinorErrorNumber());
        }
        try
        {
            reader.readFloat("veryLargeFloat");
            fail("Failed to detect overflow");
        } catch (HDF5DatatypeInterfaceException ex)
        {
            assertEquals(HDF5Constants.H5E_CANTCONVERT, ex.getMinorErrorNumber());
        }
        try
        {
            reader.readLong("veryLargeFloat");
            fail("Failed to detect overflow");
        } catch (HDF5DatatypeInterfaceException ex)
        {
            assertEquals(HDF5Constants.H5E_CANTCONVERT, ex.getMinorErrorNumber());
        }
        try
        {
            reader.readFloat("DINFINITY");
            fail("Failed to detect overflow");
        } catch (HDF5DatatypeInterfaceException ex)
        {
            assertEquals(HDF5Constants.H5E_CANTCONVERT, ex.getMinorErrorNumber());
        }
        try
        {
            reader.readLong("INFINITY");
            fail("Failed to detect overflow");
        } catch (HDF5DatatypeInterfaceException ex)
        {
            assertEquals(HDF5Constants.H5E_CANTCONVERT, ex.getMinorErrorNumber());
        }
        reader.close();

    }

}