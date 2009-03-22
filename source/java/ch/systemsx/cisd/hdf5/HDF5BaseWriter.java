/*
 * Copyright 2009 ETH Zuerich, CISD
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

import static ch.systemsx.cisd.hdf5.HDF5Utils.DATATYPE_GROUP;
import static ch.systemsx.cisd.hdf5.HDF5Utils.TYPE_VARIANT_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.VARIABLE_LENGTH_STRING_DATA_TYPE;
import static ch.systemsx.cisd.hdf5.HDF5Utils.isEmpty;
import static ncsa.hdf.hdf5lib.H5.H5Dwrite;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_SCALAR;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5S_UNLIMITED;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.EnumSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import ch.systemsx.cisd.common.concurrent.NamingThreadPoolExecutor;
import ch.systemsx.cisd.common.process.ICallableWithCleanUp;
import ch.systemsx.cisd.common.process.ICleanUpRegistry;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation.StorageLayout;
import ch.systemsx.cisd.hdf5.HDF5WriterConfigurator.FileFormat;
import ch.systemsx.cisd.hdf5.HDF5WriterConfigurator.SyncMode;

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * Class that provides base methods for reading and writing HDF5 files.
 * 
 * @author Bernd Rinn
 */
final class HDF5BaseWriter extends HDF5BaseReader
{

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 60;

    private static final int MAX_TYPE_VARIANT_TYPES = 1024;

    private final static EnumSet<SyncMode> BLOCKING_SYNC_MODES =
            EnumSet.of(SyncMode.SYNC_BLOCK, SyncMode.SYNC_ON_FLUSH_BLOCK);

    private final static EnumSet<SyncMode> NON_BLOCKING_SYNC_MODES =
            EnumSet.of(SyncMode.SYNC, SyncMode.SYNC_ON_FLUSH);

    private final static EnumSet<SyncMode> SYNC_ON_CLOSE_MODES =
            EnumSet.of(SyncMode.SYNC_BLOCK, SyncMode.SYNC);

    /**
     * The size threshold for the COMPACT storage layout.
     */
    final static int COMPACT_LAYOUT_THRESHOLD = 256;

    /**
     * ExecutorService for calling <code>fsync(2)</code> in a non-blocking way.
     */
    private final static ExecutorService syncExecutor =
            new NamingThreadPoolExecutor("HDF5 Sync").corePoolSize(3).daemonize();

    static
    {
        // Ensure all sync() calls are finished.
        Runtime.getRuntime().addShutdownHook(new Thread()
            {
                @Override
                public void run()
                {
                    syncExecutor.shutdownNow();
                    try
                    {
                        syncExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    } catch (InterruptedException ex)
                    {
                        // Unexpected
                        ex.printStackTrace();
                    }
                }
            });
    }

    private final RandomAccessFile fileForSyncing;

    private enum Command
    {
        SYNC, CLOSE_ON_EXIT, CLOSE_SYNC, EXIT
    }

    private final BlockingQueue<Command> commandQueue;

    final boolean useExtentableDataTypes;

    final boolean overwrite;

    final SyncMode syncMode;

    final FileFormat fileFormat;

    final int variableLengthStringDataTypeId;

    HDF5BaseWriter(File hdf5File, boolean performNumericConversions, FileFormat fileFormat,
            boolean useExtentableDataTypes, boolean overwrite, SyncMode syncMode)
    {
        super(hdf5File, performNumericConversions, fileFormat, overwrite);
        try
        {
            this.fileForSyncing = new RandomAccessFile(hdf5File, "rw");
        } catch (FileNotFoundException ex)
        {
            // Should not be happening as openFile() was called in super()
            throw new HDF5JavaException("Cannot open RandomAccessFile: " + ex.getMessage());
        }
        this.fileFormat = fileFormat;
        this.useExtentableDataTypes = useExtentableDataTypes;
        this.overwrite = overwrite;
        this.syncMode = syncMode;
        readNamedDataTypes();
        variableLengthStringDataTypeId = openOrCreateVLStringType();
        commandQueue = new LinkedBlockingQueue<Command>();
        setupSyncThread();
    }

    private void setupSyncThread()
    {
        syncExecutor.execute(new Runnable()
            {
                public void run()
                {
                    while (true)
                    {
                        try
                        {
                            switch (commandQueue.take())
                            {
                                case SYNC:
                                    syncNow();
                                    break;
                                case CLOSE_ON_EXIT:
                                    closeNow();
                                    return;
                                case CLOSE_SYNC:
                                    closeSync();
                                    return;
                                case EXIT:
                                    return;
                            }
                        } catch (InterruptedException ex)
                        {
                            // Shutdown has been triggered by showdownNow(), add
                            // <code>CLOSEHDF</code> to queue.
                            // (Note that a close() on a closed RandomAccessFile is harmless.)
                            commandQueue.add(Command.CLOSE_ON_EXIT);
                        }
                    }
                }
            });
    }

    @Override
    int openFile(FileFormat fileFormatInit, boolean overwriteInit)
    {
        final boolean enforce_1_8 = (fileFormat == FileFormat.STRICTLY_1_8);
        if (hdf5File.exists() && overwriteInit == false)
        {
            return h5.openFileReadWrite(hdf5File.getPath(), enforce_1_8,
                    fileRegistry);
        } else
        {
            final File directory = hdf5File.getParentFile();
            if (directory.exists() == false)
            {
                throw new HDF5JavaException("Directory '" + directory.getPath()
                        + "' does not exist.");
            }
            return h5.createFile(hdf5File.getPath(), enforce_1_8, fileRegistry);
        }
    }

    /**
     * Calls <code>fdatasync(2)</code> if available or else <code>fsync(2)</code> in the current
     * thread.
     */
    private void syncNow()
    {
        try
        {
            // Linux + Solaris: fdatasync(), MaxOSX: fsync(), Windows: FlushFileBuffers()
            fileForSyncing.getChannel().force(false);
        } catch (IOException ex)
        {
            throw new HDF5JavaException("Error syncing file: " + ex.getMessage());
        }
    }

    /**
     * Closes and, depending on the sync mode, syncs the HDF5 file in the current thread.
     * <p>
     * To be called from the syncer thread only.
     */
    synchronized private void closeNow()
    {
        if (state == State.OPEN)
        {
            super.close();
            if (SYNC_ON_CLOSE_MODES.contains(syncMode))
            {
                syncNow();
            }
            closeSync();
        }
    }

    private void closeSync()
    {
        try
        {
            fileForSyncing.close();
        } catch (IOException ex)
        {
            throw new HDF5JavaException("Error closing file: " + ex.getMessage());
        }
    }

    synchronized void flush()
    {
        h5.flushFile(fileId);
        if (NON_BLOCKING_SYNC_MODES.contains(syncMode))
        {
            commandQueue.add(Command.SYNC);
        } else if (BLOCKING_SYNC_MODES.contains(syncMode))
        {
            syncNow();
        }
    }

    synchronized void flushSyncBlocking()
    {
        h5.flushFile(fileId);
        syncNow();
    }

    @Override
    synchronized void close()
    {
        if (state == State.OPEN)
        {
            super.close();
            if (SyncMode.SYNC == syncMode)
            {
                commandQueue.add(Command.SYNC);
            } else if (SyncMode.SYNC_BLOCK == syncMode)
            {
                syncNow();
            }

            if (EnumSet.complementOf(NON_BLOCKING_SYNC_MODES).contains(syncMode))
            {
                closeSync();
                commandQueue.add(Command.EXIT);
            } else
            {
                // End syncer thread and avoid a race condition for non-blocking sync modes as the
                // syncer thread still may want to use the fileForSynching
                commandQueue.add(Command.CLOSE_SYNC);
            }
        }
    }

    @Override
    void commitDataType(final String dataTypePath, final int dataTypeId)
    {
        h5.commitDataType(fileId, dataTypePath, dataTypeId);
    }

    HDF5EnumerationType openOrCreateTypeVariantDataType(final HDF5Writer writer)
    {
        final HDF5EnumerationType dataType;
        int dataTypeId = getDataTypeId(HDF5Utils.TYPE_VARIANT_DATA_TYPE);
        if (dataTypeId < 0
                || h5.getNumberOfMembers(dataTypeId) < HDF5DataTypeVariant.values().length)
        {
            final String typeVariantPath = findFirstUnusedTypeVariantPath(writer);
            dataType = createTypeVariantDataType();
            commitDataType(typeVariantPath, dataType.getStorageTypeId());
            writer.createOrUpdateSoftLink(typeVariantPath.substring(DATATYPE_GROUP.length() + 1),
                    TYPE_VARIANT_DATA_TYPE);
        } else
        {
            final int nativeDataTypeId = h5.getNativeDataType(dataTypeId, fileRegistry);
            final String[] typeVariantNames = h5.getNamesForEnumOrCompoundMembers(dataTypeId);
            dataType =
                    new HDF5EnumerationType(fileId, dataTypeId, nativeDataTypeId,
                            TYPE_VARIANT_DATA_TYPE, typeVariantNames);

        }
        return dataType;
    }

    private String findFirstUnusedTypeVariantPath(final HDF5Reader reader)
    {
        int number = 0;
        String path;
        do
        {
            path = TYPE_VARIANT_DATA_TYPE + "." + (number++);
        } while (reader.exists(path) && number < MAX_TYPE_VARIANT_TYPES);
        return path;
    }

    private int openOrCreateVLStringType()
    {
        int dataTypeId = getDataTypeId(HDF5Utils.VARIABLE_LENGTH_STRING_DATA_TYPE);
        if (dataTypeId < 0)
        {
            dataTypeId = h5.createDataTypeVariableString(fileRegistry);
            commitDataType(VARIABLE_LENGTH_STRING_DATA_TYPE, dataTypeId);
        }
        return dataTypeId;
    }

    /**
     * Write a scalar value provided as <code>byte[]</code>.
     */
    void writeScalar(final String dataSetPath, final int storageDataTypeId,
            final int nativeDataTypeId, final byte[] value)
    {
        assert dataSetPath != null;
        assert storageDataTypeId >= 0;
        assert nativeDataTypeId >= 0;
        assert value != null;

        final ICallableWithCleanUp<Object> writeScalarRunnable = new ICallableWithCleanUp<Object>()
            {
                public Object call(ICleanUpRegistry registry)
                {
                    writeScalar(dataSetPath, storageDataTypeId, nativeDataTypeId, value, registry);
                    return null; // Nothing to return.
                }
            };
        runner.call(writeScalarRunnable);
    }

    /**
     * Internal method for writing a scalar value provided as <code>byte[]</code>.
     */
    int writeScalar(final String dataSetPath, final int storageDataTypeId,
            final int nativeDataTypeId, final byte[] value, ICleanUpRegistry registry)
    {
        final int dataSetId;
        if (h5.exists(fileId, dataSetPath))
        {
            dataSetId = h5.openObject(fileId, dataSetPath, registry);
        } else
        {
            dataSetId = h5.createScalarDataSet(fileId, storageDataTypeId, dataSetPath, registry);
        }
        H5Dwrite(dataSetId, nativeDataTypeId, H5S_SCALAR, H5S_SCALAR, H5P_DEFAULT, value);
        return dataSetId;
    }

    /**
     * Creates a data set.
     */
    /**
     * Creates a data set.
     */
    int createDataSet(final String objectPath, final int storageDataTypeId,
            final HDF5AbstractCompression compression, final long[] dimensions,
            final long[] chunkSizeOrNull, boolean enforceCompactLayout, ICleanUpRegistry registry)
    {
        final int dataSetId;
        final boolean empty = isEmpty(dimensions);
        final long[] definitiveChunkSizeOrNull;
        if (empty)
        {
            definitiveChunkSizeOrNull =
                    HDF5Utils.tryGetChunkSize(dimensions, compression.requiresChunking(), true);
        } else if (enforceCompactLayout)
        {
            definitiveChunkSizeOrNull = null;
        } else if (chunkSizeOrNull != null)
        {
            definitiveChunkSizeOrNull = chunkSizeOrNull;
        } else
        {
            definitiveChunkSizeOrNull =
                    HDF5Utils.tryGetChunkSize(dimensions, compression.requiresChunking(),
                            useExtentableDataTypes);
        }
        final StorageLayout layout =
                determineLayout(storageDataTypeId, dimensions, definitiveChunkSizeOrNull,
                        enforceCompactLayout);
        dataSetId =
                h5.createDataSet(fileId, dimensions, definitiveChunkSizeOrNull, storageDataTypeId,
                        compression, objectPath, layout, fileFormat, registry);
        return dataSetId;
    }

    /**
     * Determine which {@link StorageLayout} to use for the given <var>storageDataTypeId</var>.
     */
    StorageLayout determineLayout(final int storageDataTypeId, final long[] dimensions,
            final long[] chunkSizeOrNull, boolean enforceCompactLayout)
    {
        if (chunkSizeOrNull != null)
        {
            return StorageLayout.CHUNKED;
        }
        if (enforceCompactLayout
                || computeSizeForDimensions(storageDataTypeId, dimensions) < HDF5BaseWriter.COMPACT_LAYOUT_THRESHOLD)
        {
            return StorageLayout.COMPACT;
        }
        return StorageLayout.CONTIGUOUS;
    }

    private int computeSizeForDimensions(int dataTypeId, long[] dimensions)
    {
        int size = h5.getDataTypeSize(dataTypeId);
        for (long d : dimensions)
        {
            size *= d;
        }
        return size;
    }

    /**
     * Checks whether the given <var>dimensions</var> are in bounds for <var>dataSetId</var>.
     */
    boolean areDimensionsInBounds(final int dataSetId, final long[] dimensions)
    {
        final long[] maxDimensions = h5.getDataMaxDimensions(dataSetId);

        if (dimensions.length != maxDimensions.length) // Actually an error condition
        {
            return false;
        }

        for (int i = 0; i < dimensions.length; ++i)
        {
            if (maxDimensions[i] != H5S_UNLIMITED && dimensions[i] > maxDimensions[i])
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the data set id for the given <var>objectPath</var>.
     */
    int getDataSetId(final String objectPath, final int storageDataTypeId, final long[] dimensions,
            final HDF5AbstractCompression compression, ICleanUpRegistry registry)
    {
        final int dataSetId;
        if (h5.exists(fileId, objectPath))
        {
            dataSetId = h5.openDataSet(fileId, objectPath, registry);
            // Implementation note: HDF5 1.8 seems to be able to change the size even if
            // dimensions are not in bound of max dimensions, but the resulting file can
            // no longer be read by HDF5 1.6, thus we may only do it if config.useLatestFileFormat
            // == true.
            if (areDimensionsInBounds(dataSetId, dimensions) || fileFormat.isHDF5_1_8_OK())
            {
                h5.setDataSetExtent(dataSetId, dimensions);
                // FIXME 2008-09-15, Bernd Rinn: This is a work-around for an apparent bug in HDF5
                // 1.8.1 and 1.8.2 with contiguous data sets! Without the flush, the next
                // config.h5.writeDataSet() call will not overwrite the data.
                if (h5.getLayout(dataSetId, registry) == StorageLayout.CONTIGUOUS)
                {
                    h5.flushFile(fileId);
                }
            }
        } else
        {
            dataSetId =
                    createDataSet(objectPath, storageDataTypeId, compression, dimensions, null,
                            false, registry);
        }
        return dataSetId;
    }

}
