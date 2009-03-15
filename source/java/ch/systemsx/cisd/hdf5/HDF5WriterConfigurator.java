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

import java.io.File;

/**
 * The configuration of this writer config is done by chaining calls to methods {@link #overwrite()}
 * , {@link #dontUseExtendableDataTypes()} and {@link #useLatestFileFormat()} before calling
 * {@link #writer()}.
 * 
 * @author Bernd Rinn
 */
public class HDF5WriterConfigurator extends HDF5ReaderConfigurator
{

    private boolean useExtentableDataTypes = true;

    private boolean overwrite = false;

    private boolean useLatestFileFormat = false;

    private SyncMode syncMode = SyncMode.SYNC_ON_FLUSH;

    /**
     * The mode of synchronizing changes (using a method like <code>fsync(2)</code>) to the HDF5
     * file with the underlying storage. As <code>fsync(2)</code> is blocking, the synchonization is
     * by default performed in a separate thread to minimize latency effects on the application. In
     * order to ensure that <code>fsync(2)</code> is called in the same thread, use one of the
     * <code>*_BLOCK</code> modes.
     */
    public enum SyncMode
    {
        /**
         * Do not synchronize at all.
         */
        NO_SYNC,
        /**
         * Synchronize whenver {@link HDF5Writer#flush()} or {@link HDF5Writer#close()} are called.
         */
        SYNC,
        /**
         * Synchronize whenever {@link HDF5Writer#flush()} or {@link HDF5Writer#close()} are called.
         * Block until synchronize is finished.
         */
        SYNC_BLOCK,
        /**
         * Synchronize whenever {@link HDF5Writer#flush()} is called. <i>Default</i>
         */
        SYNC_ON_FLUSH,
        /**
         * Synchronize whenever {@link HDF5Writer#flush()} is called. Block until synchronize is
         * finished.
         */
        SYNC_ON_FLUSH_BLOCK,
    }

    public HDF5WriterConfigurator(File hdf5File)
    {
        super(hdf5File);
    }

    /**
     * The file will be truncated to length 0 if it already exists, that is its content will be
     * deleted.
     */
    public HDF5WriterConfigurator overwrite()
    {
        this.overwrite = true;
        return this;
    }

    /**
     * Use data types which can not be extended later on. This may reduce the initial size of the
     * HDF5 file.
     */
    public HDF5WriterConfigurator dontUseExtendableDataTypes()
    {
        this.useExtentableDataTypes = false;
        return this;
    }

    /**
     * A file will be created that uses the latest available file format. This may improve
     * performance or space consumption but in general means that older versions of the library are
     * no longer able to read this file.
     */
    public HDF5WriterConfigurator useLatestFileFormat()
    {
        this.useLatestFileFormat = true;
        return this;
    }

    /**
     * Sets the {@link SyncMode}.
     */
    public HDF5WriterConfigurator syncMode(SyncMode newSyncMode)
    {
        this.syncMode = newSyncMode;
        return this;
    }

    /**
     * Will try to perform numeric conversions where appropriate if supported by the platform.
     * <p>
     * <strong>Numeric conversions can be platform dependent and are not available on all platforms.
     * Be advised not to rely on numeric conversions if you can help it!</strong>
     */
    @Override
    public HDF5WriterConfigurator performNumericConversions()
    {
        return (HDF5WriterConfigurator) super.performNumericConversions();
    }

    /**
     * Returns an {@link HDF5Writer} based on this configuration.
     */
    public HDF5Writer writer()
    {
        if (readerWriterOrNull == null)
        {
            readerWriterOrNull =
                    new HDF5Writer(new HDF5BaseWriter(hdf5File, performNumericConversions,
                            useLatestFileFormat, useExtentableDataTypes, overwrite, syncMode));
        }
        return (HDF5Writer) readerWriterOrNull;
    }

}
