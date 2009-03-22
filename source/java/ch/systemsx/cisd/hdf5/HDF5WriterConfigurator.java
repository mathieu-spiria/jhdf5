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
import ch.systemsx.cisd.common.utilities.OSUtilities;

/**
 * The configuration of the writer is done by chaining calls to configuration methods before calling
 * {@link #writer()}.
 * 
 * @author Bernd Rinn
 */
public class HDF5WriterConfigurator extends HDF5ReaderConfigurator
{

    /**
     * The mode of synchronizing changes (using a method like <code>fsync(2)</code>) to the HDF5
     * file with the underlying storage. As <code>fsync(2)</code> is blocking, the synchonization is
     * by default performed in a separate thread to minimize latency effects on the application. In
     * order to ensure that <code>fsync(2)</code> is called in the same thread, use one of the
     * <code>*_BLOCK</code> modes.
     * <p>
     * Note that non-blocking modes can have unexpected interactions with mandatory locks on
     * Windows. The symptom of that will be that the program holds a lock to the HDF5 file for some
     * (short) time even after the file has been closed. Thus, on Windows by default a blocking mode
     * is chosen.
     */
    public enum SyncMode
    {
        /**
         * Do not synchronize at all.
         */
        NO_SYNC,
        /**
         * Synchronize whenever {@link HDF5Writer#flush()} or {@link HDF5Writer#close()} are called.
         */
        SYNC,
        /**
         * Synchronize whenever {@link HDF5Writer#flush()} or {@link HDF5Writer#close()} are called.
         * Block until synchronize is finished.
         */
        SYNC_BLOCK,
        /**
         * Synchronize whenever {@link HDF5Writer#flush()} is called. <i>Default on Unix</i>
         */
        SYNC_ON_FLUSH,
        /**
         * Synchronize whenever {@link HDF5Writer#flush()} is called. Block until synchronize is
         * finished. <i>Default on Windows</i>.
         */
        SYNC_ON_FLUSH_BLOCK,
    }

    /**
     * Specify file format compatibility settings.
     */
    public enum FileFormat
    {
        /**
         * Enforce compatibility with HDF5 1.6 format.
         */
        STRICTLY_1_6,

        /**
         * Start with HDF5 1.6 format, but allow usage of features which require HDF5 1.8 library to
         * read. <i>Default</i>.
         */
        ALLOW_1_8,

        /**
         * Enforce compatibility with HDF5 1.8 format.
         */
        STRICTLY_1_8;
        
        /**
         * Returns <code>true</code> if using HDF5 1.8 features is OK.
         */
        boolean isHDF5_1_8_OK()
        {
            return ordinal() > STRICTLY_1_6.ordinal();
        }
        
    }

    private boolean useExtentableDataTypes = true;

    private boolean overwrite = false;

    private FileFormat fileFormat = FileFormat.ALLOW_1_8;

    // For Windows, use a blocking sync mode by default as otherwise the mandatory locks are up for
    // some surprises after the file has been closed.
    private SyncMode syncMode =
            OSUtilities.isWindows() ? SyncMode.SYNC_ON_FLUSH_BLOCK : SyncMode.SYNC_ON_FLUSH;

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
     * Sets the file format compatibility for the writer.
     */
    public HDF5WriterConfigurator fileFormat(FileFormat newFileFormat)
    {
        this.fileFormat = newFileFormat;
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
                            fileFormat, useExtentableDataTypes, overwrite, syncMode));
        }
        return (HDF5Writer) readerWriterOrNull;
    }

}
