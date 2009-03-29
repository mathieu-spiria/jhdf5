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

import ch.systemsx.cisd.base.utilities.OSUtilities;

/**
 * The configuration of the writer is done by chaining calls to configuration methods before calling
 * {@link #writer()}.
 * 
 * @author Bernd Rinn
 */
final class HDF5WriterConfigurator extends HDF5ReaderConfigurator implements IHDF5WriterConfigurator
{

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

    public HDF5WriterConfigurator overwrite()
    {
        this.overwrite = true;
        return this;
    }

    public HDF5WriterConfigurator dontUseExtendableDataTypes()
    {
        this.useExtentableDataTypes = false;
        return this;
    }

    public HDF5WriterConfigurator fileFormat(FileFormat newFileFormat)
    {
        this.fileFormat = newFileFormat;
        return this;
    }

    public HDF5WriterConfigurator syncMode(SyncMode newSyncMode)
    {
        this.syncMode = newSyncMode;
        return this;
    }

    @Override
    public HDF5WriterConfigurator performNumericConversions()
    {
        return (HDF5WriterConfigurator) super.performNumericConversions();
    }

    public IHDF5Writer writer()
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
