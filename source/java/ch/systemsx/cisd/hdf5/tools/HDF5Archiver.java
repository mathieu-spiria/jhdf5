/*
 * Copyright 2008 ETH Zuerich, CISD
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

package ch.systemsx.cisd.hdf5.tools;

import java.io.File;
import java.util.List;

import ch.systemsx.cisd.hdf5.HDF5FactoryProvider;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.SyncMode;

/**
 * An archiver based on HDF5 as archive format for directory with fast random access to particular
 * files.
 * 
 * @author Bernd Rinn
 */
public class HDF5Archiver
{

    private final IHDF5Writer hdf5WriterOrNull;

    private final IHDF5Reader hdf5Reader;

    private final ArchivingStrategy strategy;

    private final boolean continueOnError;

    public HDF5Archiver(File archiveFile, boolean readOnly, boolean noSync, FileFormat fileFormat,
            boolean continueOnError)
    {
        if (readOnly)
        {
            this.hdf5WriterOrNull = null;
            this.hdf5Reader = HDF5FactoryProvider.get().openForReading(archiveFile);
        } else
        {
            final IHDF5WriterConfigurator config = HDF5FactoryProvider.get().configure(archiveFile);
            config.fileFormat(fileFormat);
            if (noSync == false)
            {
                config.syncMode(SyncMode.SYNC);
            }
            this.hdf5WriterOrNull = config.writer();
            this.hdf5Reader = hdf5WriterOrNull;
        }
        this.continueOnError = continueOnError;
        this.strategy = new ArchivingStrategy();
    }

    public ArchivingStrategy getStrategy()
    {
        return strategy;
    }

    public HDF5Archiver archiveAll(File path, boolean verbose) throws IllegalStateException
    {
        final File absolutePath = path.getAbsoluteFile();
        return archive(absolutePath.getParentFile(), absolutePath, verbose);
    }

    public HDF5Archiver archive(File root, File path, boolean verbose) throws IllegalStateException
    {
        if (hdf5WriterOrNull == null)
        {
            throw new IllegalStateException("Cannot archive in read-only mode.");
        }
        HDF5ArchiveTools.archive(hdf5WriterOrNull, strategy, root.getAbsoluteFile(), path
                .getAbsoluteFile(), continueOnError, verbose);
        return this;
    }

    public HDF5Archiver extract(File root, String path, boolean verbose)
            throws IllegalStateException
    {
        HDF5ArchiveTools.extract(hdf5Reader, strategy, root, path, continueOnError, verbose);
        return this;
    }

    public HDF5Archiver delete(List<String> hdf5ObjectPaths, boolean verbose)
    {
        if (hdf5WriterOrNull == null)
        {
            // delete() must not be called if we only have a reader.
            throw new IllegalStateException("Cannot delete in read-only mode.");
        }
        HDF5ArchiveTools.delete(hdf5WriterOrNull, hdf5ObjectPaths, continueOnError, verbose);
        return this;
    }

    public void list(String fileOrDir, String rootOrNull, boolean recursive,
            boolean suppressDirectoryEntries, boolean verbose, boolean numeric,
            HDF5ArchiveTools.Check check, HDF5ArchiveTools.ListEntryVisitor visitor)
    {
        HDF5ArchiveTools.list(hdf5Reader, new HDF5ArchiveTools.ListParameters()
                .fileOrDirectoryInArchive(fileOrDir).directoryOnFileSystem(rootOrNull).strategy(
                        strategy).recursive(recursive)
                .suppressDirectoryEntries(suppressDirectoryEntries).numeric(numeric).verbose(verbose)
                .check(check), visitor, continueOnError);
    }

    public void close()
    {
        hdf5Reader.close();
    }

}
