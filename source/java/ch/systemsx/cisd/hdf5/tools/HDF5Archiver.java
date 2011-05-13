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
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import ch.systemsx.cisd.base.exceptions.IOExceptionUnchecked;
import ch.systemsx.cisd.hdf5.HDF5DataBlock;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;

/**
 * An archiver based on HDF5 as archive format for directory with fast random access to particular
 * files in the archive.
 * 
 * @author Bernd Rinn
 */
public class HDF5Archiver
{

    private final static int MB = 1024 * 1024;

    final static int BUFFER_SIZE = 10 * MB;

    private final IHDF5Reader hdf5Reader;

    private final IErrorStrategy errorStrategy;

    private final HDF5ArchiveLister lister;

    private final HDF5ArchiveExtractor extracter;

    private final HDF5ArchiveUpdater updaterOrNull;

    private final HDF5ArchiveDeleter deleterOrNull;

    public HDF5Archiver(File archiveFile, boolean readOnly, boolean noSync, FileFormat fileFormat,
            IErrorStrategy errorStrategyOrNull)
    {
        this(archiveFile, new ArchivingStrategy(), readOnly, noSync, fileFormat,
                errorStrategyOrNull);
    }

    public HDF5Archiver(File archiveFile, ArchivingStrategy strategy, boolean readOnly,
            boolean noSync, FileFormat fileFormat, IErrorStrategy errorStrategyOrNull)
    {
        final byte[] buffer = new byte[BUFFER_SIZE];
        final IHDF5Writer hdf5WriterOrNull =
                readOnly ? null : HDF5ArchiveUpdater.createHDF5Writer(archiveFile, fileFormat,
                        noSync);
        hdf5Reader =
                (hdf5WriterOrNull != null) ? hdf5WriterOrNull : HDF5ArchiveExtractor
                        .createHDF5Reader(archiveFile);
        this.lister = new HDF5ArchiveLister(hdf5Reader, strategy, errorStrategyOrNull, buffer);
        this.extracter =
                new HDF5ArchiveExtractor(hdf5Reader, strategy, errorStrategyOrNull, buffer);
        if (hdf5WriterOrNull == null)
        {
            this.updaterOrNull = null;
            this.deleterOrNull = null;
        } else
        {
            this.updaterOrNull =
                    new HDF5ArchiveUpdater(hdf5WriterOrNull, strategy, errorStrategyOrNull, buffer);
            this.deleterOrNull = new HDF5ArchiveDeleter(hdf5WriterOrNull, errorStrategyOrNull);
        }
        if (errorStrategyOrNull == null)
        {
            this.errorStrategy = IErrorStrategy.DEFAULT_ERROR_STRATEGY;
        } else
        {
            this.errorStrategy = errorStrategyOrNull;
        }
    }

    public void close()
    {
        extracter.close();
    }

    public void list(String fileOrDir, String rootOrNull, boolean recursive,
            boolean suppressDirectoryEntries, boolean verbose, boolean numeric, Check check,
            IListEntryVisitor visitor)
    {
        lister.list(fileOrDir, rootOrNull, recursive, suppressDirectoryEntries, verbose, numeric,
                check, visitor);
    }

    public HDF5Archiver cat(File root, String path) throws IOExceptionUnchecked
    {
        if (hdf5Reader.isDataSet(path) == false)
        {
            errorStrategy.dealWithError(new UnarchivingException(path, "not found in archive"));
            return this;
        }
        try
        {
            final OutputStream os = new FileOutputStream(FileDescriptor.out);
            for (HDF5DataBlock<byte[]> block : hdf5Reader.getAsByteArrayNaturalBlocks(path))
            {
                os.write(block.getData());
            }
        } catch (IOException ex)
        {
            errorStrategy.dealWithError(new UnarchivingException(new File("stdout"), ex));
        }
        return this;
    }

    public HDF5Archiver extract(File root, String path, IPathVisitor pathVisitorOrNull)
            throws IllegalStateException
    {
        extracter.extract(root, path, pathVisitorOrNull);
        return this;
    }

    public HDF5Archiver archiveAll(File path, IPathVisitor pathVisitorOrNull)
            throws IllegalStateException
    {
        checkReadWrite();
        updaterOrNull.archiveAll(path, pathVisitorOrNull);
        return this;
    }

    public HDF5Archiver archive(File root, File path, IPathVisitor pathVisitorOrNull)
            throws IllegalStateException
    {
        checkReadWrite();
        updaterOrNull.archive(root, path, pathVisitorOrNull);
        return this;
    }

    public HDF5Archiver delete(List<String> hdf5ObjectPaths, IPathVisitor pathVisitorOrNull)
    {
        checkReadWrite();
        deleterOrNull.delete(hdf5ObjectPaths, pathVisitorOrNull);
        return this;
    }

    private void checkReadWrite()
    {
        if (updaterOrNull == null)
        {
            throw new IllegalStateException("Cannot update archive in read-only mode.");
        }
    }
}
