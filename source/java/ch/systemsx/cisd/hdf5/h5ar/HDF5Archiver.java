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

package ch.systemsx.cisd.hdf5.h5ar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ch.systemsx.cisd.base.exceptions.IOExceptionUnchecked;
import ch.systemsx.cisd.hdf5.HDF5DataBlock;
import ch.systemsx.cisd.hdf5.HDF5FactoryProvider;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.SyncMode;
import ch.systemsx.cisd.hdf5.h5ar.NewArchiveEntry.NewDirectoryArchiveEntry;
import ch.systemsx.cisd.hdf5.h5ar.NewArchiveEntry.NewFileArchiveEntry;
import ch.systemsx.cisd.hdf5.h5ar.NewArchiveEntry.NewSymLinkArchiveEntry;

/**
 * An archiver based on HDF5 as archive format for directory with fast random access to particular
 * files in the archive.
 * 
 * @author Bernd Rinn
 */
public class HDF5Archiver implements Closeable, Flushable
{

    private final static int MB = 1024 * 1024;

    final static int BUFFER_SIZE = 10 * MB;

    /**
     * An error strategy that just re-throws the exception.
     */
    public static final IErrorStrategy RETHROWING_ERROR_STRATEGY = new IErrorStrategy()
        {
            public void dealWithError(ArchiverException ex) throws ArchiverException
            {
                throw ex;
            }

            public void warning(String message)
            {
                System.err.println(message);
            }
        };

    private final IHDF5Reader hdf5Reader;

    private final IHDF5Writer hdf5WriterOrNull;

    private final IErrorStrategy errorStrategy;

    private final DirectoryIndexProvider indexProvider;

    private final byte[] buffer;

    private final HDF5ArchiveUpdater updaterOrNull;

    private final HDF5ArchiveDeleter deleterOrNull;

    private final HDF5ArchiveTraverser processor;

    static IHDF5Reader createHDF5Reader(File archiveFile)
    {
        return HDF5FactoryProvider.get().configureForReading(archiveFile)
                .useUTF8CharacterEncoding().reader();
    }

    static IHDF5Writer createHDF5Writer(File archiveFile, FileFormat fileFormat, boolean noSync)
    {
        final IHDF5WriterConfigurator config = HDF5FactoryProvider.get().configure(archiveFile);
        config.fileFormat(fileFormat);
        config.useUTF8CharacterEncoding();
        if (noSync == false)
        {
            config.syncMode(SyncMode.SYNC);
        }
        return config.writer();
    }

    public HDF5Archiver(File archiveFile, boolean readOnly)
    {
        this(archiveFile, readOnly, false, FileFormat.STRICTLY_1_6, RETHROWING_ERROR_STRATEGY);
    }

    public HDF5Archiver(File archiveFile, boolean readOnly, boolean noSync, FileFormat fileFormat,
            IErrorStrategy errorStrategyOrNull)
    {
        this.buffer = new byte[BUFFER_SIZE];
        this.hdf5WriterOrNull = readOnly ? null : createHDF5Writer(archiveFile, fileFormat, noSync);
        this.hdf5Reader =
                (hdf5WriterOrNull != null) ? hdf5WriterOrNull : createHDF5Reader(archiveFile);
        if (errorStrategyOrNull == null)
        {
            this.errorStrategy = IErrorStrategy.DEFAULT_ERROR_STRATEGY;
        } else
        {
            this.errorStrategy = errorStrategyOrNull;
        }
        this.indexProvider = new DirectoryIndexProvider(hdf5Reader, errorStrategy);
        this.processor = new HDF5ArchiveTraverser(hdf5Reader, indexProvider);
        if (hdf5WriterOrNull == null)
        {
            this.updaterOrNull = null;
            this.deleterOrNull = null;
        } else
        {
            this.updaterOrNull = new HDF5ArchiveUpdater(hdf5WriterOrNull, indexProvider, buffer);
            this.deleterOrNull = new HDF5ArchiveDeleter(hdf5WriterOrNull, indexProvider);
        }
    }

    public HDF5Archiver(IHDF5Reader reader, IErrorStrategy errorStrategyOrNull)
    {
        this.buffer = new byte[BUFFER_SIZE];
        this.hdf5WriterOrNull = (reader instanceof IHDF5Writer) ? (IHDF5Writer) reader : null;
        if (errorStrategyOrNull == null)
        {
            this.errorStrategy = IErrorStrategy.DEFAULT_ERROR_STRATEGY;
        } else
        {
            this.errorStrategy = errorStrategyOrNull;
        }
        this.hdf5Reader = reader;
        this.indexProvider = new DirectoryIndexProvider(hdf5Reader, errorStrategy);
        this.processor = new HDF5ArchiveTraverser(hdf5Reader, indexProvider);
        if (hdf5WriterOrNull == null)
        {
            this.updaterOrNull = null;
            this.deleterOrNull = null;
        } else
        {
            this.updaterOrNull = new HDF5ArchiveUpdater(hdf5WriterOrNull, indexProvider, buffer);
            this.deleterOrNull = new HDF5ArchiveDeleter(hdf5WriterOrNull, indexProvider);
        }
    }

    //
    // Closeable
    //

    public void close()
    {
        hdf5Reader.close();
    }

    //
    // Flusheable
    //

    public void flush() throws IOException
    {
        if (hdf5WriterOrNull != null)
        {
            hdf5WriterOrNull.flush();
        }
    }

    //
    // IHDF5Archiver
    //

    public List<ArchiveEntry> list(String fileOrDir)
    {
        return list(fileOrDir, ListParameters.DEFAULT);
    }

    public List<ArchiveEntry> list(String fileOrDir, ListParameters params)
    {
        final List<ArchiveEntry> result = new ArrayList<ArchiveEntry>(100);
        list(fileOrDir, new IListEntryVisitor()
            {
                public void visit(ArchiveEntry entry)
                {
                    result.add(entry);
                }
            }, params);
        return result;
    }

    public HDF5Archiver list(String fileOrDir, IListEntryVisitor visitor)
    {
        return list(fileOrDir, visitor, ListParameters.DEFAULT);
    }

    public HDF5Archiver list(String fileOrDir, IListEntryVisitor visitor, ListParameters params)
    {
        final ArchiveEntryListProcessor listProcessor =
                new ArchiveEntryListProcessor(visitor, buffer, params.isCheckArchive());
        processor.process(fileOrDir, params.isRecursive(), params.isReadLinkTargets(),
                listProcessor);
        return this;
    }

    public HDF5Archiver verifyAgainstFilesystem(String fileOrDir, String rootDirectory,
            IListEntryVisitor visitor)
    {
        return verifyAgainstFilesystem(fileOrDir, rootDirectory, visitor, VerifyParameters.DEFAULT);
    }

    public HDF5Archiver verifyAgainstFilesystem(String fileOrDir, String rootDirectory,
            IListEntryVisitor visitor, VerifyParameters params)
    {
        final ArchiveEntryVerifyProcessor verifyProcessor =
                new ArchiveEntryVerifyProcessor(visitor, rootDirectory, buffer,
                        params.isVerifyAttributes(), params.isNumeric());
        processor.process(fileOrDir, params.isRecursive(), params.isReadLinkTargets(),
                verifyProcessor);
        return this;
    }

    public HDF5Archiver extract(String path, OutputStream out) throws IOExceptionUnchecked
    {
        if (hdf5Reader.isDataSet(path) == false)
        {
            errorStrategy.dealWithError(new UnarchivingException(path, "not found in archive"));
            return this;
        }
        try
        {
            for (HDF5DataBlock<byte[]> block : hdf5Reader.getAsByteArrayNaturalBlocks(path))
            {
                out.write(block.getData());
            }
        } catch (IOException ex)
        {
            errorStrategy.dealWithError(new UnarchivingException(new File("stdout"), ex));
        }
        return this;
    }

    public byte[] extract(String path) throws IOExceptionUnchecked
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        extract(path, out);
        return out.toByteArray();
    }

    public HDF5Archiver extractToFilesystem(File root, String path)
            throws IllegalStateException
    {
        return extractToFilesystem(root, path, ArchivingStrategy.DEFAULT, null);
    }

    public HDF5Archiver extractToFilesystem(File root, String path, IListEntryVisitor visitorOrNull)
            throws IllegalStateException
    {
        return extractToFilesystem(root, path, ArchivingStrategy.DEFAULT, visitorOrNull);
    }

    public HDF5Archiver extractToFilesystem(File root, String path, ArchivingStrategy strategy,
            IListEntryVisitor visitorOrNull) throws IllegalStateException
    {
        final IArchiveEntryProcessor extractor =
                new ArchiveEntryExtractProcessor(visitorOrNull, strategy, root.getAbsolutePath(),
                        buffer);
        processor.process(path, true, true, extractor);
        return this;
    }

    public HDF5Archiver archiveFromFilesystem(File path, ArchivingStrategy strategy)
            throws IllegalStateException
    {
        return archiveFromFilesystem(path, strategy, (IPathVisitor) null);
    }

    public HDF5Archiver archiveFromFilesystem(File path, IPathVisitor pathVisitorOrNull)
            throws IllegalStateException
    {
        return archiveFromFilesystem(path, ArchivingStrategy.DEFAULT, pathVisitorOrNull);
    }

    public HDF5Archiver archiveFromFilesystem(File path, ArchivingStrategy strategy,
            IPathVisitor pathVisitorOrNull) throws IllegalStateException
    {
        checkReadWrite();
        updaterOrNull.archive(path, strategy, pathVisitorOrNull);
        return this;
    }

    public HDF5Archiver archiveFromFilesystem(File root, File path) throws IllegalStateException
    {
        return archiveFromFilesystem(root, path, ArchivingStrategy.DEFAULT);
    }

    public HDF5Archiver archiveFromFilesystem(File root, File path, ArchivingStrategy strategy)
            throws IllegalStateException
    {
        return archiveFromFilesystem(root, path, strategy, null);
    }

    public HDF5Archiver archiveFromFilesystem(File root, File path, ArchivingStrategy strategy,
            IPathVisitor pathVisitorOrNull) throws IllegalStateException
    {
        checkReadWrite();
        updaterOrNull.archive(root, path, strategy, pathVisitorOrNull);
        return this;
    }

    public HDF5Archiver archiveFile(String path, byte[] data) throws IllegalStateException
    {
        return archiveFile(NewArchiveEntry.file(path), new ByteArrayInputStream(data), null);
    }

    public HDF5Archiver archiveFile(String path, InputStream input) throws IllegalStateException
    {
        return archiveFile(NewArchiveEntry.file(path), input, null);
    }

    public HDF5Archiver archiveFile(NewFileArchiveEntry entry, InputStream input)
            throws IllegalStateException, IllegalArgumentException
    {
        return archiveFile(entry, input, null);
    }

    public HDF5Archiver archiveFile(NewFileArchiveEntry entry, byte[] data)
            throws IllegalStateException, IllegalArgumentException
    {
        return archiveFile(entry, new ByteArrayInputStream(data), null);
    }

    public HDF5Archiver archiveFile(NewFileArchiveEntry entry, InputStream input,
            IPathVisitor pathVisitorOrNull) throws IllegalStateException, IllegalArgumentException
    {
        checkReadWrite();
        final LinkRecord link = new LinkRecord(entry);
        updaterOrNull.archive(entry.getParentPath(), link, input, entry.isCompress(),
                pathVisitorOrNull);
        entry.setCrc32(link.getCrc32());
        return this;
    }

    public HDF5Archiver archiveSymlink(NewSymLinkArchiveEntry entry) throws IllegalStateException,
            IllegalArgumentException
    {
        return archiveSymlink(entry, null);
    }

    public HDF5Archiver archiveSymlink(String path, String linkTarget)
            throws IllegalStateException, IllegalArgumentException
    {
        return archiveSymlink(NewArchiveEntry.symlink(path, linkTarget), null);
    }

    public HDF5Archiver archiveSymlink(NewSymLinkArchiveEntry entry, IPathVisitor pathVisitorOrNull)
            throws IllegalStateException, IllegalArgumentException
    {
        checkReadWrite();
        final LinkRecord link = new LinkRecord(entry);
        updaterOrNull.archive(entry.getParentPath(), link, null, false, pathVisitorOrNull);
        return this;
    }

    public HDF5Archiver archiveDirectory(String path) throws IllegalStateException,
            IllegalArgumentException
    {
        return archiveDirectory(NewArchiveEntry.directory(path), null);
    }

    public HDF5Archiver archiveDirectory(NewDirectoryArchiveEntry entry)
            throws IllegalStateException, IllegalArgumentException
    {
        return archiveDirectory(entry, null);
    }

    public HDF5Archiver archiveDirectory(NewDirectoryArchiveEntry entry,
            IPathVisitor pathVisitorOrNull) throws IllegalStateException, IllegalArgumentException
    {
        checkReadWrite();
        final LinkRecord link = new LinkRecord(entry);
        updaterOrNull.archive(entry.getParentPath(), link, null, false, pathVisitorOrNull);
        return this;
    }

    public HDF5Archiver delete(String hdf5ObjectPath)
    {
        return delete(Collections.singletonList(hdf5ObjectPath), null);
    }

    public HDF5Archiver delete(List<String> hdf5ObjectPaths)
    {
        return delete(hdf5ObjectPaths, null);
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
