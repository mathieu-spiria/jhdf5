/*
 * Copyright 2011 ETH Zuerich, CISD
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

import java.io.File;
import java.io.IOException;
import java.util.zip.CRC32;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import ch.systemsx.cisd.base.exceptions.IErrorStrategy;
import ch.systemsx.cisd.base.unix.FileLinkType;
import ch.systemsx.cisd.hdf5.HDF5LinkInformation;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.hdf5lib.HDF5Constants;

/**
 * An {@Link IArchiveEntryProcessor} that performs a file list.
 * 
 * @author Bernd Rinn
 */
class ArchiveEntryListProcessor implements IArchiveEntryProcessor
{
    private final IArchiveEntryVisitor visitor;

    private final byte[] buffer;

    private final boolean checkArchive;

    ArchiveEntryListProcessor(IArchiveEntryVisitor visitor, byte[] buffer, boolean checkArchive)
    {
        this.visitor = visitor;
        this.buffer = buffer;
        this.checkArchive = checkArchive;
    }

    @Override
    public boolean process(String dir, String path, LinkRecord link, IHDF5Reader reader,
            IdCache idCache, IErrorStrategy errorStrategy) throws IOException
    {
        String errorMessage = null;
        if (checkArchive)
        {
            final HDF5LinkInformation info = reader.getLinkInformation(path);
            final FileLinkType verifiedType = Utils.translateType(info.getType());
            link.setVerifiedType(verifiedType);
            if (verifiedType == FileLinkType.REGULAR_FILE)
            {
                final long verifiedSize = reader.getSize(path);
                int verifiedCrc32 = 0;
                try
                {
                    verifiedCrc32 = calcCRC32Archive(path, verifiedSize, reader);
                } catch (HDF5Exception ex)
                {
                    errorMessage = ex.getClass().getSimpleName() + ": " + ex.getMessage();
                }
                link.setFileVerification(verifiedSize, verifiedCrc32, Utils.UNKNOWN);
            }
        }
        visitor.visit(new ArchiveEntry(dir, path, link, idCache, errorMessage));
        return true;
    }

    @Override
    public void postProcessDirectory(String dir, String path, LinkRecord link, IHDF5Reader reader,
            IdCache idCache, IErrorStrategy errorStrategy) throws IOException, HDF5Exception
    {
    }

    private int calcCRC32Archive(String objectPath, long size, IHDF5Reader hdf5Reader)
    {
        final CRC32 crc32Digest = new CRC32();
        long offset = 0;
        while (offset < size)
        {
            final int n =
                    hdf5Reader.generic().readArrayToBlockWithOffset(objectPath, buffer, buffer.length,
                            offset, 0);
            offset += n;
            crc32Digest.update(buffer, 0, n);
        }
        return (int) crc32Digest.getValue();
    }

    @Override
    public ArchiverException createException(String objectPath, String detailedMsg)
    {
        return new ListArchiveException(objectPath, detailedMsg);
    }

    @Override
    public ArchiverException createException(String objectPath, HDF5Exception cause)
    {
        if (isTooManySymlinksError(cause))
        {
            return new ListArchiveTooManySymbolicLinksException(objectPath, cause);
        }
        return new ListArchiveException(objectPath, cause);
    }

    private boolean isTooManySymlinksError(HDF5Exception cause)
    {
        return cause instanceof HDF5LibraryException
                && ((HDF5LibraryException) cause).getMajorErrorNumber() == HDF5Constants.H5E_LINK
                && "Too many soft links in path".equals(((HDF5LibraryException) cause)
                        .getMinorError());
    }

    @Override
    public ArchiverException createException(String objectPath, RuntimeException cause)
    {
        return new ListArchiveException(objectPath, cause);
    }

    @Override
    public ArchiverException createException(File file, IOException cause)
    {
        return new ListArchiveException(file, cause);
    }

}
