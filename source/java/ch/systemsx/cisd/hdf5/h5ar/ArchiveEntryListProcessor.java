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

import ch.systemsx.cisd.base.unix.FileLinkType;
import ch.systemsx.cisd.hdf5.HDF5ObjectInformation;
import ch.systemsx.cisd.hdf5.IHDF5Reader;

/**
 * An {@Link IArchiveEntryProcessor} that performs a file list.
 *
 * @author Bernd Rinn
 */
class ArchiveEntryListProcessor implements IArchiveEntryProcessor
{
    private final IListEntryVisitor visitor;
    
    private final byte[] buffer;
    
    private final boolean checkArchive;
    
    ArchiveEntryListProcessor(IListEntryVisitor visitor, byte[] buffer, boolean checkArchive)
    {
        this.visitor = visitor;
        this.buffer = buffer;
        this.checkArchive = checkArchive;
    }

    public boolean process(String dir, String path, LinkRecord link, IHDF5Reader reader,
            IdCache idCache, IErrorStrategy errorStrategy) throws IOException
    {
        String errorMessage = null;
        if (checkArchive)
        {
            final HDF5ObjectInformation info = reader.getObjectInformation(path);
            final FileLinkType verifiedType = Utils.translateType(info.getType());
            link.setVerifiedType(verifiedType);
            if (verifiedType == FileLinkType.REGULAR_FILE)
            {
                final long verifiedSize = reader.getSize(path);
                link.setVerifiedSize(verifiedSize);
                try
                {
                    link.setVerifiedCrc32(calcCRC32Archive(path, verifiedSize, reader));
                } catch (HDF5Exception ex)
                {
                    errorMessage = ex.getClass().getSimpleName() + ": " + ex.getMessage();
                }
            }
        }
        visitor.visit(new ArchiveEntry(dir, path, link, idCache, errorMessage));
        return true;
    }

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
                    hdf5Reader.readAsByteArrayToBlockWithOffset(objectPath, buffer, buffer.length,
                            offset, 0);
            offset += n;
            crc32Digest.update(buffer, 0, n);
        }
        return (int) crc32Digest.getValue();
    }

    public ArchiverException createException(String objectPath, String detailedMsg)
    {
        return new ListArchiveException(objectPath, detailedMsg);
    }

    public ArchiverException createException(String objectPath, HDF5Exception cause)
    {
        return new ListArchiveException(objectPath, cause);
    }

    public ArchiverException createException(String objectPath, RuntimeException cause)
    {
        return new ListArchiveException(objectPath, cause);
    }

    public ArchiverException createException(File file, IOException cause)
    {
        return new ListArchiveException(file, cause);
    }

}
