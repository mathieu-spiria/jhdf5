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

import java.io.IOException;

import ch.systemsx.cisd.hdf5.IHDF5Reader;

/**
 * An {@Link IArchiveEntryProcessor} that performs a verify operation versus a directory on
 * the file system.
 * 
 * @author Bernd Rinn
 */
class ArchiveEntryVerifyProcessor implements IArchiveEntryProcessor
{
    private final IListEntryVisitor visitor;

    private final String rootDirectory;

    private final byte[] buffer;

    private final boolean checkAttributes;

    private final boolean numeric;

    ArchiveEntryVerifyProcessor(IListEntryVisitor visitor, String rootDirectory, byte[] buffer,
            boolean checkAttributes, boolean numeric)
    {
        this.visitor = visitor;
        this.rootDirectory = rootDirectory;
        this.buffer = buffer;
        this.checkAttributes = checkAttributes;
        this.numeric = numeric;
    }

    public boolean process(String dir, String path, LinkRecord link, IHDF5Reader reader,
            IdCache idCache) throws IOException
    {
        final String errorMessage =
                HDF5ArchiveLinkVersusFilesystemChecker.checkLink(link, path, rootDirectory,
                        checkAttributes, numeric, idCache, buffer);
        visitor.visit(new ArchiveEntry(dir, path, link, idCache, errorMessage));
        return true;
    }

}
