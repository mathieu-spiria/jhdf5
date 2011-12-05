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
 * A processor for an archive entry.
 * 
 * @author Bernd Rinn
 */
interface IArchiveEntryProcessor
{
    /**
     * Return <code>true</code> for continuing processing this <var>link</var>, <code>false</code>
     * to skip over this entry (only relevant for directory links).
     * 
     * @param dir The directory the current link is in.
     * @param path The path of the current link (including the link name)
     * @param link The link in the archive.
     * @param reader The HDF5 reader.
     * @param idCache The cached map of user and group ids to names.
     */
    public boolean process(String dir, String path, LinkRecord link, IHDF5Reader reader, IdCache idCache)
            throws IOException;
}
