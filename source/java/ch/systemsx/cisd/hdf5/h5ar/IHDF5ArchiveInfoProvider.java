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

import java.util.List;

/**
 * An info provider for HDF5 archives. 
 *
 * @author Bernd Rinn
 */
interface IHDF5ArchiveInfoProvider
{
    public boolean exists(String path);

    public boolean isDirectory(String path);

    public boolean isRegularFile(String path);

    public boolean isSymLink(String path);

    public ArchiveEntry tryGetEntry(String path, boolean readLinkTarget);

    public List<ArchiveEntry> list(String fileOrDir);

    public List<ArchiveEntry> list(String fileOrDir, final ListParameters params);

    public IHDF5ArchiveInfoProvider list(String fileOrDir, IListEntryVisitor visitor);

    public IHDF5ArchiveInfoProvider list(String fileOrDir, final IListEntryVisitor visitor,
            ListParameters params);

}