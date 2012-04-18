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

    /**
     * Resolves the symbolic link of <var>entry</var>, if any.
     * 
     * @param entry The archive entry to resolve.
     * @return The resolved link, if <var>entry</var> is a symbolic link that links to an existing
     *         file or directory target, <code>null</code> if <var>entry</var> is a symbolic link
     *         that links to a non-existing target, or <var>entry</var>, if this is not a link.
     */
    public ArchiveEntry tryResolveLink(ArchiveEntry entry);

    /**
     * Returns the archive entry for <var>path</var>. If <var>path</var> is a symbolic link, the
     * entry will be resolved to the real file or directory in the archive, or <code>null</code>, if
     * the link target doesn't exist.
     * 
     * @param path The path in the archive to get the entry for.
     * @param keepPath If <code>true</code>, the resolved entry will keep the <var>path</var>, i.e.
     *            the returned entry of a symlink will look like a hard link. If <code>false</code>,
     *            the returned entry will be the entry of the resolved path.
     * @return The resolved link, if <var>path</var> denotes a file, directory, or symbolic link
     *         that links to an existing file or directory target, <code>null</code> if
     *         <var>path</var> denotes a symbolic link that links to a non-existing target.
     */
    public ArchiveEntry tryGetResolvedEntry(String path, boolean keepPath);

    /**
     * Returns the list of all entries in the archive with links resolved.
     */
    public List<ArchiveEntry> list();

    public List<ArchiveEntry> list(String fileOrDir);

    public List<ArchiveEntry> list(String fileOrDir, final ListParameters params);

    public IHDF5ArchiveInfoProvider list(String fileOrDir, IListEntryVisitor visitor);

    public IHDF5ArchiveInfoProvider list(String fileOrDir, final IListEntryVisitor visitor,
            ListParameters params);

}