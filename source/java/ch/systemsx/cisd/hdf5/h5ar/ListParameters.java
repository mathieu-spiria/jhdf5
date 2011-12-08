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

/**
 * A class that represents parameters for {@link HDF5Archiver#list(String, ListParameters)}.
 * 
 * @author Bernd Rinn
 */
public final class ListParameters
{
    private final boolean recursive;

    private final boolean readLinkTargets;

    private final boolean checkArchive;

    private final boolean suppressDirectoryEntries;

    public static final ListParameters DEFAULT = new ListParameters(true, true, false, false);

    public static final class ListParametersBuilder
    {
        private boolean recursive = true;

        private boolean readLinkTargets = true;

        private boolean checkArchive = false;

        private boolean suppressDirectoryEntries = false;

        private ListParametersBuilder()
        {
        }

        public ListParametersBuilder nonRecursive()
        {
            this.recursive = false;
            return this;
        }

        public ListParametersBuilder recursive(@SuppressWarnings("hiding")
        boolean recursive)
        {
            this.recursive = recursive;
            return this;
        }

        public ListParametersBuilder noReadLinkTarget()
        {
            this.readLinkTargets = false;
            return this;
        }

        public ListParametersBuilder readLinkTargets(@SuppressWarnings("hiding")
        boolean readLinkTargets)
        {
            this.readLinkTargets = readLinkTargets;
            return this;
        }

        public ListParametersBuilder checkArchive()
        {
            this.checkArchive = true;
            return this;
        }

        public ListParametersBuilder checkArchive(@SuppressWarnings("hiding")
        boolean checkArchive)
        {
            this.checkArchive = checkArchive;
            return this;
        }

        public ListParametersBuilder suppressDirectoryEntries()
        {
            this.suppressDirectoryEntries = true;
            return this;
        }

        public ListParametersBuilder suppressDirectoryEntries(@SuppressWarnings("hiding")
        boolean suppressDirectoryEntries)
        {
            this.suppressDirectoryEntries = suppressDirectoryEntries;
            return this;
        }

        public ListParameters get()
        {
            return new ListParameters(recursive, readLinkTargets, checkArchive,
                    suppressDirectoryEntries);
        }
    }

    public static ListParametersBuilder build()
    {
        return new ListParametersBuilder();
    }

    private ListParameters(boolean recursive, boolean readLinkTargets, boolean checkArchive,
            boolean suppressDirectoryEntries)
    {
        this.recursive = recursive;
        this.readLinkTargets = readLinkTargets;
        this.checkArchive = checkArchive;
        this.suppressDirectoryEntries = suppressDirectoryEntries;
    }

    public boolean isRecursive()
    {
        return recursive;
    }

    public boolean isReadLinkTargets()
    {
        return readLinkTargets;
    }

    public boolean isCheckArchive()
    {
        return checkArchive;
    }

    public boolean isSuppressDirectoryEntries()
    {
        return suppressDirectoryEntries;
    }
}
