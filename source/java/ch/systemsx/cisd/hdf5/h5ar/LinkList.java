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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * An immutable list of {@link LinkRecord}s in a fixed order. The order is to have all directories
 * (in alphabetical order) before all files (in alphabetical order).
 * 
 * @author Bernd Rinn
 */
final class LinkList implements Iterable<LinkRecord>
{
    private final ArrayList<LinkRecord> internalList;

    /**
     * The index that points to the first file in {@link #links} (all smaller indices point to
     * directories).
     */
    private final int firstFileIndex;

    /**
     * Creates a new empty link list.
     */
    LinkList()
    {
        this(new ArrayList<LinkRecord>(100), false);
    }

    /**
     * Creates a new link list, using <var>entries</var> as internal list. The <var>entries</var>
     * will be sorted, by directories and files, and then alphabetically.
     * 
     * @param entries The internal list backing this link list. Note that this is the live object to
     *            avoid any copy operation!
     */
    LinkList(ArrayList<LinkRecord> entries)
    {
        this(entries, false);
    }

    /**
     * Creates a new link list, using <var>entries</var> as internal list.
     * 
     * @param entries The internal list backing this link list. Note that this is the live object to
     *            avoid any copy operation!
     * @param forceSort if <code>true</code>, force a sort operation on <var>entries</var>.
     */
    LinkList(ArrayList<LinkRecord> entries, boolean forceSort)
    {
        this.internalList = entries;
        sortIfNecessary(forceSort);
        this.firstFileIndex = findFirstFileIndex();
    }

    private void sortIfNecessary(boolean forceSort)
    {
        if (forceSort)
        {
            Collections.sort(internalList);
            return;
        }
        for (int i = 0; i < internalList.size() - 1; ++i)
        {
            if (internalList.get(i).compareTo(internalList.get(i + 1)) > 0)
            {
                Collections.sort(internalList);
                return;
            }
        }
    }

    private int findFirstFileIndex()
    {
        // Note: only works because link array is ordered by directory / non-directory
        // We do it linearly from the start because we assume that the number of directories will be
        // considerably smaller than the number of files.
        int firstFile = 0;
        for (LinkRecord link : internalList)
        {
            if (link.isDirectory())
            {
                ++firstFile;
            }
        }
        return firstFile;
    }

    /**
     * Returns an array of the links in this list.
     */
    public LinkRecord[] toArray()
    {
        return internalList.toArray(new LinkRecord[internalList.size()]);
    }

    /**
     * Returns the link with {@link LinkRecord#getLinkName()} equal to <var>name</var>, or
     * <code>null</code>, if there is no such link in the directory index.
     */
    public LinkRecord tryGetLink(String name)
    {
        int index = getDirectoryLinkIndex(name);
        if (index >= 0)
        {
            return internalList.get(index);
        } else
        {
            index = getFileLinkIndex(name);
            return (index >= 0) ? internalList.get(index) : null;
        }
    }

    public boolean exists(String name)
    {
        return (getDirectoryLinkIndex(name) >= 0) ? true : (getFileLinkIndex(name) >= 0);
    }

    /**
     * Returns the directory link with {@link LinkRecord#getLinkName()} equal to <var>name</var>, or
     * <code>null</code>, if there is no such link in the directory index or if it is not a
     * directory.
     */
    public LinkRecord tryGetDirectoryLink(String name)
    {
        int index = getDirectoryLinkIndex(name);
        return (index >= 0) ? internalList.get(index) : null;
    }

    public boolean isDirectory(String name)
    {
        return (getDirectoryLinkIndex(name) >= 0);
    }

    /**
     * Returns the file/symlink link with {@link LinkRecord#getLinkName()} equal to <var>name</var>,
     * or <code>null</code>, if there is no such link in the directory index or if it is not a file
     * or symlink.
     */
    public LinkRecord tryGetFileLink(String name)
    {
        int index = getFileLinkIndex(name);
        return (index >= 0) ? internalList.get(index) : null;
    }

    private int getLinkIndex(String name, int size)
    {
        // Try directory
        int index = binarySearch(name, 0, firstFileIndex);
        if (index >= 0)
        {
            return index;
        }
        // Try file / symlink
        index = binarySearch(name, firstFileIndex, size);
        if (index >= 0)
        {
            return index;
        } else
        {
            return -1;
        }
    }

    private int getDirectoryLinkIndex(String name)
    {
        // Try directory
        final int index = binarySearch(name, 0, firstFileIndex);
        if (index >= 0)
        {
            return index;
        } else
        {
            return -1;
        }
    }

    private int getFileLinkIndex(String name)
    {
        // Try file / symlink
        final int index = binarySearch(name, firstFileIndex, internalList.size());
        if (index >= 0)
        {
            return index;
        } else
        {
            return -1;
        }
    }

    private int binarySearch(String key, int startIndex, int endIndex)
    {
        int low = startIndex;
        int high = endIndex - 1;

        while (low <= high)
        {
            int mid = (low + high) >> 1;
            final String midVal = internalList.get(mid).getLinkName();
            int cmp = midVal.compareTo(key);

            if (cmp < 0)
            {
                low = mid + 1;
            } else if (cmp > 0)
            {
                high = mid - 1;
            } else
            {
                return mid; // key found
            }
        }
        return -(low + 1); // key not found.
    }

    /**
     * Returns <code>true</code> if this list is empty.
     */
    public boolean isEmpty()
    {
        return internalList.isEmpty();
    }

    //
    // Iterable<Link>
    //

    /**
     * Returns an iterator over all links in the list.
     */
    public Iterator<LinkRecord> iterator()
    {
        return internalList.iterator();
    }

    /**
     * Updates the <var>entries</var>. Each entry in <var>entries</var> is checked on whether it
     * already exists in the list, and, if yes, updated. So, given the current links are unique on
     * link names, the new <code>LinkList</code> will be unique as well.
     */
    public void update(Collection<LinkRecord> entries)
    {
        final int oldSize = internalList.size();
        for (LinkRecord entry : entries)
        {
            int index = getLinkIndex(entry.getLinkName(), oldSize);
            if (index < 0)
            {
                internalList.add(entry);
            } else
            {
                internalList.set(index, entry);
            }
        }
        if (internalList.size() != oldSize)
        {
            sortIfNecessary(true);
        }
    }

    /**
     * Removes the <var>entries</var> from the list.
     */
    public void remove(Collection<LinkRecord> entries)
    {
        internalList.removeAll(entries);
    }

}
