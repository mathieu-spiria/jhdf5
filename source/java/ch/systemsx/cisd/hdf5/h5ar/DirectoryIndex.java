/*
 * Copyright 2009 ETH Zuerich, CISD
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

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.CRC32;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import ch.systemsx.cisd.base.exceptions.IOExceptionUnchecked;
import ch.systemsx.cisd.hdf5.CharacterEncoding;
import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5LinkInformation;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.StringUtils;

/**
 * Memory representation of the directory index stored in an HDF5 archive.
 * <p>
 * Can operate in read-only or read-write mode. The mode is automatically determined by the
 * <var>hdf5Reader</var> provided the constructor: If this is an instance of {@link IHDF5Writer},
 * the directory index will be read-write, otherwise read-only.
 * <p>
 * This class is thread-safe.
 * 
 * @author Bernd Rinn
 */
class DirectoryIndex implements Iterable<LinkRecord>, Closeable, Flushable
{
    private static final String CRC32_ATTRIBUTE_NAME = "CRC32";

    private final IHDF5Reader hdf5Reader;

    private final IHDF5Writer hdf5WriterOrNull;

    private final String groupPath;

    private final IErrorStrategy errorStrategy;

    /**
     * The list of all links in this directory.
     * <p>
     * The order is to have all directories (in alphabetical order) before all files (in
     * alphabetical order).
     */
    private LinkList links;

    private boolean readLinkTargets;

    private boolean dirty;

    /**
     * Converts an array of {@link File}s into a list of {@link LinkRecord}s. The list is optimized for
     * iterating through it and removing single entries during the iteration.
     * <p>
     * Note that the length of the list will always be the same as the length of <var>entries</var>.
     * If some <code>stat</code> call failed on an entry, this entry will be <code>null</code>, so
     * code using the returned list of this method needs to be prepared that this list may contain
     * <code>null</code> values!
     * 
     * @return A list of {@link LinkRecord}s in the same order as <var>entries</var>.
     */
    public static List<LinkRecord> convertFilesToLinks(File[] files, boolean storeOwnerAndPermissions,
            IErrorStrategy errorStrategy)
    {
        final List<LinkRecord> list = new LinkedList<LinkRecord>();
        for (File file : files)
        {
            list.add(LinkRecord.tryCreate(file, storeOwnerAndPermissions, errorStrategy));
        }
        return list;
    }

    private static HDF5CompoundType<LinkRecord> getHDF5LinkCompoundType(IHDF5Reader reader)
    {
        return reader.getInferredCompoundType(LinkRecord.class);
    }

    /**
     * Creates a new directory (group) index. Note that <var>hdf5Reader</var> needs to be an
     * instance of {@link IHDF5Writer} if you intend to write the index to the archive.
     */
    DirectoryIndex(IHDF5Reader hdf5Reader, String groupPath, IErrorStrategy errorStrategyOrNull,
            boolean readLinkTargets)
    {
        assert hdf5Reader != null;
        assert groupPath != null;

        this.hdf5Reader = hdf5Reader;
        this.hdf5WriterOrNull =
                (hdf5Reader instanceof IHDF5Writer) ? (IHDF5Writer) hdf5Reader : null;
        if (hdf5WriterOrNull != null)
        {
            hdf5WriterOrNull.addFlushable(this);
        }
        this.groupPath = (groupPath.length() == 0) ? "/" : groupPath;
        if (errorStrategyOrNull == null)
        {
            this.errorStrategy = IErrorStrategy.DEFAULT_ERROR_STRATEGY;
        } else
        {
            this.errorStrategy = errorStrategyOrNull;
        }
        readIndex(readLinkTargets);
    }

    /**
     * Amend the index with link targets. If the links targets have already been read, this method
     * is a noop.
     */
    public void amendLinkTargets()
    {
        if (readLinkTargets)
        {
            return;
        }
        flush();
        readIndex(true);
    }

    private String getIndexDataSetName()
    {
        return groupPath + "/__INDEX__";
    }

    private String getIndexNamesDataSetName()
    {
        return groupPath + "/__INDEXNAMES__";
    }

    /**
     * (Re-)Reads the directory index from the archive represented by <var>hdf5Reader</var>.
     */
    private void readIndex(boolean withLinkTargets)
    {
        boolean readingH5ArIndexWorked = false;
        try
        {
            if (hdf5Reader.exists(getIndexDataSetName())
                    && hdf5Reader.exists(getIndexNamesDataSetName()))
            {
                final HDF5CompoundType<LinkRecord> linkCompoundType = getHDF5LinkCompoundType(hdf5Reader);
                final CRC32 crc32Digester = new CRC32();
                final String indexDataSetName = getIndexDataSetName();
                final ArrayList<LinkRecord> work =
                        new ArrayList<LinkRecord>(Arrays.asList(hdf5Reader.readCompoundArray(
                                indexDataSetName, linkCompoundType,
                                new IHDF5Reader.IByteArrayInspector()
                                    {
                                        public void inspect(byte[] byteArray)
                                        {
                                            crc32Digester.update(byteArray);
                                        }
                                    })));
                int crc32 = (int) crc32Digester.getValue();
                int crc32Stored =
                        hdf5Reader.getIntAttribute(indexDataSetName, CRC32_ATTRIBUTE_NAME);
                if (crc32 != crc32Stored)
                {
                    throw new ListArchiveException(groupPath,
                            "CRC checksum mismatch on index (links). Expected: "
                                    + Utils.crc32ToString(crc32Stored) + ", found: "
                                    + Utils.crc32ToString(crc32));
                }
                final String indexNamesDataSetName = getIndexNamesDataSetName();
                final String concatenatedNames = hdf5Reader.readString(indexNamesDataSetName);
                crc32 = calcCrc32(concatenatedNames);
                crc32Stored =
                        hdf5Reader.getIntAttribute(indexNamesDataSetName, CRC32_ATTRIBUTE_NAME);
                if (crc32 != crc32Stored)
                {
                    throw new ListArchiveException(groupPath,
                            "CRC checksum mismatch on index (names). Expected: "
                                    + Utils.crc32ToString(crc32Stored) + ", found: "
                                    + Utils.crc32ToString(crc32));
                }
                initLinks(work, concatenatedNames, withLinkTargets);
                setLinks(new LinkList(work));
                readingH5ArIndexWorked = true;
            }
        } catch (RuntimeException ex)
        {
            errorStrategy.dealWithError(new ListArchiveException(groupPath, ex));
        }
        // Fallback: couldn't read the index, reconstructing it from the group information.
        if (readingH5ArIndexWorked == false)
        {
            if (hdf5Reader.isGroup(groupPath, false))
            {
                final List<HDF5LinkInformation> hdf5LinkInfos =
                        hdf5Reader.getGroupMemberInformation(groupPath, withLinkTargets);
                final ArrayList<LinkRecord> work = new ArrayList<LinkRecord>(hdf5LinkInfos.size());
                for (HDF5LinkInformation linfo : hdf5LinkInfos)
                {
                    final long size =
                            linfo.isDataSet() ? hdf5Reader.getDataSetInformation(linfo.getPath())
                                    .getSize() : Utils.UNKNOWN;
                    work.add(new LinkRecord(linfo, size));
                }
                setLinks(new LinkList(work, true));
            }
        }
        readLinkTargets = withLinkTargets;
        dirty = false;
    }

    private void initLinks(final List<LinkRecord> work, final String concatenatedNames,
            boolean withLinkTargets)
    {
        int namePos = 0;
        for (LinkRecord link : work)
        {
            namePos =
                    link.initAfterReading(concatenatedNames, namePos, hdf5Reader, groupPath,
                            withLinkTargets);
        }
    }

    /**
     * Returns the link with {@link LinkRecord#getLinkName()} equal to <var>name</var>, or
     * <code>null</code>, if there is no such link in the directory index.
     * <p>
     * Can work on the list or map data structure.
     */
    public LinkRecord tryGetLink(String name)
    {
        return tryGetLinks().tryGetLink(name);
    }

    /**
     * Returns <code>true</code>, if this class has link targets read.
     */
    public boolean hasLinkTargets()
    {
        return readLinkTargets;
    }

    //
    // Iterable
    //

    public Iterator<LinkRecord> iterator()
    {
        return tryGetLinks().iterator();
    }

    //
    // Writing methods
    //

    /**
     * Writes the directory index to the archive represented by <var>hdf5Writer</var>.
     * <p>
     * Works on the list data structure.
     */
    public void flush()
    {
        if (dirty == false)
        {
            return;
        }
        ensureWriteMode();
        synchronized (this)
        {
            try
            {
                final StringBuilder concatenatedNames = new StringBuilder();
                for (LinkRecord link : tryGetLinks())
                {
                    link.prepareForWriting(concatenatedNames);
                }
                final String indexNamesDataSetName = getIndexNamesDataSetName();
                final String concatenatedNamesStr = concatenatedNames.toString();
                hdf5WriterOrNull.writeStringVariableLength(indexNamesDataSetName,
                        concatenatedNamesStr);
                hdf5WriterOrNull.setIntAttribute(indexNamesDataSetName, CRC32_ATTRIBUTE_NAME,
                        calcCrc32(concatenatedNamesStr));
                final String indexDataSetName = getIndexDataSetName();
                final CRC32 crc32 = new CRC32();
                hdf5WriterOrNull.writeCompoundArray(indexDataSetName,
                        getHDF5LinkCompoundType(hdf5WriterOrNull),
                        tryGetLinks().toArray(), HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION,
                        new IHDF5Reader.IByteArrayInspector()
                            {
                                public void inspect(byte[] byteArray)
                                {
                                    crc32.update(byteArray);
                                }
                            });
                hdf5WriterOrNull.setIntAttribute(indexDataSetName, CRC32_ATTRIBUTE_NAME,
                        (int) crc32.getValue());
            } catch (HDF5Exception ex)
            {
                errorStrategy.dealWithError(new ListArchiveException(groupPath, ex));
            }
            dirty = false;
        }
    }

    /**
     * Add <var>entries</var> to the index. Any link that already exists in the index will be
     * replaced.
     */
    public void updateIndex(List<LinkRecord> entries)
    {
        ensureWriteMode();
        synchronized (this)
        {
            final LinkList linkListOrNull = tryGetLinks();
            setLinks((linkListOrNull == null) ? new LinkList(toArrayList(entries)) : linkListOrNull
                    .update(entries));
        }
    }

    private ArrayList<LinkRecord> toArrayList(List<LinkRecord> entries)
    {
        if (entries instanceof ArrayList<?>)
        {
            return (ArrayList<LinkRecord>) entries;
        } else
        {
            return new ArrayList<LinkRecord>(entries);
        }
    }

    /**
     * Removes <var>name</var> from the index, if it exists.
     * 
     * @return <code>true</code>, if <var>name</var> was removed.
     */
    public boolean remove(String name)
    {
        ensureWriteMode();
        synchronized (this)
        {
            final LinkList linkListOrNull = tryGetLinks();
            final LinkRecord linkOrNull =
                    (linkListOrNull == null) ? null : linkListOrNull.tryGetLink(name);
            if (linkOrNull != null)
            {
                setLinks(tryGetLinks().remove(Collections.singleton(linkOrNull)));
                return true;
            }
            return false;
        }
    }

    private LinkList tryGetLinks()
    {
        return links;
    }

    private void setLinks(LinkList newLinks)
    {
        links = newLinks;
        dirty = true;
    }

    private void ensureWriteMode()
    {
        if (hdf5WriterOrNull == null)
        {
            throw new IllegalStateException("Cannot write index in read-only mode.");
        }
    }

    private int calcCrc32(String names)
    {
        final CRC32 crc32 = new CRC32();
        crc32.update(StringUtils.toBytes0Term(names, names.length(), CharacterEncoding.UTF8));
        return (int) crc32.getValue();
    }

    //
    // Closeable
    //

    public void close() throws IOExceptionUnchecked
    {
        flush();
        if (hdf5WriterOrNull != null)
        {
            hdf5WriterOrNull.removeFlushable(this);
        }
    }

}
