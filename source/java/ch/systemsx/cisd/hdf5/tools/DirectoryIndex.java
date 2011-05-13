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

package ch.systemsx.cisd.hdf5.tools;

import static ch.systemsx.cisd.hdf5.HDF5CompoundMemberMapping.mapping;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import ch.systemsx.cisd.base.unix.FileLinkType;
import ch.systemsx.cisd.hdf5.CharacterEncoding;
import ch.systemsx.cisd.hdf5.HDF5CompoundMemberMapping;
import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5EnumerationType;
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
public class DirectoryIndex implements Iterable<Link>
{
    private final String CRC32_ATTRIBUTE_NAME = "CRC32";

    private final IHDF5Reader hdf5Reader;

    private final IHDF5Writer hdf5WriterOrNull;

    private final String groupPath;

    private final IErrorStrategy errorStrategy;

    private final boolean readLinkTargets;

    /**
     * Atomic reference to a list of all links in this directory.
     * <p>
     * The order is to have all directories (in alphabetical order) before all files (in
     * alphabetical order).
     */
    private final AtomicReference<LinkList> links = new AtomicReference<LinkList>();

    /**
     * Converts an array of {@link File}s into a list of {@link Link}s. The list is optimized for
     * iterating through it and removing single entries during the iteration.
     * <p>
     * Note that the length of the list will always be the same as the length of <var>entries</var>.
     * If some <code>stat</code> call failed on an entry, this entry will be <code>null</code>, so
     * code using the returned list of this method needs to be prepared that this list may contain
     * <code>null</code> values!
     * 
     * @return A list of {@link Link}s in the same order as <var>entries</var>.
     */
    public static List<Link> convertFilesToLinks(File[] entries, boolean storeOwnerAndPermissions,
            IErrorStrategy errorStrategy)
    {
        final List<Link> list = new LinkedList<Link>();
        for (File entry : entries)
        {
            list.add(Link.tryCreate(entry, storeOwnerAndPermissions, errorStrategy));
        }
        return list;
    }

    private static HDF5EnumerationType getHDF5LinkTypeEnumeration(IHDF5Reader reader)
    {
        return reader.getEnumType("linkType", getFileLinkTypeValues());
    }

    private static HDF5CompoundType<Link> getHDF5LinkCompoundType(IHDF5Reader reader)
    {
        return getHDF5LinkCompoundType(reader, getHDF5LinkTypeEnumeration(reader));
    }

    private static HDF5CompoundType<Link> getHDF5LinkCompoundType(IHDF5Reader reader,
            HDF5EnumerationType hdf5LinkTypeEnumeration)
    {
        return reader.getCompoundType(null, Link.class, getMapping(hdf5LinkTypeEnumeration));
    }

    private static String[] getFileLinkTypeValues()
    {
        final FileLinkType[] fileLinkTypes = FileLinkType.values();
        final String[] values = new String[fileLinkTypes.length];
        for (int i = 0; i < values.length; ++i)
        {
            values[i] = fileLinkTypes[i].name();
        }
        return values;
    }

    private static HDF5CompoundMemberMapping[] getMapping(HDF5EnumerationType linkEnumerationType)
    {
        return new HDF5CompoundMemberMapping[]
            {
                    mapping("linkNameLength"),
                    mapping("linkType").fieldName("hdf5EncodedLinkType").enumType(
                            linkEnumerationType), mapping("size"), mapping("lastModified"),
                    mapping("uid"), mapping("gid"), mapping("permissions"),
                    mapping("checksum").fieldName("crc32") };
    }

    /**
     * Creates a new directory (group) index. Note that <var>hdf5Reader</var> needs to be an
     * instance of {@link IHDF5Writer} if you intend to write the index to the archive.
     */
    public DirectoryIndex(IHDF5Reader hdf5Reader, String groupPath,
            IErrorStrategy errorStrategyOrNull, boolean readLinkTargets)
    {
        assert hdf5Reader != null;
        assert groupPath != null;

        this.hdf5Reader = hdf5Reader;
        this.hdf5WriterOrNull =
                (hdf5Reader instanceof IHDF5Writer) ? (IHDF5Writer) hdf5Reader : null;
        this.groupPath = (groupPath.length() == 0) ? "/" : groupPath;
        this.readLinkTargets = readLinkTargets;
        if (errorStrategyOrNull == null)
        {
            this.errorStrategy = IErrorStrategy.DEFAULT_ERROR_STRATEGY;
        } else
        {
            this.errorStrategy = errorStrategyOrNull;
        }
        readIndex();
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
    private void readIndex()
    {
        boolean listRead = false;
        try
        {
            if (hdf5Reader.exists(getIndexDataSetName())
                    && hdf5Reader.exists(getIndexNamesDataSetName()))
            {
                final HDF5CompoundType<Link> linkCompoundType = getHDF5LinkCompoundType(hdf5Reader);
                final CRC32 crc32Digester = new CRC32();
                final String indexDataSetName = getIndexDataSetName();
                final ArrayList<Link> work =
                        new ArrayList<Link>(Arrays.asList(hdf5Reader.readCompoundArray(
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
                                    + ListEntry.hashToString(crc32Stored) + ", found: "
                                    + ListEntry.hashToString(crc32));
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
                                    + ListEntry.hashToString(crc32Stored) + ", found: "
                                    + ListEntry.hashToString(crc32));
                }
                initLinks(work, concatenatedNames);
                setLinks(new LinkList(work));
                listRead = true;
            }
        } catch (RuntimeException ex)
        {
            errorStrategy.dealWithError(new ListArchiveException(groupPath, ex));
        }
        if (listRead == false)
        {
            if (hdf5Reader.isGroup(groupPath, false))
            {
                final List<HDF5LinkInformation> hdf5LinkInfos =
                        hdf5Reader.getGroupMemberInformation(groupPath, readLinkTargets);
                final ArrayList<Link> work = new ArrayList<Link>(hdf5LinkInfos.size());
                for (HDF5LinkInformation linfo : hdf5LinkInfos)
                {
                    final long size =
                            linfo.isDataSet() ? hdf5Reader.getDataSetInformation(linfo.getPath())
                                    .getSize() : Link.UNKNOWN;
                    work.add(new Link(linfo, size));
                }
                setLinks(new LinkList(work, true));
            }
        }
    }

    private void initLinks(final List<Link> work, final String concatenatedNames)
    {
        int namePos = 0;
        for (Link link : work)
        {
            namePos =
                    link.initAfterReading(concatenatedNames, namePos, hdf5Reader, groupPath,
                            readLinkTargets);
        }
    }

    /**
     * Returns the link with {@link Link#getLinkName()} equal to <var>name</var>, or
     * <code>null</code>, if there is no such link in the directory index.
     * <p>
     * Can work on the list or map data structure.
     */
    public Link tryGetLink(String name)
    {
        return tryGetLinks().tryGetLink(name);
    }

    //
    // Iterable
    //

    public Iterator<Link> iterator()
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
     * 
     * @throws IllegalStateException If this directory index is in read-only mode.
     */
    public void writeIndexToArchive() throws IllegalStateException
    {
        ensureWriteMode();
        try
        {
            final HDF5EnumerationType linkTypeEnumeration =
                    getHDF5LinkTypeEnumeration(hdf5WriterOrNull);
            final StringBuilder concatenatedNames = new StringBuilder();
            for (Link link : tryGetLinks())
            {
                link.prepareForWriting(linkTypeEnumeration, concatenatedNames);
            }
            final String indexNamesDataSetName = getIndexNamesDataSetName();
            final String concatenatedNamesStr = concatenatedNames.toString();
            hdf5WriterOrNull.writeStringVariableLength(indexNamesDataSetName, concatenatedNamesStr);
            hdf5WriterOrNull.setIntAttribute(indexNamesDataSetName, CRC32_ATTRIBUTE_NAME,
                    calcCrc32(concatenatedNamesStr));
            final String indexDataSetName = getIndexDataSetName();
            final CRC32 crc32 = new CRC32();
            hdf5WriterOrNull.writeCompoundArray(indexDataSetName,
                    getHDF5LinkCompoundType(hdf5WriterOrNull, linkTypeEnumeration), tryGetLinks()
                            .toArray(), HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION,
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
    }

    /**
     * Add <var>entries</var> to the index. Any link that already exists in the index will be
     * replaced.
     */
    public void updateIndex(List<Link> entries)
    {
        ensureWriteMode();
        final LinkList linkListOrNull = tryGetLinks();
        setLinks((linkListOrNull == null) ? new LinkList(toArrayList(entries)) : linkListOrNull
                .update(entries));
    }

    private ArrayList<Link> toArrayList(List<Link> entries)
    {
        if (entries instanceof ArrayList<?>)
        {
            return (ArrayList<Link>) entries;
        } else
        {
            return new ArrayList<Link>(entries);
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
        final LinkList linkListOrNull = tryGetLinks();
        final Link linkOrNull = (linkListOrNull == null) ? null : linkListOrNull.tryGetLink(name);
        if (linkOrNull != null)
        {
            setLinks(tryGetLinks().remove(Collections.singleton(linkOrNull)));
            return true;
        }
        return false;
    }

    private LinkList tryGetLinks()
    {
        return links.get();
    }

    private void setLinks(LinkList newLinks)
    {
        links.set(newLinks);
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

}
