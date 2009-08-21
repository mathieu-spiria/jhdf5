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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import ch.systemsx.cisd.base.unix.FileLinkType;
import ch.systemsx.cisd.hdf5.HDF5CompoundMemberMapping;
import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5EnumerationType;
import ch.systemsx.cisd.hdf5.HDF5GenericCompression;
import ch.systemsx.cisd.hdf5.HDF5LinkInformation;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;

/**
 * Memory representation of the directory index stored in an HDF5 archive.
 * <p>
 * Can operate in read-only or read-write mode. The mode is automatically determined by the
 * <var>hdf5Reader</var> provided the constructor: If this is an instance of {@link IHDF5Writer},
 * the directory index will be read-write, otherwise read-only.
 * <p>
 * Note that some methods are working exclusively on an internal list data structure, while other
 * methods work on a map data structure. While this is transparent to the caller in terms of
 * functionality, it is not transparent in terms of performance. When calling a method that works on
 * the map after calling a method that works on a list, the other data structure needs to be created
 * which may be CPU intensive.
 * 
 * @author Bernd Rinn
 */
public class DirectoryIndex implements Iterable<Link>
{
    private final String CRC32_ATTRIBUTE_NAME = "CRC32";

    private final IHDF5Reader hdf5Reader;

    private final IHDF5Writer hdf5WriterOrNull;

    private final String groupPath;

    private final boolean continueOnError;

    private final boolean readLinkTargets;

    private Link[] linksOrNull;

    /**
     * The index that points to the first file in {@link #linksOrNull} (all smaller indices point to
     * directories).
     */
    private int firstFileIndex;

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
            boolean continueOnError)
    {
        final List<Link> list = new LinkedList<Link>();
        for (File entry : entries)
        {
            list.add(Link.tryCreate(entry, storeOwnerAndPermissions, continueOnError));
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
            { mapping("linkNameLength"),
                    mapping("hdf5EncodedLinkType", "linkType", linkEnumerationType),
                    mapping("size"), mapping("lastModified"), mapping("uid"), mapping("gid"),
                    mapping("permissions"), mapping("crc32", "checksum") };
    }

    /**
     * Creates a new directory (group) index. Note that <var>hdf5Reader</var> needs to be an
     * instance of {@link IHDF5Writer} if you intend to write the index to the archive.
     */
    public DirectoryIndex(IHDF5Reader hdf5Reader, String groupPath, boolean continueOnError,
            boolean readLinkTargets)
    {
        assert hdf5Reader != null;
        assert groupPath != null;
        assert hdf5Reader.isGroup(groupPath, false);

        this.hdf5Reader = hdf5Reader;
        this.hdf5WriterOrNull =
                (hdf5Reader instanceof IHDF5Writer) ? (IHDF5Writer) hdf5Reader : null;
        this.groupPath = groupPath;
        this.continueOnError = continueOnError;
        this.readLinkTargets = readLinkTargets;
    }

    private String getIndexDataSetName()
    {
        return groupPath + "/__INDEX__";
    }

    private String getIndexNamesDataSetName()
    {
        return groupPath + "/__INDEXNAMES__";
    }

    private Map<String, Link> indexMapOrNull = null;

    private boolean isMapAvailable()
    {
        return indexMapOrNull != null;
    }

    /**
     * Reads the directory index from the archive represented by <var>hdf5Reader</var> if that
     * hasn't yet happened.
     * 
     * @param buildMap If <code>true</code>, the map data structure is build, otherwise the list
     *            data structure.
     */
    private void ensureIndexIsRead(boolean buildMap)
    {
        if (this.linksOrNull != null)
        {
            if (buildMap && indexMapOrNull == null)
            {
                listToMap();
            } else if (buildMap == false && indexMapOrNull != null)
            {
                mapToList();
            }
            return;
        }
        try
        {
            this.linksOrNull = null;
            if (hdf5Reader.exists(getIndexDataSetName())
                    && hdf5Reader.exists(getIndexNamesDataSetName()))
            {
                final HDF5CompoundType<Link> linkCompoundType = getHDF5LinkCompoundType(hdf5Reader);
                final CRC32 crc32Digester = new CRC32();
                final String indexDataSetName = getIndexDataSetName();
                final Link[] linksProcessing =
                        hdf5Reader.readCompoundArray(indexDataSetName, linkCompoundType,
                                new IHDF5Reader.IByteArrayInspector()
                                    {
                                        public void inspect(byte[] byteArray)
                                        {
                                            crc32Digester.update(byteArray);
                                        }
                                    });
                int crc32 = (int) crc32Digester.getValue();
                int crc32Stored =
                        hdf5Reader.getIntAttribute(indexDataSetName, CRC32_ATTRIBUTE_NAME);
                if (crc32 != crc32Stored)
                {
                    throw new ListArchiveException(groupPath,
                            "CRC checksum mismatch on index (links). Expected: "
                                    + HDF5ArchiveTools.hashToString(crc32Stored) + ", found: "
                                    + HDF5ArchiveTools.hashToString(crc32));
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
                                    + HDF5ArchiveTools.hashToString(crc32Stored) + ", found: "
                                    + HDF5ArchiveTools.hashToString(crc32));
                }
                int namePos = 0;
                this.firstFileIndex = 0;
                for (Link link : linksProcessing)
                {
                    namePos =
                            link.initAfterReading(concatenatedNames, namePos, hdf5Reader,
                                    groupPath, readLinkTargets);
                    // Note: only works because link array is ordered by directory / non-directory
                    if (link.isDirectory())
                    {
                        ++firstFileIndex;
                    }
                }
                this.linksOrNull = linksProcessing;
            }
        } catch (RuntimeException ex)
        {
            HDF5ArchiveTools
                    .dealWithError(new ListArchiveException(groupPath, ex), continueOnError);
        }
        if (this.linksOrNull == null)
        {
            if (hdf5Reader.isGroup(groupPath, false))
            {
                final List<HDF5LinkInformation> hdf5LinkInfos =
                        hdf5Reader.getGroupMemberInformation(groupPath, readLinkTargets);
                linksOrNull = new Link[hdf5LinkInfos.size()];
                int idx = 0;
                for (HDF5LinkInformation linfo : hdf5LinkInfos)
                {
                    final long size =
                            linfo.isDataSet() ? hdf5Reader.getDataSetInformation(linfo.getPath())
                                    .getSize() : Link.UNKNOWN;
                    linksOrNull[idx++] = new Link(linfo, size);
                }
            }
        }
        if (buildMap)
        {
            listToMap();
        }
    }

    private void mapToList()
    {
        linksOrNull = indexMapOrNull.values().toArray(new Link[indexMapOrNull.size()]);
        Arrays.sort(linksOrNull);
        indexMapOrNull = null;
    }

    private void listToMap()
    {
        assert linksOrNull != null;

        indexMapOrNull = new HashMap<String, Link>(linksOrNull.length);
        for (Link link : linksOrNull)
        {
            indexMapOrNull.put(link.getLinkName(), link);
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
        if (isMapAvailable())
        {
            return indexMapOrNull.get(name);
        }
        ensureIndexIsRead(false);
        // Try directory
        int index = binarySearch(linksOrNull, name, 0, firstFileIndex);
        if (linksOrNull == null)
        {
            return null;
        }
        if (index >= 0)
        {
            return linksOrNull[index];
        }
        // Try file / symlink
        index = binarySearch(linksOrNull, name, firstFileIndex, linksOrNull.length);
        if (index >= 0)
        {
            return linksOrNull[index];
        } else
        {
            return null;
        }
    }

    private static int binarySearch(Link[] a, String key, int startIndex, int endIndex)
    {
        int low = startIndex;
        int high = endIndex - 1;

        while (low <= high)
        {
            int mid = (low + high) >> 1;
            final String midVal = a[mid].getLinkName();
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
     * Writes the directory index to the archive represented by <var>hdf5Writer</var>.
     * <p>
     * Works on the list data structure.
     * 
     * @throws IllegalStateException If this directory index is in read-only mode.
     */
    public void writeIndexToArchive() throws IllegalStateException
    {
        if (hdf5WriterOrNull == null)
        {
            throw new IllegalStateException("Cannot write index in read-only mode.");
        }
        ensureIndexIsRead(false);
        try
        {
            final HDF5EnumerationType linkTypeEnumeration =
                    getHDF5LinkTypeEnumeration(hdf5WriterOrNull);
            final StringBuilder concatenatedNames = new StringBuilder();
            for (Link link : linksOrNull)
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
            hdf5WriterOrNull.writeCompoundArray(indexDataSetName, getHDF5LinkCompoundType(
                    hdf5WriterOrNull, linkTypeEnumeration), linksOrNull,
                    HDF5GenericCompression.GENERIC_NO_COMPRESSION,
                    new IHDF5Reader.IByteArrayInspector()
                        {
                            public void inspect(byte[] byteArray)
                            {
                                crc32.update(byteArray);
                            }
                        });
            hdf5WriterOrNull.setIntAttribute(indexDataSetName, CRC32_ATTRIBUTE_NAME, (int) crc32
                    .getValue());
        } catch (HDF5Exception ex)
        {
            HDF5ArchiveTools
                    .dealWithError(new ListArchiveException(groupPath, ex), continueOnError);
        }
    }

    private int calcCrc32(String names)
    {
        final CRC32 crc32 = new CRC32();
        crc32.update(names.getBytes());
        return (int) crc32.getValue();
    }

    /**
     * Add <var>entries</var> to the index. Any link that already exists in the index will be
     * replaced.
     * <p>
     * Can work on the list or the map data structure.
     */
    public void addToIndex(List<Link> entries)
    {
        if (isMapAvailable())
        {
            for (Link link : entries)
            {
                indexMapOrNull.put(link.getLinkName(), link);
            }
        } else
        {
            if (linksOrNull == null)
            {
                ensureIndexIsRead(false);
            }
            if (linksOrNull == null || this.linksOrNull.length == 0)
            {
                this.linksOrNull = entries.toArray(new Link[entries.size()]);
                Arrays.sort(this.linksOrNull);
            } else
            {
                ensureIndexIsRead(true);
                for (Link link : entries)
                {
                    indexMapOrNull.put(link.getLinkName(), link);
                }
            }
        }
    }

    /**
     * Works on the list structure.
     */
    public Iterator<Link> iterator()
    {
        ensureIndexIsRead(false);
        return new Iterator<Link>()
            {
                private Link[] iteratedLinks = linksOrNull;

                private int idx = 0;

                public boolean hasNext()
                {
                    return iteratedLinks != null && idx < iteratedLinks.length;
                }

                public Link next()
                {
                    return iteratedLinks[idx++];
                }

                public void remove() throws UnsupportedOperationException
                {
                    throw new UnsupportedOperationException();
                }
            };
    }

    /**
     * Removes <var>name</var> from the index, if it existss.
     * <p>
     * Works on the map data structure.
     * 
     * @return <code>true</code>, if <var>name</var> was removed.
     */
    public boolean remove(String name)
    {
        ensureIndexIsRead(true);
        return indexMapOrNull.remove(name) != null;
    }

}
