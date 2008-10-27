/*
 * Copyright 2008 ETH Zuerich, CISD.
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

package ch.systemsx.cisd.hdf5;

import static ncsa.hdf.hdf5lib.HDF5Constants.H5L_TYPE_EXTERNAL;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5L_TYPE_SOFT;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5O_TYPE_DATASET;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5O_TYPE_GROUP;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5O_TYPE_NAMED_DATATYPE;
import static ncsa.hdf.hdf5lib.HDF5Constants.H5O_TYPE_NTYPES;
import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;

/**
 * Information about an HDF5 link.
 * 
 * @author Bernd Rinn
 */
public final class HDF5LinkInformation
{
    static final HDF5LinkInformation ROOT_LINK_INFO =
            new HDF5LinkInformation("/", HDF5ObjectType.GROUP, null);

    private final String path;

    private final HDF5ObjectType type;

    private final String symbolicLinkTargetOrNull;

    private HDF5LinkInformation(String path, HDF5ObjectType type, String symbolicLinkTargetOrNull)
    {
        assert path != null;

        this.path = path;
        this.type = type;
        this.symbolicLinkTargetOrNull = symbolicLinkTargetOrNull;
    }

    static HDF5LinkInformation create(String path, int typeId, String symbolicLinkTargetOrNull)
    {
        final HDF5ObjectType type = objectTypeIdToObjectType(typeId);
        return new HDF5LinkInformation(path, type, symbolicLinkTargetOrNull);
    }

    static HDF5ObjectType objectTypeIdToObjectType(final int objectTypeId)
    {
        if (-1 == objectTypeId)
        {
            return HDF5ObjectType.NONEXISTENT;
        } else if (H5O_TYPE_GROUP == objectTypeId)
        {
            return HDF5ObjectType.GROUP;
        } else if (H5O_TYPE_DATASET == objectTypeId)
        {
            return HDF5ObjectType.DATASET;
        } else if (H5O_TYPE_NAMED_DATATYPE == objectTypeId)
        {
            return HDF5ObjectType.DATATYPE;
        } else if (objectTypeId >= H5O_TYPE_NTYPES)
        {
            final int linkTypeId = objectTypeId - H5O_TYPE_NTYPES;
            if (linkTypeId == H5L_TYPE_SOFT)
            {
                return HDF5ObjectType.SOFT_LINK;
            } else if (linkTypeId == H5L_TYPE_EXTERNAL)
            {
                return HDF5ObjectType.EXTERNAL_LINK;
            }
        }
        return HDF5ObjectType.OTHER;
    }

    /**
     * @throws HDF5JavaException If the link does not exist.
     */
    public void checkExists() throws HDF5JavaException
    {
        if (exists() == false)
        {
            throw new HDF5JavaException("Link '" + getPath() + "' does not exist.");
        }
    }

    /**
     * Returns the path of this link in the HDF5 file.
     */
    public String getPath()
    {
        return path;
    }

    /**
     * Returns the type of this link.
     */
    public HDF5ObjectType getType()
    {
        return type;
    }

    /**
     * Returns the symbolic link target of this link, or <code>null</code>, if this link does not
     * exist or is not a symbolic link.
     * <p>
     * Note that external links have a special format: They start with a prefix "
     * <code>EXTERNAL::</code>", then comes the path of the external file (beware that this part
     * uses the native path separator, i.e. "\" on Windows). Finally, separated by "<code>::</code>
     * ", the path of the link in the external file is provided (this part always uses "/" as path
     * separator).
     */
    public String tryGetSymbolicLinkTarget()
    {
        return symbolicLinkTargetOrNull;
    }

    /**
     * Returns <code>true</code>, if the link exists.
     */
    public boolean exists()
    {
        return HDF5ObjectType.exists(type);
    }

    /**
     * Returns <code>true</code>, if the link is a group.
     */
    public boolean isGroup()
    {
        return HDF5ObjectType.isGroup(type);
    }

    /**
     * Returns <code>true</code>, if the link is a data set.
     */
    public boolean isDataSet()
    {
        return HDF5ObjectType.isDataSet(type);
    }

    /**
     * Returns <code>true</code>, if the link is a data type.
     */
    public boolean isDataType()
    {
        return HDF5ObjectType.isDataType(type);
    }

    /**
     * Returns <code>true</code>, if the link is a soft link.
     */
    public boolean isSoftLink()
    {
        return HDF5ObjectType.isSoftLink(type);
    }

    /**
     * Returns <code>true</code>, if the link is an external link.
     */
    public boolean isExternalLink()
    {
        return HDF5ObjectType.isExternalLink(type);
    }

    /**
     * Returns <code>true</code>, if the link is either a soft link or an external link.
     */
    public boolean isSymbolicLink()
    {
        return HDF5ObjectType.isSymbolicLink(type);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((path == null) ? 0 : path.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        final HDF5LinkInformation other = (HDF5LinkInformation) obj;
        if (path == null)
        {
            if (other.path != null)
            {
                return false;
            }
        } else if (path.equals(other.path) == false)
        {
            return false;
        }
        return true;
    }

}