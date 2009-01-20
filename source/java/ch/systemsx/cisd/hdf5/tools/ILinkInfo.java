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

/**
 * An interface to provide information about a link in a Unix file system or in an HDF5 container. 
 *
 * @author Bernd Rinn
 */
public interface ILinkInfo
{

    /** The name of the link. */
    public abstract String getLinkName();

    /** Is <code>true</code>, if the link represents a directory. */
    public abstract boolean isDirectory();

    /** Is <code>true</code>, if the link represents a symbolic link. */
    public abstract boolean isSymLink();

    /** Is <code>true</code>, if the link represents a regular file. */
    public abstract boolean isRegularFile();

    /** Returns the size of the file behind the link in bytes (0 for directories). */
    public abstract long getSize();

    /** Returns the time of last modifications in number of seconds since start of the Epoch. */
    public abstract long getLastModified();

    /** Returns the user id that the link belongs to. */
    public abstract int getUid();

    /** Returns the group id that the link belongs to. */
    public abstract int getGid();

    /** Returns the Unix permissions of the link. */
    public abstract short getPermissions();

}