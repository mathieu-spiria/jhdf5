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

package ch.systemsx.cisd.hdf5.tools;

/**
 * An enumeration for the checks to be performed while running {@link HDF5Archiver#list}.
 * 
 * @author Bernd Rinn
 */
public enum Check
{
    /** Do not perform any check. */
    NO_CHECK,
    /** Check CRC32 checksums against archive content. */
    CHECK_CRC_ARCHIVE,
    /** Verify CRC32 checksums against the file system. */
    VERIFY_CRC_FS,
    /** Verify CRC32 checksums and attributes against the file system. */
    VERIFY_CRC_ATTR_FS;
}