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

import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

/**
 * Utility routines for the output of the HDF5 archiver to the console.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiveOutputHelper
{

    static void dealWithError(final ArchiverException ex, boolean continueOnError)
            throws ArchivingException
    {
        if (continueOnError)
        {
            System.err.println(ex.getMessage());
        } else
        {
            if (ex.getCause() instanceof HDF5LibraryException)
            {
                System.err.println(((HDF5LibraryException) ex.getCause())
                        .getHDF5ErrorStackAsString());
            }
            throw ex;
        }
    }

    static void writeToConsole(String hdf5ObjectPath, boolean verbose)
    {
        if (verbose)
        {
            System.out.println(hdf5ObjectPath);
        }
    }

    static void writeToConsole(String hdf5ObjectPath, boolean checksumStored,
            boolean checksumOK, int crc32, boolean verbose)
    {
        if (verbose)
        {
            if (checksumStored)
            {
                if (checksumOK)
                {
                    System.out.println(hdf5ObjectPath + "\t" + ListEntry.hashToString(crc32)
                            + "\tOK");
                } else
                {
                    System.out.println(hdf5ObjectPath + "\t" + ListEntry.hashToString(crc32)
                            + "\tFAILED");
                }
            } else
            {
                System.out.println(hdf5ObjectPath + "\t" + ListEntry.hashToString(crc32)
                        + "\t(NO CHECKSUM)");
            }
        }
    }

}
