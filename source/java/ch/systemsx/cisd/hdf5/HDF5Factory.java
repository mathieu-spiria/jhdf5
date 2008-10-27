/*
 * Copyright 2008 ETH Zuerich, CISD
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

import java.io.File;

/**
 * A factory for creating simple writers and readers of HDF5 files.
 * <p>
 * If you need full control over the creation process, see the constructors and configuration
 * methods of {@link HDF5Writer} and {@link HDF5Reader}.
 * 
 * @author Bernd Rinn
 */
public class HDF5Factory
{

    private HDF5Factory()
    {
        // Not to be instantiated.
    }

    /**
     * Opens an HDF5 <var>file</var> for writing and reading. If the file does not yet exist, it
     * will be created.
     * <p>
     * Note: This method will add automatically add an extension <code>h5</code> to the file name if
     * it doesn't already have one.
     */
    public static HDF5SimpleWriter open(File file)
    {
        return new HDF5Writer(ensureExtension(file)).open();
    }

    /**
     * Opens an HDF5 <var>file</var> for reading. It is an error if the file does not exist.
     * <p>
     * Note: This method will add automatically add an extension <code>h5</code> to the file name if
     * it doesn't already have one.
     */
    public static HDF5SimpleReader openForReading(File file)
    {
        return new HDF5Reader(ensureExtension(file)).open();
    }

    private static File ensureExtension(File file)
    {
        String fileName = file.getAbsolutePath();
        if (fileName.endsWith(".h5") == false && file.exists() == false)
        {
            fileName += ".h5";
        }
        return new File(fileName);
    }
}
