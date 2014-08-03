/*
 * Copyright 2007 - 2014 ETH Zuerich, CISD and SIS.
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
 * A static wrapper for the {@link IHDF5Factory} for creating writers and readers of HDF5 files. For
 * straight-forward creation, see methods {@link #open(File)} and {@link #openForReading(File)}. If
 * you need full control over the creation process, see the methods {@link #configure(File)} and
 * {@link #configureForReading(File)}.
 * 
 * @author Bernd Rinn
 */
public final class HDF5Factory
{

    /**
     * Opens an HDF5 <var>file</var> for writing and reading. If the file does not yet exist, it
     * will be created.
     */
    public static IHDF5Writer open(File file)
    {
        return HDF5FactoryProvider.get().open(file);
    }

    /**
     * Opens an HDF5 file named <var>filePath</var> for writing and reading. If the file does not
     * yet exist, it will be created.
     */
    public static IHDF5Writer open(String filePath)
    {
        return HDF5FactoryProvider.get().open(new File(filePath));
    }

    /**
     * Opens an HDF5 <var>file</var> for reading. It is an error if the file does not exist.
     */
    public static IHDF5Reader openForReading(File file)
    {
        return HDF5FactoryProvider.get().openForReading(file);
    }

    /**
     * Opens an HDF5 file named <var>filePath</var> for reading. It is an error if the file does not
     * exist.
     */
    public static IHDF5Reader openForReading(String filePath)
    {
        return HDF5FactoryProvider.get().openForReading(new File(filePath));
    }

    /**
     * Opens a configurator for an HDF5 <var>file</var> for writing and reading. Configure the
     * writer as you need and then call {@link IHDF5WriterConfigurator#writer()} in order to start
     * reading and writing the file.
     */
    public static IHDF5WriterConfigurator configure(File file)
    {
        return HDF5FactoryProvider.get().configure(file);
    }

    /**
     * Opens a configurator for an HDF5 file named <var>filePath</var> for writing and reading.
     * Configure the writer as you need and then call {@link IHDF5WriterConfigurator#writer()} in
     * order to start reading and writing the file.
     */
    public static IHDF5WriterConfigurator configure(String filePath)
    {
        return HDF5FactoryProvider.get().configure(new File(filePath));
    }

    /**
     * Opens a configurator for an HDF5 <var>file</var> for reading. Configure the reader as you
     * need and then call {@link IHDF5ReaderConfigurator#reader()} in order to start reading the
     * file.
     */
    public static IHDF5ReaderConfigurator configureForReading(File file)
    {
        return HDF5FactoryProvider.get().configureForReading(file);
    }

    /**
     * Opens a configurator for an HDF5 file named <var>filePath</var> for reading. Configure the
     * reader as you need and then call {@link IHDF5ReaderConfigurator#reader()} in order to start
     * reading the file.
     */
    public static IHDF5ReaderConfigurator configureForReading(String filePath)
    {
        return HDF5FactoryProvider.get().configureForReading(new File(filePath));
    }

    /**
     * Returns <code>true</code>, if the <var>file</var> is an HDF5 file and <code>false</code>
     * otherwise.
     */
    public static boolean isHDF5File(File file)
    {
        return HDF5FactoryProvider.get().isHDF5File(file);
    }

    /**
     * Returns <code>true</code>, if the file named <var>filePath</var> is an HDF5 file and
     * <code>false</code> otherwise.
     */
    public static boolean isHDF5File(String filePath)
    {
        return HDF5FactoryProvider.get().isHDF5File(new File(filePath));
    }

}
