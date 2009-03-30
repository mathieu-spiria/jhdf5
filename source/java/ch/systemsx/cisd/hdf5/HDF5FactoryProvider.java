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
 * Provides access to a factory for HDF5 readers and writers.
 * 
 * @author Bernd Rinn
 */
public final class HDF5FactoryProvider
{
    private static class HDF5Factory implements IHDF5Factory
    {

        public IHDF5WriterConfigurator configure(File file)
        {
            return new HDF5WriterConfigurator(file);
        }

        public IHDF5ReaderConfigurator configureForReading(File file)
        {
            return new HDF5ReaderConfigurator(file);
        }

        public IHDF5Writer open(File file)
        {
            return new HDF5WriterConfigurator(file).writer();
        }

        public IHDF5Reader openForReading(File file)
        {
            return new HDF5ReaderConfigurator(file).reader();
        }

    }

    /**
     * The (only) instance of the factory.
     */
    private static IHDF5Factory factory = new HDF5Factory();

    private HDF5FactoryProvider()
    {
        // Not to be instantiated.
    }

    /**
     * Returns the {@link IHDF5Factory}. This is your access to creation of {@link IHDF5Reader} and
     * {@link IHDF5Writer} instances.
     */
    public static synchronized IHDF5Factory get()
    {
        return factory;
    }

    /**
     * Sets the {@link IHDF5Factory}. In normal operation this method is not used, but it is a hook
     * that can be used if you need to track or influence the factory's operation, for example for
     * mocking in unit tests.
     */
    public static synchronized void set(IHDF5Factory factory)
    {
        HDF5FactoryProvider.factory = factory;
    }

}
