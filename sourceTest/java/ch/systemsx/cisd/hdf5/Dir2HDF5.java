/*
 * Copyright 2007 ETH Zuerich, CISD
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
import java.io.IOException;

import org.apache.commons.lang.time.StopWatch;

import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;
import ch.systemsx.cisd.hdf5.tools.HDF5Archiver;

/**
 * Creates a HDF5 file from a directory.
 * 
 * @author Bernd Rinn
 */
public class Dir2HDF5
{

    public static void main(String[] args) throws IOException
    {
        if (args.length == 0)
        {
            System.err.println("Syntax: Dir2HDF5 <hdf5 file> [<file or dir> ...]");
            System.exit(1);
        }
        final File hdf5File = new File(args[0]);
        final StopWatch watch = new StopWatch();
        watch.start();
        final HDF5Archiver archiver =
                new HDF5Archiver(hdf5File, false, true, FileFormat.ALLOW_1_8, false);
        if (args.length > 1)
        {
            for (int i = 1; i < args.length; ++i)
            {
                archiver.archiveAll(new File(args[i]), true);
            }
        } else
        {
            archiver.archiveAll(new File("."), true);
        }
        archiver.close();
        watch.stop();
        System.out.println("Creating hdf5 archive took " + watch);
    }

}
