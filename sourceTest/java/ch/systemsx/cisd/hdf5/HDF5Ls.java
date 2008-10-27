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
import java.util.List;

import org.apache.commons.lang.time.StopWatch;

import ch.systemsx.cisd.hdf5.tools.HDF5Archiver;

/**
 * @author Bernd Rinn
 */
public class HDF5Ls
{

    public static void main(String[] args)
    {
        if (args.length != 1)
        {
            System.err.println("Syntax: HDF5Ls <hdf5 file>");
            System.exit(1);
        }
        final StopWatch watch = new StopWatch();
        watch.start();
        final HDF5Archiver archiver = new HDF5Archiver(new File(args[0]), true, false, false);
        final List<String> entries = archiver.list(true, false);
        archiver.close();
        for (String e : entries)
        {
            System.out.println(e);
        }
        watch.stop();
        System.out.println("Listing hdf5 file took " + watch);
    }

}
