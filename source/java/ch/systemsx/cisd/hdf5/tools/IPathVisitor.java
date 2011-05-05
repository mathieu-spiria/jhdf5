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
 * A visitor for paths in an HDF5 archive.
 * 
 * @author Bernd Rinn
 */
public interface IPathVisitor
{
    public final static IPathVisitor DEFAULT_PATH_VISITOR = new IPathVisitor()
    {
        public void visit(String path)
        {
            System.out.println(path);
        }

        public void visit(String filePath, boolean checksumStored, boolean checksumOK, int crc32)
        {
            if (checksumStored)
            {
                if (checksumOK)
                {
                    System.out
                            .println(filePath + "\t" + ListEntry.hashToString(crc32) + "\tOK");
                } else
                {
                    System.out.println(filePath + "\t" + ListEntry.hashToString(crc32)
                            + "\tFAILED");
                }
            } else
            {
                System.out.println(filePath + "\t" + ListEntry.hashToString(crc32)
                        + "\t(NO CHECKSUM)");
            }
        }
    };

    /**
     * Called for each <var>path</var> that is visited.
     */
    public void visit(String path);

    /**
     * Called for each <var>path</var> representing a file that is visited.
     */
    public void visit(String filePath, boolean checksumStored, boolean checksumOK, int crc32);
}
