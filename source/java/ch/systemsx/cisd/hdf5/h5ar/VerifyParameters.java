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

package ch.systemsx.cisd.hdf5.h5ar;

/**
 * A class that represents parameters for
 * {@link HDF5Archiver#verifyAgainstFilesystem(String, String, IListEntryVisitor, VerifyParameters)
 * )} .
 * 
 * @author Bernd Rinn
 */
public final class VerifyParameters
{
    private final boolean recursive;

    private final boolean numeric;

    private final boolean verifyAttributes;

    public static final VerifyParameters DEFAULT = new VerifyParameters(true, false, false);

    public static final class VerifyParametersBuilder
    {
        private boolean recursive = true;

        private boolean numeric = false;

        private boolean verifyAttributes = false;

        private VerifyParametersBuilder()
        {
        }

        public VerifyParametersBuilder nonRecursive()
        {
            this.recursive = false;
            return this;
        }

        public VerifyParametersBuilder recursive(@SuppressWarnings("hiding")
        boolean recursive)
        {
            this.recursive = recursive;
            return this;
        }

        public VerifyParametersBuilder numeric()
        {
            this.numeric = true;
            return this;
        }

        public VerifyParametersBuilder numeric(@SuppressWarnings("hiding")
        boolean numeric)
        {
            this.numeric = numeric;
            return this;
        }

        public VerifyParametersBuilder verifyAttributes()
        {
            this.verifyAttributes = true;
            return this;
        }

        public VerifyParametersBuilder verifyAttributes(@SuppressWarnings("hiding")
        boolean verifyAttributes)
        {
            this.verifyAttributes = verifyAttributes;
            return this;
        }

        public VerifyParameters get()
        {
            return new VerifyParameters(recursive, numeric, verifyAttributes);
        }
    }

    public static VerifyParametersBuilder build()
    {
        return new VerifyParametersBuilder();
    }

    private VerifyParameters(boolean recursive, boolean numeric, boolean verifyAttributes)
    {
        this.recursive = recursive;
        this.numeric = numeric;
        this.verifyAttributes = verifyAttributes;
    }

    public boolean isRecursive()
    {
        return recursive;
    }

    public boolean isNumeric()
    {
        return numeric;
    }

    public boolean isVerifyAttributes()
    {
        return verifyAttributes;
    }

}
