/*
 * Copyright 2012 ETH Zuerich, CISD
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

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * Test cases for {@link Utils}.
 *
 * @author Bernd Rinn
 */
public class UtilsTest
{
    @Test
    public void testNormalizePath()
    {
        assertEquals("/a", Utils.normalizePath("a"));
        assertEquals("/a", Utils.normalizePath("a/"));
        assertEquals("/a", Utils.normalizePath("/a/"));
        assertEquals("/a/b/c", Utils.normalizePath("a/b/c"));
        assertEquals("/a/c", Utils.normalizePath("a/b/../c/./"));
    }
    
}
