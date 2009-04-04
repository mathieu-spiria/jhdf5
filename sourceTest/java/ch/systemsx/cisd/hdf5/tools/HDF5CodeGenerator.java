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

package ch.systemsx.cisd.hdf5.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

/**
 * A code generator for the identical parts of the HDF5 Java classes class for different numerical
 * types.
 * 
 * @author Bernd Rinn
 */
public class HDF5CodeGenerator
{

    static class TemplateParameters
    {
        final String name;

        final String appendix;

        final String capitalizedName;

        final String capitalizedClassName;

        final String upperCaseClassName;

        final String wrapperName;

        final String storageType;

        final String memoryType;

        final String elementSize;

        TemplateParameters(String name, String appendix, String capitalizedName,
                String capitalizedClassName, String wrapperName, String storageType,
                String memoryType, String elementSize)
        {
            this.name = name;
            this.appendix = appendix;
            this.capitalizedName = capitalizedName;
            this.capitalizedClassName = capitalizedClassName;
            this.upperCaseClassName = capitalizedClassName.toUpperCase();
            this.wrapperName = wrapperName;
            this.storageType = storageType;
            this.memoryType = memoryType;
            this.elementSize = elementSize;
        }

    }

    static TemplateParameters tp(String name, String appendix, String capitalizedName,
            String capitalizedClassName, String wrapperName, String storageType, String memoryType,
            String elementSize)
    {
        return new TemplateParameters(name, appendix, capitalizedName, capitalizedClassName,
                wrapperName, storageType, memoryType, elementSize);
    }

    static final TemplateParameters PLACEHOLDERS =
            tp("__name__", "__appendix__", "__Name__", "__Classname__", "__Wrappername__",
                    "__Storagetype__", "__Memorytype__", "__elementsize__");

    static final TemplateParameters[] NUMERICAL_TYPES =
            new TemplateParameters[]
                {
                        tp("byte", "", "Byte", "Int", "Byte", "H5T_STD_I8LE", "H5T_NATIVE_INT8", "1"),
                        tp("short", "_short", "Short", "Int", "Short", "H5T_STD_I16LE",
                                "H5T_NATIVE_INT16", "2"),
                        tp("int", "_int", "Int", "Int", "Integer", "H5T_STD_I32LE",
                                "H5T_NATIVE_INT32", "4"),
                        tp("long", "_long", "Long", "Int", "Long", "H5T_STD_I64LE",
                                "H5T_NATIVE_INT64", "8"),
                        tp("float", "_float", "Float", "Float", "Float", "H5T_IEEE_F32LE",
                                "H5T_NATIVE_FLOAT", "4"),
                        tp("double", "_double", "Double", "Float", "Double", "H5T_IEEE_F64LE",
                                "H5T_NATIVE_DOUBLE", "8") };

    /**
     * Generate the code for all numerical types from <var>codeTemplate</var> and write it to
     * <code>stdout</code>.
     */
    static void generateCode(final String codeTemplate)
    {
        for (TemplateParameters t : NUMERICAL_TYPES)
        {
            generateCode(codeTemplate, t, System.out);
        }
    }

    /**
     * Generate the code for all numerical types from <var>codeTemplate</var> and write it to
     * <code>out</code>.
     */
    static void generateCode(final String codeTemplate, final TemplateParameters params,
            final PrintStream out)
    {
        String s = codeTemplate;
        s = StringUtils.replace(s, PLACEHOLDERS.name, params.name);
        s = StringUtils.replace(s, PLACEHOLDERS.appendix, params.appendix);
        s = StringUtils.replace(s, PLACEHOLDERS.capitalizedName, params.capitalizedName);
        s = StringUtils.replace(s, PLACEHOLDERS.capitalizedClassName, params.capitalizedClassName);
        s = StringUtils.replace(s, PLACEHOLDERS.upperCaseClassName, params.upperCaseClassName);
        s = StringUtils.replace(s, PLACEHOLDERS.wrapperName, params.wrapperName);
        s = StringUtils.replace(s, PLACEHOLDERS.storageType, params.storageType);
        s = StringUtils.replace(s, PLACEHOLDERS.memoryType, params.memoryType);
        s = StringUtils.replace(s, PLACEHOLDERS.elementSize, params.elementSize);
        out.println(s);
    }

    public static void main(String[] args) throws IOException
    {
        for (TemplateParameters t : NUMERICAL_TYPES)
        {
            final String interfaceTemplateReader =
                    FileUtils
                            .readFileToString(new File(
                                    "sourceTest/java/ch/systemsx/cisd/hdf5/IHDF5PrimitiveReader.java.templ"));
            final PrintStream outInterfaceReader =
                    new PrintStream(new File("source/java/ch/systemsx/cisd/hdf5/IHDF5"
                            + t.capitalizedName + "Reader.java"));
            generateCode(interfaceTemplateReader, t, outInterfaceReader);
            outInterfaceReader.close();
            final String classTemplateReader =
                    FileUtils
                            .readFileToString(new File(
                                    "sourceTest/java/ch/systemsx/cisd/hdf5/HDF5PrimitiveReader.java.templ"));
            final PrintStream outclassReader =
                    new PrintStream(new File("source/java/ch/systemsx/cisd/hdf5/HDF5"
                            + t.capitalizedName + "Reader.java"));
            generateCode(classTemplateReader, t, outclassReader);
            outclassReader.close();
            final String interfaceTemplateWriter =
                    FileUtils
                            .readFileToString(new File(
                                    "sourceTest/java/ch/systemsx/cisd/hdf5/IHDF5PrimitiveWriter.java.templ"));
            final PrintStream outInterfaceWriter =
                    new PrintStream(new File("source/java/ch/systemsx/cisd/hdf5/IHDF5"
                            + t.capitalizedName + "Writer.java"));
            generateCode(interfaceTemplateWriter, t, outInterfaceWriter);
            outInterfaceWriter.close();
            final String classTemplateWriter =
                    FileUtils
                            .readFileToString(new File(
                                    "sourceTest/java/ch/systemsx/cisd/hdf5/HDF5PrimitiveWriter.java.templ"));
            final PrintStream outclassWriter =
                    new PrintStream(new File("source/java/ch/systemsx/cisd/hdf5/HDF5"
                            + t.capitalizedName + "Writer.java"));
            generateCode(classTemplateWriter, t, outclassWriter);
            outclassWriter.close();
        }
    }
}
