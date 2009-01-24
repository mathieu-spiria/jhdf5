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
import java.util.ArrayList;
import java.util.List;
import ch.systemsx.cisd.args4j.Argument;
import ch.systemsx.cisd.args4j.CmdLineException;
import ch.systemsx.cisd.args4j.CmdLineParser;
import ch.systemsx.cisd.args4j.ExampleMode;
import ch.systemsx.cisd.args4j.Option;
import ch.systemsx.cisd.common.utilities.BuildAndEnvironmentInfo;

/**
 * The main class of the HDF5 based archiver.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiverMain
{

    private enum Command
    {
        ARCHIVE(new String[]
            { "A", "AR", "ARCHIVE" }, false), EXTRACT(new String[]
            { "E", "EX", "EXTRACT" }, true), DELETE(new String[]
            { "D", "RM", "DELETE", "REMOVE" }, false), LIST(new String[]
            { "L", "LS", "LIST" }, true), HELP(new String[]
            { "H", "HELP" }, true);

        String[] forms;

        boolean readOnly;

        Command(String[] forms, boolean readOnly)
        {
            this.forms = forms;
            this.readOnly = readOnly;
        }

        boolean isReadOnly()
        {
            return readOnly;
        }

        static Command parse(String commandStr)
        {
            final String commandStrU = commandStr.toUpperCase();
            for (Command cmd : values())
            {
                for (String frm : cmd.forms)
                {
                    if (frm.equals(commandStrU))
                    {
                        return cmd;
                    }
                }
            }
            return HELP;
        }
    }

    @Argument
    private List<String> arguments;

    private Command command;

    private File archiveFile;

    @Option(name = "i", longName = "include", metaVar = "REGEX", skipForExample = true, usage = "Regex of files to include")
    private List<String> fileWhiteList = new ArrayList<String>();

    @Option(name = "e", longName = "exclude", metaVar = "REGEX", usage = "Regex of files to exclude")
    private List<String> fileBlackList = new ArrayList<String>();

    @Option(longName = "include-dirs", metaVar = "REGEX", skipForExample = true, usage = "Regex of directories to include")
    private List<String> dirWhiteList = new ArrayList<String>();

    @Option(longName = "exclude-dirs", metaVar = "REGEX", skipForExample = true, usage = "Regex of directories to exclude")
    private List<String> dirBlackList = new ArrayList<String>();

    @Option(name = "c", longName = "compress", metaVar = "REGEX", skipForExample = true, usage = "Regex of files to compress")
    private List<String> compressionWhiteList = new ArrayList<String>();

    @Option(name = "C", longName = "compress-all", usage = "Regex of files to compress")
    private boolean compressAll = false;

    @Option(name = "r", longName = "root-dir", metaVar = "DIR", usage = "Root directory for archiving / extracting")
    private File rootOrNull;

    @Option(name = "R", longName = "recursive", usage = "Recursive output")
    private boolean recursive = false;
    
    @Option(name = "v", longName = "verbose", usage = "Verbose output")
    private boolean verbose = false;

    @Option(name = "n", longName = "numeric", usage = "Use numeric values for mode, uid and gid when listing")
    private boolean numeric = false;
    
    @Option(longName = "latest-file-format", skipForExample = true, usage = "If true, an HDF5 file with the latest file format will be created")
    private boolean useLatestFileFormat = false;

    @Option(longName = "stop-on-error", skipForExample = true, usage = "Stop on first error and give detailed error report")
    private boolean stopOnError = false;

    private HDF5Archiver archiver;

    /**
     * The command line parser.
     */
    private final CmdLineParser parser = new CmdLineParser(this);

    private HDF5ArchiverMain(String[] args)
    {
        try
        {
            parser.parseArgument(args);
        } catch (CmdLineException ex)
        {
            System.err.printf("Error when parsing command line: '%s'\n", ex.getMessage());
            printHelpAndExit(true);
        }
        if (arguments == null || arguments.size() < 2)
        {
            printHelpAndExit(true);
        }
        command = Command.parse(arguments.get(0));
        if (command == null || command == Command.HELP)
        {
            printHelpAndExit(true);
        }
        if (arguments.get(1).endsWith(".h5"))
        {
            archiveFile = new File(arguments.get(1));
        } else
        {
            archiveFile = new File(arguments.get(1) + ".h5");
        }
        if (command.isReadOnly() && archiveFile.exists() == false)
        {
            System.err.println("Archive '" + archiveFile.getAbsolutePath() + "' does not exist.");
        }
    }

    @Option(longName = "version", skipForExample = true, usage = "Prints out the version information")
    void printVersion(final boolean exit)
    {
        System.err.println("HDF5 archiver version "
                + BuildAndEnvironmentInfo.INSTANCE.getFullVersion());
        if (exit)
        {
            System.exit(0);
        }
    }

    @Option(longName = "help", skipForExample = true, usage = "Shows this help text")
    void printHelpAndExit(final boolean dummy)
    {
        parser.printHelp("hdf5archiver", "[option [...]]",
                "[ARCHIVE <archive_file> <item-to-archive> [...] | "
                        + "EXTRACT <archive_file> [<item-to-unarchive> [...]] | "
                        + "DELETE <archive_file> <item-to-delete> [...] | "
                        + "LIST <archive_file>]", ExampleMode.NONE);
        System.err.println("java -jar jhdf5.jar" + parser.printExample(ExampleMode.ALL)
                + " ARCHIVE archive.h5 .");
        System.exit(0);
    }

    private void createArchiver()
    {
        archiver =
                new HDF5Archiver(archiveFile, command.isReadOnly(), useLatestFileFormat,
                        (stopOnError == false));
        archiver.getStrategy().setCompressAll(compressAll);
        for (String pattern : fileWhiteList)
        {
            archiver.getStrategy().addToFileWhiteList(pattern);
        }
        for (String pattern : fileBlackList)
        {
            archiver.getStrategy().addToFileBlackList(pattern);
        }
        for (String pattern : dirWhiteList)
        {
            archiver.getStrategy().addToDirWhiteList(pattern);
        }
        for (String pattern : dirBlackList)
        {
            archiver.getStrategy().addToDirBlackList(pattern);
        }
        for (String pattern : fileWhiteList)
        {
            archiver.getStrategy().addToFileWhiteList(pattern);
        }
        for (String pattern : compressionWhiteList)
        {
            archiver.getStrategy().addToCompressionWhiteList(pattern);
        }
    }

    void run()
    {
        try
        {
            switch (command)
            {
                case ARCHIVE:
                    if (arguments.size() == 2)
                    {
                        System.err.println("Nothing to archive.");
                        printHelpAndExit(true);
                    }
                    createArchiver();
                    if (rootOrNull != null)
                    {
                        for (int i = 2; i < arguments.size(); ++i)
                        {
                            archiver.archive(rootOrNull, new File(rootOrNull, arguments.get(i)),
                                    verbose);
                        }
                    } else
                    {
                        for (int i = 2; i < arguments.size(); ++i)
                        {
                            archiver.archiveAll(new File(arguments.get(i)), verbose);
                        }
                    }
                    break;
                case EXTRACT:
                    createArchiver();
                    if (arguments.size() == 2)
                    {
                        if (rootOrNull != null)
                        {
                            archiver.extract(rootOrNull, "/", verbose);
                        } else
                        {
                            archiver.extract(new File("."), "/", verbose);
                        }
                    } else
                    {
                        if (rootOrNull != null)
                        {
                            for (int i = 2; i < arguments.size(); ++i)
                            {
                                archiver.extract(rootOrNull, arguments.get(i), verbose);
                            }
                        } else
                        {
                            for (int i = 2; i < arguments.size(); ++i)
                            {
                                archiver.extract(new File("."), arguments.get(i), verbose);
                            }
                        }
                    }
                    break;
                case DELETE:
                    if (arguments.size() == 2)
                    {
                        System.err.println("Nothing to delete.");
                        printHelpAndExit(true);
                    }
                    createArchiver();
                    archiver.delete(arguments.subList(2, arguments.size()), verbose);
                    break;
                case LIST:
                    createArchiver();
                    final String dir = (arguments.size() > 2) ? arguments.get(2) : "/";
                    for (String s : archiver.list(dir, recursive, verbose, numeric))
                    {
                        System.out.println(s);
                    }
                    break;
                case HELP: // Can't happen any more at this point
                    printHelpAndExit(true);
                    break;
            }
        } finally
        {
            if (archiver != null)
            {
                archiver.close();
            }
        }
    }

    public static void main(String[] args)
    {
        final HDF5ArchiverMain main = new HDF5ArchiverMain(args);
        main.run();
    }

}
