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

import ncsa.hdf.hdf5lib.exceptions.HDF5JavaException;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import ch.systemsx.cisd.args4j.Argument;
import ch.systemsx.cisd.args4j.CmdLineException;
import ch.systemsx.cisd.args4j.CmdLineParser;
import ch.systemsx.cisd.args4j.ExampleMode;
import ch.systemsx.cisd.args4j.Option;
import ch.systemsx.cisd.hdf5.BuildAndEnvironmentInfo;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator.FileFormat;
import ch.systemsx.cisd.hdf5.tools.HDF5ArchiveTools.Check;
import ch.systemsx.cisd.hdf5.tools.HDF5ArchiveTools.ListEntry;

/**
 * The main class of the HDF5 based archiver.
 * 
 * @author Bernd Rinn
 */
public class HDF5ArchiverMain
{

    private static final String FILE_EXTENSION_H5 = ".h5";

    private static final String FILE_EXTENSION_H5AR = ".h5ar";

    private enum Command
    {
        ARCHIVE(new String[]
            { "A", "AR", "ARCHIVE" }, false), EXTRACT(new String[]
            { "E", "EX", "EXTRACT" }, true), DELETE(new String[]
            { "D", "RM", "DELETE", "REMOVE" }, false), LIST(new String[]
            { "L", "LS", "LIST" }, true), VERIFY(new String[]
            { "V", "VF", "VERIFY" }, true), HELP(new String[]
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

    private final boolean initializationOK;

    @Option(name = "i", longName = "include", metaVar = "REGEX", skipForExample = true, usage = "Regex of files to include")
    private List<String> fileWhiteList = new ArrayList<String>();

    @Option(name = "e", longName = "exclude", metaVar = "REGEX", usage = "Regex of files to exclude")
    private List<String> fileBlackList = new ArrayList<String>();

    @Option(name = "I", longName = "include-dirs", metaVar = "REGEX", skipForExample = true, usage = "Regex of directories to include")
    private List<String> dirWhiteList = new ArrayList<String>();

    @Option(name = "E", longName = "exclude-dirs", metaVar = "REGEX", skipForExample = true, usage = "Regex of directories to exclude")
    private List<String> dirBlackList = new ArrayList<String>();

    @Option(name = "c", longName = "compress", metaVar = "REGEX", skipForExample = true, usage = "Regex of files to compress")
    private List<String> compressionWhiteList = new ArrayList<String>();

    @Option(name = "C", longName = "compress-all", usage = "Regex of files to compress")
    private boolean compressAll = false;

    @Option(name = "r", longName = "root-dir", metaVar = "DIR", usage = "Root directory for archiving / extracting")
    private File rootOrNull;

    @Option(name = "D", longName = "suppress-directories", usage = "Supress output for directories itself for LIST and VERIFY")
    private boolean suppressDirectoryEntries = false;

    @Option(name = "R", longName = "recursive", usage = "Recursive LIST and VERIFY")
    private boolean recursive = false;

    @Option(name = "v", longName = "verbose", usage = "Verbose output (all operations)")
    private boolean verbose = false;

    @Option(name = "n", longName = "numeric", usage = "Use numeric values for mode, uid and gid for LIST and VERIFY")
    private boolean numeric = false;

    @Option(name = "t", longName = "test-checksums", usage = "Test CRC32 checksums of files in archive for LIST")
    private boolean testAgainstChecksums = false;

    @Option(name = "a", longName = "verify-attributes", usage = "Consider file attributes for VERIFY")
    private boolean verifyAttributes = false;

    @Option(longName = "file-format", skipForExample = true, usage = "Specifies the file format version when creating an archive (N=1 -> HDF51.6 (default), N=2 -> HDF51.8)")
    private int fileFormat = 1;

    @Option(longName = "stop-on-error", skipForExample = true, usage = "Stop on first error and give detailed error report")
    private boolean stopOnError = false;

    @Option(longName = "no-sync", skipForExample = true, usage = "Do not sync to disk before program exits (writing mode only)")
    private boolean noSync = false;

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
            printHelp(true);
            initializationOK = false;
            return;
        }
        if (arguments == null || arguments.size() < 2)
        {
            printHelp(true);
            initializationOK = false;
            return;
        }
        command = Command.parse(arguments.get(0));
        if (command == null || command == Command.HELP)
        {
            printHelp(true);
            initializationOK = false;
            return;
        }
        if (arguments.get(1).endsWith(FILE_EXTENSION_H5)
                || arguments.get(1).endsWith(FILE_EXTENSION_H5AR))
        {
            archiveFile = new File(arguments.get(1));
        } else
        {
            archiveFile = new File(arguments.get(1) + FILE_EXTENSION_H5AR);
            if (command.isReadOnly() && archiveFile.exists() == false)
            {
                archiveFile = new File(arguments.get(1) + FILE_EXTENSION_H5);
                if (command.isReadOnly() && archiveFile.exists() == false)
                {
                    archiveFile = new File(arguments.get(1));
                }
            }
        }
        if (command.isReadOnly() && archiveFile.exists() == false)
        {
            System.err.println("Archive '" + archiveFile.getAbsolutePath() + "' does not exist.");
            initializationOK = false;
            return;
        }
        initializationOK = true;
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

    private boolean helpPrinted = false;

    @Option(longName = "help", skipForExample = true, usage = "Shows this help text")
    void printHelp(final boolean dummy)
    {
        if (helpPrinted)
        {
            return;
        }
        parser.printHelp("h5ar", "[option [...]]",
                "[ARCHIVE <archive_file> <item-to-archive> [...] | "
                        + "EXTRACT <archive_file> [<item-to-unarchive> [...]] | "
                        + "DELETE <archive_file> <item-to-delete> [...] | "
                        + "LIST <archive_file>]", ExampleMode.NONE);
        System.err
                .println("Command aliases: ARCHIVE: A, AR; EXTRACT: E, EX; DELETE: D, REMOVE, RM; LIST: L, LS");
        System.err.println("Example: h5ar" + parser.printExample(ExampleMode.ALL)
                + " ARCHIVE archive.h5ar .");
        helpPrinted = true;
    }

    private boolean createArchiver()
    {
        final FileFormat fileFormatEnum =
                (fileFormat == 1) ? FileFormat.STRICTLY_1_6 : FileFormat.STRICTLY_1_8;
        try
        {
            archiver =
                    new HDF5Archiver(archiveFile, command.isReadOnly(), noSync, fileFormatEnum,
                            (stopOnError == false));
        } catch (HDF5JavaException ex)
        {
            // Problem opening the archive file: non readable / writable
            System.err.println("Error opening archive file: " + ex.getMessage());
            return false;
        } catch (HDF5LibraryException ex)
        {
            // Problem opening the archive file: corrupt file
            System.err.println("Error opening archive file: corrupt file ["
                    + ex.getClass().getSimpleName() + ": " + ex.getMessage() + "]");
            return false;
        }
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
        return true;
    }

    private File getFSRoot()
    {
        return (rootOrNull == null) ? new File(".") : rootOrNull;
    }

    private static class ListingVisitor implements HDF5ArchiveTools.ListEntryVisitor
    {
        private final boolean verifying;

        private int checkSumFailures;

        ListingVisitor(boolean verifying)
        {
            this.verifying = verifying;
        }

        public void visit(ListEntry entry)
        {
            if (verifying)
            {
                final boolean ok = entry.checkOK();
                final String statusStr = ok ? "\tOK" : "\tFAILED";
                System.out.printf("%s%s\n", entry.outputLine, statusStr);
                if (ok == false)
                {
                    System.err.println(entry.errorLineOrNull);
                    ++checkSumFailures;
                }
            } else
            {
                System.out.println(entry.outputLine);
            }
        }

        boolean isOK()
        {
            if (verifying && checkSumFailures > 0)
            {
                System.err.println(checkSumFailures + " file(s) failed the CRC checksum test.");
                return false;
            } else
            {
                return true;
            }
        }
    }

    boolean run()
    {
        if (initializationOK == false)
        {
            return false;
        }
        try
        {
            switch (command)
            {
                case ARCHIVE:
                    if (arguments.size() == 2)
                    {
                        System.err.println("Nothing to archive.");
                        printHelp(true);
                    }
                    if (createArchiver() == false)
                    {
                        break;
                    }
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
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    if (arguments.size() == 2)
                    {
                        archiver.extract(getFSRoot(), "/", verbose);
                    } else
                    {
                        for (int i = 2; i < arguments.size(); ++i)
                        {
                            archiver.extract(getFSRoot(), arguments.get(i), verbose);
                        }
                    }
                    break;
                case DELETE:
                    if (arguments.size() == 2)
                    {
                        System.err.println("Nothing to delete.");
                        printHelp(true);
                        break;
                    }
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    archiver.delete(arguments.subList(2, arguments.size()), verbose);
                    break;
                case VERIFY:
                {
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    final String fileOrDir = (arguments.size() > 2) ? arguments.get(2) : "/";
                    final ListingVisitor visitor = new ListingVisitor(true);
                    archiver.list(fileOrDir, getFSRoot().getPath(), recursive,
                            suppressDirectoryEntries, verbose, numeric,
                            verifyAttributes ? Check.VERIFY_CRC_ATTR_FS : Check.VERIFY_CRC_FS,
                            visitor);
                    return visitor.isOK();
                }
                case LIST:
                {
                    if (createArchiver() == false)
                    {
                        break;
                    }
                    final String fileOrDir = (arguments.size() > 2) ? arguments.get(2) : "/";
                    final ListingVisitor visitor = new ListingVisitor(testAgainstChecksums);
                    archiver.list(fileOrDir, getFSRoot().getPath(), recursive,
                            suppressDirectoryEntries, verbose, numeric,
                            testAgainstChecksums ? Check.CHECK_CRC_ARCHIVE : Check.NO_CHECK,
                            visitor);
                    return visitor.isOK();
                }
                case HELP: // Can't happen any more at this point
                    break;
            }
            return true;
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
        if (main.run() == false)
        {
            System.exit(1);
        }
    }

}
