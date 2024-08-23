package com.huawei.bdsolution.loadsmetric.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Shell {

    private static final Map<Shell, Object> CHILD_SHELLS =
            Collections.synchronizedMap(new WeakHashMap<Shell, Object>());

    public static final Logger LOG = LoggerFactory.getLogger(Shell.class);
    private static final String WINDOWS_PROBLEMS =
            "https://wiki.apache.org/hadoop/WindowsProblems";
    static final String WINUTILS_EXE = "winutils.exe";
    public static final String SYSPROP_HADOOP_HOME_DIR = "hadoop.home.dir";
    public static final String ENV_HADOOP_HOME = "HADOOP_HOME";

    @Deprecated
    public static boolean isJava7OrAbove() {
        return true;
    }
    public static final int WINDOWS_MAX_SHELL_LENGTH = 8191;

    @Deprecated
    public static final int WINDOWS_MAX_SHELL_LENGHT = WINDOWS_MAX_SHELL_LENGTH;

    public static String join(CharSequence separator, String[] strings) {
        // Ideally we don't have to duplicate the code here if array is iterable.
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s : strings) {
            if (first) {
                first = false;
            } else {
                sb.append(separator);
            }
            sb.append(s);
        }
        return sb.toString();
    }

    public static final Object WindowsProcessLaunchLock = new Object();

    // OSType detection

    public enum OSType {
        OS_TYPE_LINUX,
        OS_TYPE_WIN,
        OS_TYPE_SOLARIS,
        OS_TYPE_MAC,
        OS_TYPE_FREEBSD,
        OS_TYPE_OTHER
    }

    public static final OSType osType = getOSType();

    private static OSType getOSType() {
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows")) {
            return OSType.OS_TYPE_WIN;
        } else if (osName.contains("SunOS") || osName.contains("Solaris")) {
            return OSType.OS_TYPE_SOLARIS;
        } else if (osName.contains("Mac")) {
            return OSType.OS_TYPE_MAC;
        } else if (osName.contains("FreeBSD")) {
            return OSType.OS_TYPE_FREEBSD;
        } else if (osName.startsWith("Linux")) {
            return OSType.OS_TYPE_LINUX;
        } else {
            // Some other form of Unix
            return OSType.OS_TYPE_OTHER;
        }
    }

    // Helper static vars for each platform
    public static final boolean WINDOWS = (osType == OSType.OS_TYPE_WIN);
    public static final boolean SOLARIS = (osType == OSType.OS_TYPE_SOLARIS);
    public static final boolean MAC     = (osType == OSType.OS_TYPE_MAC);
    public static final boolean FREEBSD = (osType == OSType.OS_TYPE_FREEBSD);
    public static final boolean LINUX   = (osType == OSType.OS_TYPE_LINUX);
    public static final boolean OTHER   = (osType == OSType.OS_TYPE_OTHER);

    protected long timeOutInterval = 0L;
    private final AtomicBoolean timedOut = new AtomicBoolean(false);
    protected boolean inheritParentEnv = true;

    private static File checkHadoopHome() throws FileNotFoundException {
        String home = System.getProperty(SYSPROP_HADOOP_HOME_DIR);

        if (home == null) {
            home = System.getenv(ENV_HADOOP_HOME);
        }
        return checkHadoopHomeInner(home);
    }

    static final String E_DOES_NOT_EXIST = "does not exist";
    static final String E_IS_RELATIVE = "is not an absolute path.";
    static final String E_NOT_DIRECTORY = "is not a directory.";
    static final String E_NO_EXECUTABLE = "Could not locate Hadoop executable";
    static final String E_NOT_EXECUTABLE_FILE = "Not an executable file";
    static final String E_HADOOP_PROPS_UNSET = ENV_HADOOP_HOME + " and "
            + SYSPROP_HADOOP_HOME_DIR + " are unset.";
    static final String E_HADOOP_PROPS_EMPTY = ENV_HADOOP_HOME + " or "
            + SYSPROP_HADOOP_HOME_DIR + " set to an empty string";
    static final String E_NOT_A_WINDOWS_SYSTEM = "Not a Windows system";

    static File checkHadoopHomeInner(String home) throws FileNotFoundException {
        // couldn't find either setting for hadoop's home directory
        if (home == null) {
            throw new FileNotFoundException(E_HADOOP_PROPS_UNSET);
        }
        // strip off leading and trailing double quotes
        while (home.startsWith("\"")) {
            home = home.substring(1);
        }
        while (home.endsWith("\"")) {
            home = home.substring(0, home.length() - 1);
        }

        // after stripping any quotes, check for home dir being non-empty
        if (home.isEmpty()) {
            throw new FileNotFoundException(E_HADOOP_PROPS_EMPTY);
        }

        // check that the hadoop home dir value
        // is an absolute reference to a directory
        File homedir = new File(home);
        if (!homedir.isAbsolute()) {
            throw new FileNotFoundException("Hadoop home directory " + homedir
                    + " " + E_IS_RELATIVE);
        }
        if (!homedir.exists()) {
            throw new FileNotFoundException("Hadoop home directory " + homedir
                    + " " + E_DOES_NOT_EXIST);
        }
        if (!homedir.isDirectory()) {
            throw new FileNotFoundException("Hadoop home directory " + homedir
                    + " "+ E_NOT_DIRECTORY);
        }
        return homedir;
    }

    private static final File HADOOP_HOME_FILE;

    private static final IOException HADOOP_HOME_DIR_FAILURE_CAUSE;

    static {
        File home;
        IOException ex;
        try {
            home = checkHadoopHome();
            ex = null;
        } catch (IOException ioe) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to detect a valid hadoop home directory", ioe);
            }
            ex = ioe;
            home = null;
        }
        HADOOP_HOME_FILE = home;
        HADOOP_HOME_DIR_FAILURE_CAUSE = ex;
    }

    private static String addOsText(String message) {
        return WINDOWS ? (message + " -see " + WINDOWS_PROBLEMS) : message;
    }

    private static FileNotFoundException fileNotFoundException(String text, Exception ex) {
        return (FileNotFoundException) new FileNotFoundException(text).initCause(ex);
    }

    private static File getHadoopHomeDir() throws FileNotFoundException {
        if (HADOOP_HOME_DIR_FAILURE_CAUSE != null) {
            throw fileNotFoundException(
                    addOsText(HADOOP_HOME_DIR_FAILURE_CAUSE.toString()),
                    HADOOP_HOME_DIR_FAILURE_CAUSE);
        }
        return HADOOP_HOME_FILE;
    }

    public static File getQualifiedBin(String executable)
            throws FileNotFoundException {
        // construct hadoop bin path to the specified executable
        return getQualifiedBinInner(getHadoopHomeDir(), executable);
    }


    static File getQualifiedBinInner(File hadoopHomeDir, String executable)
            throws FileNotFoundException {
        String binDirText = "Hadoop bin directory ";
        File bin = new File(hadoopHomeDir, "bin");
        if (!bin.exists()) {
            throw new FileNotFoundException(addOsText(binDirText + E_DOES_NOT_EXIST
                    + ": " + bin));
        }
        if (!bin.isDirectory()) {
            throw new FileNotFoundException(addOsText(binDirText + E_NOT_DIRECTORY
                    + ": " + bin));
        }

        File exeFile = new File(bin, executable);
        if (!exeFile.exists()) {
            throw new FileNotFoundException(
                    addOsText(E_NO_EXECUTABLE + ": " + exeFile));
        }
        if (!exeFile.isFile()) {
            throw new FileNotFoundException(
                    addOsText(E_NOT_EXECUTABLE_FILE + ": " + exeFile));
        }
        try {
            return exeFile.getCanonicalFile();
        } catch (IOException e) {
            // this isn't going to happen, because of all the upfront checks.
            // so if it does, it gets converted to a FNFE and rethrown
            throw fileNotFoundException(e.toString(), e);
        }
    }

    @Deprecated
    public static final String WINUTILS;

    private static final String WINUTILS_PATH;

    private static final File WINUTILS_FILE;

    private static final IOException WINUTILS_FAILURE;

    static {
        IOException ioe = null;
        String path = null;
        File file = null;
        // invariant: either there's a valid file and path,
        // or there is a cached IO exception.
        if (WINDOWS) {
            try {
                file = getQualifiedBin(WINUTILS_EXE);
                path = file.getCanonicalPath();
                ioe = null;
            } catch (IOException e) {
                LOG.warn("Did not find {}: {}", WINUTILS_EXE, e);
                // stack trace comes at debug level
                LOG.debug("Failed to find " + WINUTILS_EXE, e);
                file = null;
                path = null;
                ioe = e;
            }
        } else {
            // on a non-windows system, the invariant is kept
            // by adding an explicit exception.
            ioe = new FileNotFoundException(E_NOT_A_WINDOWS_SYSTEM);
        }
        WINUTILS_PATH = path;
        WINUTILS_FILE = file;

        WINUTILS = path;
        WINUTILS_FAILURE = ioe;
    }

    public static File getWinUtilsFile() throws FileNotFoundException {
        if (WINUTILS_FAILURE == null) {
            return WINUTILS_FILE;
        } else {
            // raise a new exception to generate a new stack trace
            throw fileNotFoundException(WINUTILS_FAILURE.toString(),
                    WINUTILS_FAILURE);
        }
    }

    private static boolean isSetsidSupported() {
        if (Shell.WINDOWS) {
            return false;
        }
        ShellCommandExecutor shexec = null;
        boolean setsidSupported = true;
        try {
            String[] args = {"setsid", "bash", "-c", "echo $$"};
            shexec = new ShellCommandExecutor(args);
            shexec.execute();
        } catch (IOException ioe) {
            LOG.debug("setsid is not available on this machine. So not using it.");
            setsidSupported = false;
        } catch (SecurityException se) {
            LOG.debug("setsid is not allowed to run by the JVM "+
                    "security manager. So not using it.");
            setsidSupported = false;
        } catch (Error err) {
            if (err.getMessage() != null
                    && err.getMessage().contains("posix_spawn is not " +
                    "a supported process launch mechanism")
                    && (Shell.FREEBSD || Shell.MAC)) {
                // HADOOP-11924: This is a workaround to avoid failure of class init
                // by JDK issue on TR locale(JDK-8047340).
                LOG.info("Avoiding JDK-8047340 on BSD-based systems.", err);
                setsidSupported = false;
            }
        }  finally { // handle the exit code
            if (LOG.isDebugEnabled()) {
                LOG.debug("setsid exited with exit code "
                        + (shexec != null ? shexec.getExitCode() : "(null executor)"));
            }
        }
        return setsidSupported;
    }

    public static final String TOKEN_SEPARATOR_REGEX
            = WINDOWS ? "[|\n\r]" : "[ \t\n\r\f]";

    private long interval;   // refresh interval in msec
    private long lastTime;   // last time the command was performed
    private final boolean redirectErrorStream; // merge stdout and stderr
    private Map<String, String> environment; // env for the command execution
    private File dir;
    private Process process; // sub process used to execute the command
    private int exitCode;
    private Thread waitingThread;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    protected Shell() {
        this(0L);
    }

    protected Shell(long interval) {
        this(interval, false);
    }

    protected Shell(long interval, boolean redirectErrorStream) {
        this.interval = interval;
        this.lastTime = (interval < 0) ? 0 : -interval;
        this.redirectErrorStream = redirectErrorStream;
    }

    protected void setEnvironment(Map<String, String> env) {
        this.environment = env;
    }

    protected void setWorkingDirectory(File dir) {
        this.dir = dir;
    }

    private static final long NANOSECONDS_PER_MILLISECOND = 1000000;

    public static long monotonicNow() {
        return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
    }

    protected void run() throws IOException {
        if (lastTime + interval > monotonicNow()) {
            return;
        }
        exitCode = 0; // reset for next run
        if (Shell.MAC) {
            System.setProperty("jdk.lang.Process.launchMechanism", "POSIX_SPAWN");
        }
        runCommand();
    }

    private void runCommand() throws IOException {
        ProcessBuilder builder = new ProcessBuilder(getExecString());
        Timer timeOutTimer = null;
        ShellTimeoutTimerTask timeoutTimerTask = null;
        timedOut.set(false);
        completed.set(false);

        // Remove all env vars from the Builder to prevent leaking of env vars from
        // the parent process.
        if (!inheritParentEnv) {
            builder.environment().clear();
        }

        if (environment != null) {
            builder.environment().putAll(this.environment);
        }

        if (dir != null) {
            builder.directory(this.dir);
        }

        builder.redirectErrorStream(redirectErrorStream);

        if (Shell.WINDOWS) {
            synchronized (WindowsProcessLaunchLock) {
                // To workaround the race condition issue with child processes
                // inheriting unintended handles during process launch that can
                // lead to hangs on reading output and error streams, we
                // serialize process creation. More info available at:
                // http://support.microsoft.com/kb/315939
                process = builder.start();
            }
        } else {
            process = builder.start();
        }

        waitingThread = Thread.currentThread();
        CHILD_SHELLS.put(this, null);

        if (timeOutInterval > 0) {
            timeOutTimer = new Timer("Shell command timeout");
            timeoutTimerTask = new ShellTimeoutTimerTask(
                    this);
            //One time scheduling.
            timeOutTimer.schedule(timeoutTimerTask, timeOutInterval);
        }
        final BufferedReader errReader =
                new BufferedReader(new InputStreamReader(
                        process.getErrorStream(), Charset.defaultCharset()));
        BufferedReader inReader =
                new BufferedReader(new InputStreamReader(
                        process.getInputStream(), Charset.defaultCharset()));
        final StringBuffer errMsg = new StringBuffer();

        // read error and input streams as this would free up the buffers
        // free the error stream buffer
        Thread errThread = new Thread() {
            @Override
            public void run() {
                try {
                    String line = errReader.readLine();
                    while((line != null) && !isInterrupted()) {
                        errMsg.append(line)
                                .append(System.getProperty("line.separator"));
                        line = errReader.readLine();
                    }
                } catch(IOException ioe) {
                    // Its normal to observe a "Stream closed" I/O error on
                    // command timeouts destroying the underlying process
                    // so only log a WARN if the command didn't time out
                    if (!isTimedOut()) {
                        LOG.warn("Error reading the error stream", ioe);
                    } else {
                        LOG.debug("Error reading the error stream due to shell "
                                + "command timeout", ioe);
                    }
                }
            }
        };
        try {
            errThread.start();
        } catch (IllegalStateException ise) {
        } catch (OutOfMemoryError oe) {
            LOG.error("Caught " + oe + ". One possible reason is that ulimit"
                    + " setting of 'max user processes' is too low. If so, do"
                    + " 'ulimit -u <largerNum>' and try again.");
            throw oe;
        }
        try {
            parseExecResult(inReader); // parse the output
            // clear the input stream buffer
            String line = inReader.readLine();
            while(line != null) {
                line = inReader.readLine();
            }
            // wait for the process to finish and check the exit code
            exitCode  = process.waitFor();
            // make sure that the error thread exits
            joinThread(errThread);
            completed.set(true);
            //the timeout thread handling
            //taken care in finally block
            if (exitCode != 0) {
                throw new ExitCodeException(exitCode, errMsg.toString());
            }
        } catch (InterruptedException ie) {
            InterruptedIOException iie = new InterruptedIOException(ie.toString());
            iie.initCause(ie);
            throw iie;
        } finally {
            if (timeOutTimer != null) {
                timeOutTimer.cancel();
            }
            // close the input stream
            try {
                inReader.close();
            } catch (IOException ioe) {
                LOG.warn("Error while closing the input stream", ioe);
            }
            if (!completed.get()) {
                errThread.interrupt();
                joinThread(errThread);
            }
            try {
                errReader.close();
            } catch (IOException ioe) {
                LOG.warn("Error while closing the error stream", ioe);
            }
            process.destroy();
            waitingThread = null;
            CHILD_SHELLS.remove(this);
            lastTime = monotonicNow();
        }
    }

    private static void joinThread(Thread t) {
        while (t.isAlive()) {
            try {
                t.join();
            } catch (InterruptedException ie) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Interrupted while joining on: " + t, ie);
                }
                t.interrupt(); // propagate interrupt
            }
        }
    }

    /** return an array containing the command name and its parameters. */
    protected abstract String[] getExecString();

    /** Parse the execution result */
    protected abstract void parseExecResult(BufferedReader lines)
            throws IOException;


    public String getEnvironment(String env) {
        return environment.get(env);
    }

    /** get the current sub-process executing the given command.
     * @return process executing the command
     */
    public Process getProcess() {
        return process;
    }

    /** get the exit code.
     * @return the exit code of the process
     */
    public int getExitCode() {
        return exitCode;
    }

    public Thread getWaitingThread() {
        return waitingThread;
    }

    public static class ExitCodeException extends IOException {
        private final int exitCode;

        public ExitCodeException(int exitCode, String message) {
            super(message);
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }

        @Override
        public String toString() {
            final StringBuilder sb =
                    new StringBuilder("ExitCodeException ");
            sb.append("exitCode=").append(exitCode)
                    .append(": ")
                    .append(super.getMessage());
            return sb.toString();
        }
    }


    public interface CommandExecutor {

        void execute() throws IOException;

        int getExitCode() throws IOException;

        String getOutput() throws IOException;

        void close();

    }

    public static class ShellCommandExecutor extends Shell
            implements CommandExecutor {

        private String[] command;
        private StringBuffer output;


        public ShellCommandExecutor(String[] execString) {
            this(execString, null);
        }

        public ShellCommandExecutor(String[] execString, File dir) {
            this(execString, dir, null);
        }

        public ShellCommandExecutor(String[] execString, File dir,
                                    Map<String, String> env) {
            this(execString, dir, env , 0L);
        }

        public ShellCommandExecutor(String[] execString, File dir,
                                    Map<String, String> env, long timeout) {
            this(execString, dir, env , timeout, true);
        }

        /**
         * Create a new instance of the ShellCommandExecutor to execute a command.
         *
         * @param execString The command to execute with arguments
         * @param dir If not-null, specifies the directory which should be set
         *            as the current working directory for the command.
         *            If null, the current working directory is not modified.
         * @param env If not-null, environment of the command will include the
         *            key-value pairs specified in the map. If null, the current
         *            environment is not modified.
         * @param timeout Specifies the time in milliseconds, after which the
         *                command will be killed and the status marked as timed-out.
         *                If 0, the command will not be timed out.
         * @param inheritParentEnv Indicates if the process should inherit the env
         *                         vars from the parent process or not.
         */
        public ShellCommandExecutor(String[] execString, File dir,
                                    Map<String, String> env, long timeout, boolean inheritParentEnv) {
            command = execString.clone();
            if (dir != null) {
                setWorkingDirectory(dir);
            }
            if (env != null) {
                setEnvironment(env);
            }
            timeOutInterval = timeout;
            this.inheritParentEnv = inheritParentEnv;
        }

        /**
         * Returns the timeout value set for the executor's sub-commands.
         * @return The timeout value in milliseconds
         */
        public long getTimeoutInterval() {
            return timeOutInterval;
        }

        /**
         * Execute the shell command.
         * @throws IOException if the command fails, or if the command is
         * not well constructed.
         */
        public void execute() throws IOException {
            for (String s : command) {
                if (s == null) {
                    throw new IOException("(null) entry in command string: "
                            + join(" ", command));
                }
            }
            this.run();
        }

        @Override
        public String[] getExecString() {
            return command;
        }

        @Override
        protected void parseExecResult(BufferedReader lines) throws IOException {
            output = new StringBuffer();
            char[] buf = new char[512];
            int nRead;
            while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
                output.append(buf, 0, nRead);
            }
        }

        /** Get the output of the shell command. */
        public String getOutput() {
            return (output == null) ? "" : output.toString();
        }

        /**
         * Returns the commands of this instance.
         * Arguments with spaces in are presented with quotes round; other
         * arguments are presented raw
         *
         * @return a string representation of the object.
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            String[] args = getExecString();
            for (String s : args) {
                if (s.indexOf(' ') >= 0) {
                    builder.append('"').append(s).append('"');
                } else {
                    builder.append(s);
                }
                builder.append(' ');
            }
            return builder.toString();
        }

        @Override
        public void close() {
        }
    }

    public boolean isTimedOut() {
        return timedOut.get();
    }


    private void setTimedOut() {
        this.timedOut.set(true);
    }

    public static String execCommand(Map<String, String> env, String[] cmd,
                                     long timeout) throws IOException {
        ShellCommandExecutor exec = new ShellCommandExecutor(cmd, null, env,
                timeout);
        exec.execute();
        return exec.getOutput();
    }


    private static class ShellTimeoutTimerTask extends TimerTask {

        private final Shell shell;

        public ShellTimeoutTimerTask(Shell shell) {
            this.shell = shell;
        }

        @Override
        public void run() {
            Process p = shell.getProcess();
            try {
                p.exitValue();
            } catch (Exception e) {
                //Process has not terminated.
                //So check if it has completed
                //if not just destroy it.
                if (p != null && !shell.completed.get()) {
                    shell.setTimedOut();
                    p.destroy();
                }
            }
        }
    }
}

