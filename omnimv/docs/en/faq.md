# FAQs<a name="EN-US_TOPIC_0000002520041768"></a>

## Error Reported During TPC-DS Compilation<a name="EN-US_TOPIC_0000002520361774"></a>

**Symptom<a name="en-us_topic_0000001454201442_section758133012554"></a>**

The following error message is displayed during TPC-DS compilation:

```
curl failed to verify the legitimacy of the server and therefore could not establish a secure connection to it.
```

**Key Process and Cause Analysis<a name="en-us_topic_0000001454201442_section145813300553"></a>**

The server SSL certificate is not trusted by curl.

**Conclusion and Solution<a name="en-us_topic_0000001454201442_section93441811202317"></a>**

Add the  **-k**  parameter to the end of the  **curl**  command to skip SSL certificate verification, preventing connection failures due to certificate issues.

1. Open  **/opt/hive-testbench-hdp3/tpcds-gen/Makefile**.

    ```
    vi /opt/hive-testbench-hdp3/tpcds-gen/Makefile
    ```

2. Press  **i**  to enter the insert mode and add  **-k**  next to  **curl**.
3. Press  **Esc**, type  **:wq!**, and press  **Enter**  to save the file and exit.


## types.h Not Found When Pydoop Is Installed<a name="EN-US_TOPIC_0000002520041770"></a>

**Symptom<a name="en-us_topic_0000001503521325_section758133012554"></a>**

The  **types.h**  file is not found when installing Pydoop.

**Key Process and Cause Analysis<a name="en-us_topic_0000001503521325_section145813300553"></a>**

The  **tirpc**  directory is not specified during pip installation.

**Conclusion and Solution<a name="en-us_topic_0000001503521325_section93441811202317"></a>**

1. Download libtirpc dependencies.

    ```
    yum install -y libtirpc*+
    ```

2. <a id="step_2"></a>Find the absolute path of the  **types.h**  header file.

    ```
    find /usr -name "types.h" | grep rpc
    ```

3. Specify the  **tirpc**  directory during pip installation to ensure that  **types.h**  can be accessed.

    ```
    pip install --global-option=build_ext --global-option="-I/usr/include/tirpc" pydoop
    ```

    >![](public_sys-resources/icon-note.gif) **NOTE:** 
    >If the absolute path found in [Step 2](#step_2) is not  **/usr/include/tirpc**, change  **/usr/include/tirpc**  in the preceding command to the absolute path found in [Step 2](#step_2).


## Failed to Install GLPK<a name="EN-US_TOPIC_0000002520361780"></a>

**Symptom<a name="en-us_topic_0000001503521333_section758133012554"></a>**

The GLPK installation failed.

**Key Process and Cause Analysis<a name="en-us_topic_0000001503521333_section145813300553"></a>**

The downloaded installation package is incorrect or the installation method is incorrect.

**Conclusion and Solution<a name="en-us_topic_0000001503521333_section93441811202317"></a>**

Use the source code for installation.

1. Download the GLPK installation package.

    ```
    wget https://ftp.gnu.org/gnu/glpk/glpk-5.0.tar.gz
    ```

2. Decompress the installation package.

    ```
    tar zxvf glpk-5.0.tar.gz
    ```

3. Go to the  **glpk-5.0**  directory and run the configuration script.

    ```
    cd glpk-5.0
    ./configure
    ```

4. Perform compilation and installation.

    ```
    make
    make install
    ```


## Failed to Install z3-solver<a name="EN-US_TOPIC_0000002551441771"></a>

**Symptom<a name="en-us_topic_0000001503361605_section758133012554"></a>**

The z3-solver installation failed.

**Key Process and Cause Analysis<a name="en-us_topic_0000001503361605_section145813300553"></a>**

To a large extent, the GCC version and CMake version are too early \(CMake 3.4 or later is required\). Therefore, you need to upgrade GCC and CMake.

**Conclusion and Solution<a name="en-us_topic_0000001503361605_section93441811202317"></a>**

The following describes how to upgrade to GCC 7.3.0 and CMake 3.13.4.

1. Upgrade GCC.
    1. Upgrade the GCC by following instructions in  [Upgrading the GCC](https://www.hikunpeng.com/document/detail/en/kunpengcpfs/ecosystemEnable/OpenStack/kunpengopenstacktrain_04_001.html).
    2. After the upgrade is complete, add environment variables.

        ```
        export LD_LIBRARY_PATH=/usr/local/mpc-1.0.3/lib:/usr/local/gmp-6.1.2/lib:/usr/local/mpfr-3.1.5/lib:/usr/local/gcc-7.3.0/lib64:/usr/local/isl-0.18/lib:/usr/local/lib:/usr/lib:$LD_LIBRARY_PATH
        export PATH=/usr/local/gcc-7.3.0/bin:$PATH
        ```

2. Upgrade CMake.
    1. Download the CMake installation package.

        ```
        wget https://github.com/Kitware/CMake/archive/refs/tags/v3.13.4.tar.gz --no-check-certificate
        ```

    2. Decompress the package, go to the directory, and run the following commands:

        ```
        ./bootstrap
        gmake
        gmake install
        ```

3. After both GCC and CMake are upgraded, reinstall z3-solver.

    ```
    pip3 install z3-solver==4.8.15.0
    ```


