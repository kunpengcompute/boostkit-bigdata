# 常见问题<a name="ZH-CN_TOPIC_0000002520041768"></a>

## 编译TPC-DS时证书引起报错的解决方法<a name="ZH-CN_TOPIC_0000002520361774"></a>

**问题现象描述<a name="zh-cn_topic_0000001454201442_section758133012554"></a>**

编译TPC-DS时，出现以下报错。

```
curl failed to verify the legitimacy of the server and therefore could not establish a secure connection to it.
```

**关键过程、根本原因分析<a name="zh-cn_topic_0000001454201442_section145813300553"></a>**

服务器的SSL证书无法被curl信任。

**结论、解决方案及效果<a name="zh-cn_topic_0000001454201442_section93441811202317"></a>**

需要在**curl**命令后面添加参数-k，跳过SSL证书验证，从而避免因证书问题导致的连接失败。

1. 打开“/opt/hive-testbench-hdp3/tpcds-gen/Makefile“文件。

    ```
    vi /opt/hive-testbench-hdp3/tpcds-gen/Makefile
    ```

2. 按“i“进入编辑模式，在**curl**后面加上**-k**。
3. 按“Esc“键，输入**:wq!**，按“Enter“保存并退出编辑。


## 安装Pydoop时提示找不到types.h的解决方法<a name="ZH-CN_TOPIC_0000002520041770"></a>

**问题现象描述<a name="zh-cn_topic_0000001503521325_section758133012554"></a>**

安装Pydoop时提示找不到types.h。

**关键过程、根本原因分析<a name="zh-cn_topic_0000001503521325_section145813300553"></a>**

pip安装时未指定tirpc的目录。

**结论、解决方案及效果<a name="zh-cn_topic_0000001503521325_section93441811202317"></a>**

1. 下载libtirpc相关依赖包。

    ```
    yum install -y libtirpc*+
    ```

2. <a name="zh-cn_topic_0000001503521325_li103561205329"></a>找到types.h头文件的绝对路径。

    ```
    find /usr -name "types.h" | grep rpc
    ```

3. pip安装的时候指定tirpc的目录，确保types.h可以被访问到。

    ```
    pip install --global-option=build_ext --global-option="-I/usr/include/tirpc" pydoop
    ```

    >![](public_sys-resources/icon-note.gif) **说明：** 
    >如果[2](#zh-cn_topic_0000001503521325_li103561205329)中找到的绝对路径不是“/usr/include/tirpc“，需要将上述命令中“/usr/include/tirpc“的修改为[2](#zh-cn_topic_0000001503521325_li103561205329)找到的绝对路径。


## 安装GLPK失败的解决方法<a name="ZH-CN_TOPIC_0000002520361780"></a>

**问题现象描述<a name="zh-cn_topic_0000001503521333_section758133012554"></a>**

安装GLPK失败。

**关键过程、根本原因分析<a name="zh-cn_topic_0000001503521333_section145813300553"></a>**

下载的安装包不正确或安装方式不正确。

**结论、解决方案及效果<a name="zh-cn_topic_0000001503521333_section93441811202317"></a>**

采用源码安装。

1. 下载GLPK安装包。

    ```
    wget https://ftp.gnu.org/gnu/glpk/glpk-5.0.tar.gz
    ```

2. 解压安装包。

    ```
    tar zxvf glpk-5.0.tar.gz
    ```

3. 进入“glpk-5.0“目录并执行配置脚本。

    ```
    cd glpk-5.0
    ./configure
    ```

4. 编译并安装。

    ```
    make
    make install
    ```


## 安装z3-solver失败的解决方法<a name="ZH-CN_TOPIC_0000002551441771"></a>

**问题现象描述<a name="zh-cn_topic_0000001503361605_section758133012554"></a>**

z3-solver安装失败。

**关键过程、根本原因分析<a name="zh-cn_topic_0000001503361605_section145813300553"></a>**

很大程度上是由于GCC版本和CMake版本太旧导致，要求CMake 3.4以上版本，因此我们需要升级GCC版本和CMake版本。

**结论、解决方案及效果<a name="zh-cn_topic_0000001503361605_section93441811202317"></a>**

此处以升级到GCC 7.3.0和CMake 3.13.4为例。

1. 升级GCC。
    1. 参考[GCC升级](https://www.hikunpeng.com/document/detail/zh/kunpengcpfs/ecosystemEnable/OpenStack/kunpengopenstack_05_0017.html)，完成升级。
    2. 升级完成后，新增环境变量。

        ```
        export LD_LIBRARY_PATH=/usr/local/mpc-1.0.3/lib:/usr/local/gmp-6.1.2/lib:/usr/local/mpfr-3.1.5/lib:/usr/local/gcc-7.3.0/lib64:/usr/local/isl-0.18/lib:/usr/local/lib:/usr/lib:$LD_LIBRARY_PATH
        export PATH=/usr/local/gcc-7.3.0/bin:$PATH
        ```

2. 升级CMake。
    1. 下载CMake安装包。

        ```
        wget https://github.com/Kitware/CMake/archive/refs/tags/v3.13.4.tar.gz --no-check-certificate
        ```

    2. 解压后进入目录，执行如下命令。

        ```
        ./bootstrap
        gmake
        gmake install
        ```

3. GCC和CMake都升级后，重新安装z3-solver。

    ```
    pip3 install z3-solver==4.8.15.0
    ```


