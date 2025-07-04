## OmniShield机密大数据
#### 介绍
OmniShield机密大数据特性是一个大数据引擎Spark的机密计算组件，运行在客户数据中心的TEE环境内，通过在基于硬件的TEE环境中执行计算过程对数据进行加解密，保证数据在REE侧也是安全隐私的。OmniShield机密大数据作为Spark的机密计算组件，通过Spark提供的插件机制，实现Dataframe和Spark-SQL场景下对CSV、JSON和TXT行式数据源的加解密功能。通过对ORC代码的修改，实现在Spark-SQL场景下对ORC列式数据源加密的功能。通过对Spark加解密相关代码修改，实现磁盘和网络IO加解密SM4国密算法功能。通过对Spark Executor相关代码修改，实现Spark在Yarn资源管理模式下的远程证明功能。

#### 操作系统
鲲鹏架构下的openEuler系（例如openEuler 22.03 LTS）操作系统。

#### 使用说明
如何使用OmniShield机密大数据安装相关工具，请参见： https://www.hikunpeng.com/document/detail/zh/kunpengboostkithistory/2400/bds/kunpengbds_omniruntime_20_0703.html

#### 参与贡献
    如果您想为本仓库贡献代码，请向本仓库任意maintainer发送邮件
    如果您找到产品中的任何Bug，欢迎您提出ISSUE