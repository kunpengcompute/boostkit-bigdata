# Introduction to OmniShield<a name="EN-US_TOPIC_0000002551634289"></a>

## What's New<a name="EN-US_TOPIC_0000002520474308"></a>

- \[2024-12-30\]: Released OmniShield 1.1.0. Added remote attestation support for OmniShield.
- \[2024-09-30\]: Released OmniShield 1.0.0. OmniShield is a plugin of the big data engine Spark and provides the data source encryption and decryption functions. It protects data in use by executing the computing process in the hardware-based trusted execution environment \(TEE\). OmniShield ensures data security throughout the entire computing link. It enables AES encryption and decryption for reading and writing DataFrame CSV/JSON/text row-based data sources. It provides encryption and decryption functions for the Spark SQL data source, and encryption and decryption functions with SM algorithms for reading and writing the Spark SQL ORC data source.


## Introduction to the Project<a name="EN-US_TOPIC_0000002551514287"></a>

### Overview<a name="EN-US_TOPIC_0000002520634294"></a>

The big data features of OmniRuntime are presented in the form of plugins to improve the performance of data loading, computing, and exchange from end to end.

Data volumes generated from Internet services have been growing much faster than CPUs' computing power. The open-source big data ecosystem is also developing on a fast track. However, diversified computing engines and open source components make it difficult to improve data processing performance throughout the lifecycle. Different big data engines use their own unique tuning policies and technologies to improve performance and efficiency. Some tuning items may be applied across multiple engines, which may cause resource contention and conflicts, reducing overall computing performance.

OmniRuntime  consists of a series of features provided by Kunpeng BoostKit for Big Data in terms of application acceleration. It aims to improve the performance of end-to-end data loading, computing, and exchange through plugins, thereby improving the performance of big data analytics.

As a subfeature of OmniRuntime, OmniShield is a confidential computing component of the Spark big data engine. It runs in the TEE of the customer's data center to encrypt and decrypt data by executing the computing process in the hardware-based TEE. With OmniShield, data security in the REE is also safeguarded. In confidential computing scenarios, OmniShield provides data source encryption and decryption capabilities for DataFrame and SparkSQL applications, and also end-to-end security protection for Spark applications based on the Arm confidential computing trusted execution environment \(TEE\) kit.

Compatible open source components and versions:

- Spark 3.3.1
- Hive 3.1.0


### Architecture<a name="EN-US_TOPIC_0000002520634296"></a>

OmniShield is a confidential computing component for the Spark big data engine. It runs in the TEE of the customer's data center to encrypt and decrypt data by executing the computing process in the hardware-based TEE. With OmniShield, data security in the REE is also safeguarded.

OmniShield can work as a plugin of Spark to encrypt and decrypt CSV, JSON, and TXT row-based data sources in DataFrame and Spark SQL scenarios. You can modify the ORC code to encrypt ORC column-based data sources in the Spark SQL scenario. Also, you can modify the Spark encryption and decryption code to enable the SM4 algorithm for drive and network I/O encryption and decryption. In addition, you can modify the Spark Executor code to implement remote attestation for Spark in Yarn resource management mode.

[Figure 1](#en-us_topic_0000002512402638_fig26911981620)  shows the OmniShield architecture.

**Figure  1**  Software architecture of OmniShield<a id="en-us_topic_0000002512402638_fig26911981620"></a>  
![](figures/software-architecture-of-omnishield.png "software-architecture-of-omnishield")

OmniShield performs the following functions:

- Encrypts and decrypts CSV, JSON, and TXT row-based data sources in the DataFrame scenario. The used encryption algorithm is AES/GCM/NOPadding. APIs are provided for mainstream key management servers \(KMSs\) such as Hadoop to obtain keys.
- Encrypts and decrypts CSV, JSON, and TXT row-based data sources in the Spark SQL scenario. The used encryption algorithm is AES/GCM/NOPadding or SM4/GCM/NOPadding. APIs are provided for mainstream KMSs such as Hadoop to obtain keys.
- Encrypts and decrypts ORC column-based data sources in the Spark SQL scenario. The used encryption algorithm is SM4/GCM/NOPadding.
- Encrypts and decrypts Spark shuffle drive and network I/Os. The used encryption algorithm is SM4/GCM/NOPadding.
- Provides Spark on Yarn application-level remote attestation based on the Integrity Measurement Architecture \(IMA\) of the openEuler OS. An IMA-based measurement report can be generated and remote attestation can be initiated when starting the Spark Executor.

OmniShield is a confidential computing component for the Spark big data engine. It runs in the TEE of the customer's data center to encrypt and decrypt data by executing the computing process in the hardware-based TEE. With OmniShield, data security in the REE is also safeguarded.
### Application Scenarios<a name="EN-US_TOPIC_0000002551514295"></a>

Understand the OmniShield application scenarios before using the feature. OmniShield can be used on Spark 3.3.1.

OmniShield can process the intersection of privacy sets in secure multi-party computation. It ensures that no additional element information is disclosed except the intersection of privacy sets. When a user submits a Spark job, the Executor executes remote attestation on the application to check whether the application and operating environment are trusted. OmniShield encrypts and decrypts the data sources using specified parameters, and enables the SM4/GCM/NOPadding algorithm for drive and network I/O encryption and decryption to ensure security and privacy.

Understand the OmniShield application scenarios before using the feature. OmniShield can be used on Spark 3.3.1.
### Related Concepts<a name="EN-US_TOPIC_0000002551634291"></a>

- Rich execution environment \(REE\): A standard operating system that a device is running. Compared with a TEE, an REE is a traditional server running environment.
- Trusted execution environment \(TEE\): A hardware security feature that is isolated from a normal operating system, for example, an REE.



## Constraints<a name="EN-US_TOPIC_0000002520474310"></a>

Understand the OmniShield usage restrictions before configuring the feature.

- Only 128-bit or 256-bit keys of the AES/GCM/NOPadding algorithm are supported.
- Only 128-bit keys of the SM4/GCM/NOPadding algorithm are supported.
- OmniShield does not provide the KMS service or specify the KMS to be used. Determine what KMS to use by yourself.
- OmniShield does not provide the implementation of the Verifier and Attester in remote attestation. Design the interconnection by yourself.
- When using the "Encrypting and Decrypting Spark SQL ORC Data Sources Using SM Algorithms" feature of OmniShield, disable the ORC predicate pushdown function of Spark and set the Spark parameter  **spark.sql.orc.filterPushdown**  to  **false**.

Understand the OmniShield usage restrictions before configuring the feature.
## Directory Structure<a name="EN-US_TOPIC_0000002551514291"></a>

The full project directory structure is as follows:

```
├── docs                                                      # Project document directory
│   └── en                                                   # English document directory
│       ├── figures                                          # Directory of images in Chinese documents
│       ├── quick_start.md                                   # Quick Start
│       ├── release_notes.md                                 # OmniShield Release Notes
│       ├── installation_guide.md                            # OmniShield Installation Guide
│       ├── user_guide.md                                    # OmniShield User Guide
├── script                                                   # Script directory
│   └── build.sh                                             # Compilation script
├── src                                                      # Source code directory
│   ├── main                                                 # Feature source code directory
│   └── test                                                 # Unit test directory
```


## Release Notes<a name="EN-US_TOPIC_0000002520474302"></a>

For details about feature changes in each version, see [Release Notes](./docs/en/release_notes.md).


## Environment Deployment<a name="EN-US_TOPIC_0000002520634288"></a>

For details about the environment dependencies and installation methods of OmniShield, see [Installation Guide](./docs/en/installation_guide.md).


## Quick Start<a name="EN-US_TOPIC_0000002520634290"></a>

For instructions on quickly verifying whether OmniShield is active and its performance improvements, see [Quick Start](./docs/en/quick_start.md).


## Helpful Links<a name="EN-US_TOPIC_0000002551514289"></a>

|Name|Overview|
|--|--|
|[Quick Start](./docs/en/quick_start.md)|Provides guidance on how to quickly enable and verify the OmniShield feature.|
|[Release Notes](./docs/en/release_notes.md)|Provides basic information and feature updates of each OmniShield version.|
|[Installation Guide](./docs/en/installation_guide.md)|Describes how to install OmniShield.|
|[User Guide](./docs/en/user_guide.md)|Provides details about how to use OmniShield.|



## Security Statement<a name="EN-US_TOPIC_0000002551517483"></a>

### Routine Antivirus Software Check<a name="EN-US_TOPIC_0000002520477492"></a>

Periodically scan clusters and Spark components for viruses. This protects clusters from viruses, malicious code, spyware, and malicious programs, reducing risks such as system breakdown and information leakage. Mainstream antivirus software can be recommended for antivirus check.


### Log Control<a name="EN-US_TOPIC_0000002520637490"></a>

- Check whether the system can limit the size of a single log file.
- Check whether there is a mechanism for clearing logs when the log space is used up.


### Setting the Login Session Timeout<a name="EN-US_TOPIC_0000002520522492"></a>

A login session should time out if it is inactive for 30 minutes or shorter. Perform the following steps:

1. Log in to the node where Ascend-mindxdl-mindio is installed.
2. Open the  **/etc/profile**  file.

    ```
    vim /etc/profile
    ```

3. Press  **i**  to enter the insert mode and add the following content to the end of the file:

    ```
    export TMOUT=1800
    readonly TMOUT
    ```

4. Press  **Esc**, type  **:wq!**, and press  **Enter**  to save the file and exit.


### Disabling the History Command<a name="EN-US_TOPIC_0000002520682472"></a>

To prevent the history command from being executed to view historical operations, you need to disable the history command. Perform the following steps:

1. Log in to the node where Ascend-mindxdl-mindio is installed.
2. Open the  **/etc/profile**  file.

    ```
    vim /etc/profile
    ```

3. Press  **i**  to enter the insert mode and add the following content to the end of the file:

    ```
    export HISTFILESIZE=0
    readonly HISTFILESIZE
    ```

4. Press  **Esc**, type  **:wq!**, and press  **Enter**  to save the file and exit.


### Setting the umask Value<a name="EN-US_TOPIC_0000002551682471"></a>

You are advised to set the umask of the server to a value ranging from 027 to 777 to modify the file permission. For example, to set  **umask**  to  **027**, perform the following operations:

1. Log in to the server as the root user and open the  **/etc/profile**  file.

    ```
    vim /etc/profile
    ```

2. Press  **i**  to enter the insert mode and add  **umask 027**  to the end of the file:
3. Press  **Esc**, type  **:wq!**, and press  **Enter**  to save the file and exit.
4. Make the setting take effect.

    ```
    source /etc/profile
    ```


### Buffer Overflow Prevention<a name="EN-US_TOPIC_0000002551722491"></a>

To prevent buffer overflow attacks, you are advised to use the address space layout randomization \(ASLR\) technology to randomize the layout of linear areas such as the heap, stack, and shared library mapping to make it more difficult for attackers to predict target addresses and locate code. This technology can be applied to heaps, stacks, and memory mapping areas \(mmap base addresses, shared libraries, and vDSO pages\).

How to enable this function:

**echo 2 \>/proc/sys/kernel/randomize\_va\_space**


### Vulnerability Fixing<a name="EN-US_TOPIC_0000002551637469"></a>

To ensure the security of the production environment and reduce the risk of attacks, enable the firewall and periodically fix the following vulnerabilities:

- OS vulnerabilities
- JDK vulnerabilities
- Hadoop and Spark vulnerabilities
- ZooKeeper vulnerabilities
- Kerberos vulnerabilities
- OpenSSL vulnerabilities
- Vulnerabilities in other components

    The following uses CVE-2021-37137 as an example.

    Vulnerability description:

    Netty 4.1.17 has two Content-Length HTTP headers that may be confused. The vulnerability ID is CVE-2021-37137.

    The system uses the hdfs-ceph \(version 3.2.0\) service as the storage object with decoupled storage and compute. This service depends on  **aws-java-sdk-bundle-1.11.375.jar**  and involves this vulnerability. You are advised to update the vulnerability patch in a timely manner to prevent hacker attacks.

    Impact:

    Netty 4.1.68 and earlier versions

    Handling suggestion:

    Currently, the vendor has released an upgrade patch to fix the vulnerability. For details, visit  [GitHub](https://github.com/netty/netty/security/advisories/GHSA-9vjp-v76f-g363).


### SSH Hardening<a name="EN-US_TOPIC_0000002551517487"></a>

During the installation and deployment, you need to connect to the server through SSH. The  **root**  user has all the operation permissions. Logging in to the server as the  **root**  user may pose security risks. You are advised to log in to the server as a common user for installation and deployment and disable  **root**  user login using SSH to improve system security. 

Check the  **PermitRootLogin**  configuration item in  **/etc/ssh/sshd\_config**.

- If the value is  no,  **root**  user login using SSH is disabled.
- If the value is  yes, change it to  no.



## Account List<a name="EN-US_TOPIC_0000002520477498"></a>


## Disclaimer<a name="EN-US_TOPIC_0000002520474306"></a>

**To OmniShield users**

- This tool is intended solely for debugging and development. You are responsible for any risks and should carefully review the following information:
    - Data processing and deletion: Users are responsible for managing and deleting any data generated while using this tool. You are advised to promptly delete any related data after use to prevent information leaks.
    - Data confidentiality and transmission: Users understand and agree not to share or transmit any data generated by this tool. Neither the tool nor its developers are responsible for any information leaks, data breaches, or other negative consequences.
    - User input security: Users are responsible for the security of any commands they enter and for any risks or losses resulting from improper input. The tool and its developers are not liable for issues caused by incorrect command usage.

- Disclaimer scope: This disclaimer applies to all individuals and entities using this tool. By using the tool, you acknowledge and accept this statement and assume all risks and responsibilities arising from its use. If you do not agree, please stop using the tool immediately.
- Before using this tool,  **please read and understand the preceding disclaimer**. If you have any questions, contact the developer.

**To data owners**

If you do not want your model or dataset to be mentioned in OmniShield, or if you wish to update its description, please submit an issue on GitCode. We will delete or update your description according to your request. Thank you for your understanding and contribution to OmniShield.


## License<a name="EN-US_TOPIC_0000002551634293"></a>

For details, see the  [License](../LICENSE)  file.

The documents in the  _docs_  directory are licensed under CC BY 4.0. For details, see the  [License](../LICENSE)  file.


## Contribution Statement<a name="EN-US_TOPIC_0000002551634285"></a>

1. Submit an error report: If you discover a vulnerability in OmniShield that is not a security issue, first search the  **Issues**  in the OmniShield repository to avoid submitting duplicates. If the vulnerability is not listed, create a new issue. If you discover a security-related problem, do not disclose it publicly. Please refer to the security handling guidelines for details. All error reports must include complete information about the issue.
2. Security issue handling: For guidance on handling security issues in this project, please contact the core team via email for instructions.
3. Resolving existing issues: Review the repository's issue list to identify issues that need attention, and attempt to resolve them.
4. How to propose new functions: Use the  **Feature**  tag when creating an issue for a new function. We will review and confirm proposals periodically.
5. How to contribute:
    1. Fork the repository of the project.
    2. Clone it to your local machine.
    3. Create a development branch.
    4. Local testing: All unit tests, including any new test cases, must pass before submission.
    5. Commit your code.
    6. Create a pull request \(PR\).
    7. Code review: Modify the code according to review comments and resubmit your changes. This process may involve multiple rounds of iterations.
    8. After your PR is approved by the required number of reviewers, the committer will conduct the final review.
    9. After your PR is approved and all tests pass, the CI system will merge it into the project's main branch.




## Suggestions and Feedback<a name="EN-US_TOPIC_0000002551634283"></a>

You are welcome to contribute to the community. If you have any questions or suggestions, please submit an  [issue](https://gitcode.com/boostkit/boostkit-bigdata/tree/main/omnishield). We will respond as soon as possible. Thank you for your support.


## Acknowledgments<a name="EN-US_TOPIC_0000002551514285"></a>

OmniShield is jointly developed by the following Huawei departments:

Kunpeng Computing BoostKit Development Dept

Thank you to everyone in the community for your PRs. We warmly welcome contributions to OmniShield!


