/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef URI_INFO_H
#define URI_INFO_H

/// \brief A parsed URI
class  UriInfo {
public:
    UriInfo(std::string uriStr, std::string schemeStr, std::string pathStr, std::string hostStr, std::string portStr);

    UriInfo(std::string schemeStr, std::string pathStr, std::string hostStr, std::string portStr);

    ~UriInfo();

    const std::string& Scheme() const;

    /// The URI host name, such as "localhost", "127.0.0.1" or "::1", or the empty
    /// string is the URI does not have a host component.
    const std::string& Host() const;

    /// The URI path component.
    const std::string& Path() const;

    /// The URI port number, as a string such as "80", or the empty string is the URI
    /// does not have a port number component.
    const std::string& Port() const;

    /// Get the string representation of this URI.
    const std::string &ToString() const;

private:
    std::string hostStr_;
    std::string schemeStr_;
    std::string portStr_;
    std::string pathStr_;
    std::string uriStr_;
};

#endif