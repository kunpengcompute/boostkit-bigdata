#!/bin/bash
cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
mvn clean package -Ddep.os.arch="-${cpu_name}"