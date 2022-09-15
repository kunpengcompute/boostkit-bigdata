/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nova.hetu.olk.operator.benchmark;

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;

public class BenchmarkRunner
{
    private BenchmarkRunner()
    {
    }

    public static void main(String[] args) throws Exception
    {
        runBenchmark(args[0]);
    }

    public static void runBenchmark(Class<?> benchmarkClass) throws Exception
    {
        runBenchmark(benchmarkClass.getSimpleName());
    }

    public static void runBenchmark(String benchmarkClassName) throws Exception
    {
        FileUtils.forceMkdir(new File("benchmark-result"));
        String benchmarkName = benchmarkClassName.replaceAll("\\.java", "");
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include("nova.hetu.olk.operator.benchmark." + benchmarkName + ".*")
                .shouldDoGC(true)
                .resultFormat(ResultFormatType.CSV)
                .result(System.getProperty("user.dir") + "/benchmark-result/" + benchmarkName + ".csv")
                .build();
        new Runner(options).run();
    }

    @Test(timeOut = -1)
    @Ignore
    public void benchmarkRes()
    {
        URL url = getClass().getClassLoader().getResource("operator.ini");
        File file = new File(url.getFile());
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            String operator;
            while ((operator = bufferedReader.readLine()) != null) {
                if (operator.contains("#") || operator.contains("//")) {
                    continue;
                }
                runBenchmark(operator);
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}
