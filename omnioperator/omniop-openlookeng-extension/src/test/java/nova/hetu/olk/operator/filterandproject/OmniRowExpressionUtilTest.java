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

package nova.hetu.olk.operator.filterandproject;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.CharType;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static nova.hetu.olk.operator.filterandproject.OmniRowExpressionUtil.Format.JSON;
import static nova.hetu.olk.operator.filterandproject.OmniRowExpressionUtil.Format.STRING;

public class OmniRowExpressionUtilTest
        extends PowerMockTestCase
{
    @DataProvider(name = "expressionProvider")
    public Object[][] expressionProvider()
    {
        return new Object[][]{
                {
                        new CallExpression("$operator$.id", () -> new CatalogSchemaName("123", "123"), CharType.createCharType(10), new ArrayList<>())
                },
                {
                        new InputReferenceExpression(1, CharType.createCharType(10))
                },
                {
                        new LambdaDefinitionExpression(new ArrayList<>(), new ArrayList<>(), new InputReferenceExpression(1, CharType.createCharType(10)))
                },
                {
                        new SpecialForm(SpecialForm.Form.IF, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.NULL_IF, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.SWITCH, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.WHEN, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.BETWEEN, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.IS_NULL, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.COALESCE, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.IN, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.AND, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.OR, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.DEREFERENCE, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.ROW_CONSTRUCTOR, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.BIND, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new SpecialForm(SpecialForm.Form.BETWEEN_AND, CharType.createCharType(10), Arrays.asList(new InputReferenceExpression(1, CharType.createCharType(10)), new InputReferenceExpression(1, CharType.createCharType(10))))
                },
                {
                        new VariableReferenceExpression("field", CharType.createCharType(10))
                }
        };
    }

    @Test(dataProvider = "expressionProvider")
    public void testExpressionStringify(RowExpression rowExpression)
    {
        OmniRowExpressionUtil.expressionStringify(rowExpression, JSON);
        OmniRowExpressionUtil.expressionStringify(rowExpression, STRING);
    }
}
