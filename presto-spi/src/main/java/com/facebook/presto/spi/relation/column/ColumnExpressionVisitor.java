/*
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
package com.facebook.presto.spi.relation.column;

public interface ColumnExpressionVisitor<T, R, C>
{
    R visitCall(Call<T> call, C context);

    R visitInputReference(InputReference<T> reference, C context);

    R visitConstant(Constant<T> literal, C context);

    R visitLambda(LambdaDefinition<T> lambda, C context);
}
