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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

class SymbolAliasUtil
{
    private SymbolAliasUtil() {}

    static boolean aliasSetMatches(Set<SymbolAlias> aliasSet, Set<Symbol> symbolSet, SymbolAliases symbolAliases)
    {
        Set<Symbol> expectedSymbolSet = aliasSet.stream()
                .map(resolver(symbolAliases))
                .collect(toImmutableSet());
        return symbolSet.equals(expectedSymbolSet);
    }

    static boolean aliasSetListMatches(List<Set<SymbolAlias>> aliasSetList, List<Set<Symbol>> symbolSetList, SymbolAliases symbolAliases)
    {
        if (aliasSetList.size() != symbolSetList.size()) {
            return false;
        }
        for (int i = 0; i < aliasSetList.size(); ++i) {
            if (!aliasSetMatches(aliasSetList.get(i), symbolSetList.get(i), symbolAliases)) {
                return false;
            }
        }
        return true;
    }

    static boolean aliasAliasMapMatches(Map<SymbolAlias, SymbolAlias> aliasAliasMap, Map<Symbol, Symbol> symbolSymbolMap, SymbolAliases symbolAliases)
    {
        Map<Symbol, Symbol> expectedSymbolSymbolMap = aliasAliasMap.entrySet().stream()
                .collect(
                        toImmutableMap(
                                entry -> entry.getKey().toSymbol(symbolAliases),
                                entry -> entry.getValue().toSymbol(symbolAliases)));

        return symbolSymbolMap.equals(expectedSymbolSymbolMap);
    }

    static Function<SymbolAlias, Symbol> resolver(SymbolAliases symbolAliases)
    {
        return alias -> alias.toSymbol(symbolAliases);
    }
}
