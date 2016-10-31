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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.ParametricImplementations;
import com.facebook.presto.operator.annotations.AnnotationHelpers;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationStateSerializerFactory;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.operator.annotations.AnnotationHelpers.validateSignaturesCompatibility;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class AggregationFromAnnotationsParser
{
    private AggregationFromAnnotationsParser()
    {
    }

    // This function should only be used for function matching for testing purposes.
    // General purpose function matching is done through FunctionRegistry.
    @VisibleForTesting
    public static BindableAggregationFunction generateAggregationBindableFunction(Class<?> clazz)
    {
        List<BindableAggregationFunction> aggregations = generateBindableAggregationFunctions(clazz);
        checkArgument(aggregations.size() == 1, "More than one aggregation function found");
        return aggregations.get(0);
    }

    // This function should only be used for function matching for testing purposes.
    // General purpose function matching is done through FunctionRegistry.
    public static BindableAggregationFunction generateAggregationBindableFunction(Class<?> clazz, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        for (BindableAggregationFunction aggregation : generateBindableAggregationFunctions(clazz)) {
            if (aggregation.getSignature().getReturnType().equals(returnType) &&
                    aggregation.getSignature().getArgumentTypes().equals(argumentTypes)) {
                return aggregation;
            }
        }
        throw new IllegalArgumentException(String.format("No method with return type %s and arguments %s", returnType, argumentTypes));
    }

    public static List<BindableAggregationFunction> generateBindableAggregationFunctions(Class<?> aggregationDefinition)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");

        ImmutableList.Builder<BindableAggregationFunction> builder = ImmutableList.builder();

        for (Class<?> stateClass : getStateClasses(aggregationDefinition)) {
            Method combineFunction = getCombineFunction(aggregationDefinition, stateClass);
            Optional<Method> aggregationStateSerializerFactory = getAggregationStateSerializerFactory(aggregationDefinition, stateClass);
            for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateClass)) {
                for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                    for (String name : getNames(outputFunction, aggregationAnnotation)) {
                        AggregationHeader header = new AggregationHeader(
                                name,
                                AggregationImplementation.getDescription(aggregationDefinition, outputFunction),
                                aggregationAnnotation.decomposable());
                        AggregationImplementation onlyImplementation = AggregationImplementation.Parser.parseImplementation(aggregationDefinition, header, stateClass, inputFunction, outputFunction, combineFunction, aggregationStateSerializerFactory);
                        builder.add(
                                new BindableAggregationFunction(
                                        onlyImplementation.getSignature(),
                                        header,
                                        new ParametricImplementations(ImmutableMap.of(),
                                                ImmutableList.of(),
                                                ImmutableList.of(onlyImplementation))));
                    }
                }
            }
        }

        return builder.build();
    }

    public static BindableAggregationFunction generateBindableAggregationFunction(Class<?> aggregationDefinition)
    {
        ImmutableMap.Builder<Signature, AggregationImplementation> exactImplementations = ImmutableMap.builder();
        ImmutableList.Builder<AggregationImplementation> specializedImplementations = ImmutableList.builder();
        ImmutableList.Builder<AggregationImplementation> genericImplementations = ImmutableList.builder();
        Optional<Signature> genericSignature = Optional.empty();
        AggregationHeader header = parseHeader(aggregationDefinition);

        for (Class<?> stateClass : getStateClasses(aggregationDefinition)) {
            Method combineFunction = getCombineFunction(aggregationDefinition, stateClass);
            Optional<Method> aggregationStateSerializerFactory = getAggregationStateSerializerFactory(aggregationDefinition, stateClass);
            for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateClass)) {
                for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                    AggregationImplementation implementation = AggregationImplementation.Parser.parseImplementation(aggregationDefinition, header, stateClass, inputFunction, outputFunction, combineFunction, aggregationStateSerializerFactory);
                    if (implementation.getSignature().getTypeVariableConstraints().isEmpty()
                            && implementation.getSignature().getArgumentTypes().stream().noneMatch(TypeSignature::isCalculated)
                            && !implementation.getSignature().getReturnType().isCalculated()) {
                        exactImplementations.put(implementation.getSignature(), implementation);
                        continue;
                    }

                    if (implementation.hasSpecializedTypeParameters()) {
                        specializedImplementations.add(implementation);
                    }
                    else {
                        genericImplementations.add(implementation);
                    }

                    if (!genericSignature.isPresent()) {
                        genericSignature = Optional.of(implementation.getSignature());
                    }
                    validateSignaturesCompatibility(genericSignature, implementation.getSignature());
                }
            }
        }

        Signature aggregateSignature = genericSignature.orElseGet(() -> getOnlyElement(exactImplementations.build().keySet()));

        return new BindableAggregationFunction(aggregateSignature,
                        header,
                        new ParametricImplementations(
                                exactImplementations.build(),
                                specializedImplementations.build(),
                                genericImplementations.build()));
    }

    private static Optional<Method> getAggregationStateSerializerFactory(Class<?> aggregationDefinition, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> stateSerializerFactories = AnnotationHelpers.findPublicStaticMethodsWithAnnotation(aggregationDefinition, AggregationStateSerializerFactory.class).stream()
                .filter(method -> ((AggregationStateSerializerFactory) method.getAnnotation(AggregationStateSerializerFactory.class)).value().equals(stateClass))
                .collect(toImmutableList());

        if (stateSerializerFactories.isEmpty()) {
            return Optional.empty();
        }

        checkArgument(stateSerializerFactories.size() == 1,
                String.format(
                        "Expect at most 1 @AggregationStateSerializerFactory(%s.class) annotation, found %s in %s",
                        stateClass.toGenericString(),
                        stateSerializerFactories.size(),
                        aggregationDefinition.toGenericString()));
        return Optional.of(getOnlyElement(stateSerializerFactories));
    }

    private static AggregationHeader parseHeader(Class<?> aggregationDefinition)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");
        return new AggregationHeader(aggregationAnnotation.value(), AggregationImplementation.getDescription(aggregationDefinition), aggregationAnnotation.decomposable());
    }

    private static List<String> getNames(@Nullable Method outputFunction, AggregationFunction aggregationAnnotation)
    {
        List<String> defaultNames = ImmutableList.<String>builder().add(aggregationAnnotation.value()).addAll(Arrays.asList(aggregationAnnotation.alias())).build();

        if (outputFunction == null) {
            return defaultNames;
        }

        AggregationFunction annotation = outputFunction.getAnnotation(AggregationFunction.class);
        if (annotation == null) {
            return defaultNames;
        }
        else {
            return ImmutableList.<String>builder().add(annotation.value()).addAll(Arrays.asList(annotation.alias())).build();
        }
    }

    public static Method getCombineFunction(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> combineFunctions = AnnotationHelpers.findPublicStaticMethodsWithAnnotation(clazz, CombineFunction.class).stream()
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method, 0)] == stateClass)
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method, 1)] == stateClass)
                .collect(toImmutableList());

        checkArgument(combineFunctions.size() == 1, String.format("There must be exactly one @CombineFunction in class %s for the @AggregationState %s ", clazz.toGenericString(), stateClass.toGenericString()));
        return getOnlyElement(combineFunctions);
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> outputFunctions = AnnotationHelpers.findPublicStaticMethodsWithAnnotation(clazz, OutputFunction.class).stream()
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method)] == stateClass)
                .collect(toImmutableList());

        checkArgument(!outputFunctions.isEmpty(), "Aggregation has no output functions");
        return outputFunctions;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> inputFunctions = AnnotationHelpers.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class).stream()
                .filter(method -> (method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method)] == stateClass))
                .collect(toImmutableList());

        checkArgument(!inputFunctions.isEmpty(), "Aggregation has no input functions");
        return inputFunctions;
    }

    private static Set<Class<?>> getStateClasses(Class<?> clazz)
    {
        ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();
        for (Method inputFunction : AnnotationHelpers.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class)) {
            checkArgument(inputFunction.getParameterTypes().length > 0, "Input function has no parameters");
            Class<?> stateClass = AggregationImplementation.Parser.findAggregationStateParamType(inputFunction);

            checkArgument(AccumulatorState.class.isAssignableFrom(stateClass), "stateClass is not a subclass of AccumulatorState");
            builder.add(stateClass);
        }
        ImmutableSet<Class<?>> stateClasses = builder.build();
        checkArgument(!stateClasses.isEmpty(), "No input functions found");

        return stateClasses;
    }
}