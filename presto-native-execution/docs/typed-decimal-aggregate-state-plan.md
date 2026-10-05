# Coordinator-planned cuDF decimal aggregate state

## Decision and scope

Use coordinator `cudf.enabled` to select a metadata-only alternative decimal
SUM registration, following `AlternativeApproxPercentile`. Keep normal Velox
plans and the cuDF Driver adapter; do not invoke `CudfPlanRewriter` from Presto.
No new Java or CPU Velox aggregate execution implementation, no session
property, and no Java planner rewrite are required.

Initial scope: Decimal64 SUM only. Decimal128 SUM and decimal AVG retain their
legacy intermediate type. Later variants require separately defined state
contracts; they cannot all be represented by one decimal value.

## Coordinator implementation

- `FunctionsConfig`: add `cudf.enabled`, default false. It is a startup-level
  registration choice, independent of `use-alternative-function-signatures`.
- `BuiltInTypeAndFunctionNamespaceManager`: override decimal SUM only under
  that cuDF gate. Ordinary native CPU coordinators are unchanged.
- `AlternativeDecimalSumAggregation`: specialize Decimal64 SUM to metadata
  declaring input `DECIMAL(p <= 18, s)`, intermediate `DECIMAL(38, s)`, final
  `DECIMAL(38, s)`. No Java accumulator classes or aggregation metadata are
  supplied. Delegate Decimal128 specialization to the legacy implementation.
- Preserve the SQL function name and signature. Existing partial-aggregation
  planning obtains intermediate types from the selected implementation and
  propagates them into exchange and fragment schemas.
- Tests cover config defaults/mapping, both independent config gates, short
  versus long decimals, scales, unchanged AVG/integer SUM, and absent Java
  execution classes. Distributed-plan tests remain to be added.

## Native worker implementation

Presto enables the Driver adapter and no longer rewrites task plans. Normal
Velox logical plan nodes and normal CPU/GPU vector conversions remain in use.

Pending Velox work must stay under `velox/experimental/cudf/`:

1. Recognize planner-declared typed Decimal64 SUM in every aggregation step.
2. Produce/consume that state in CudfGroupby and CudfReduce, preserving null,
   cancellation, final decimal range checking, and existing memory optimizations.
3. Fail clearly if a typed aggregation cannot execute on GPU, including mixed
   aggregate nodes where another function is unsupported. Never run the legacy
   CPU decimal aggregate against a decimal intermediate.
4. Verify Driver-adapter timing: CPU aggregation construction/initialization
   may resolve signatures or instantiate accumulators before replacement.
   Metadata-only Java planning does not solve that native lifecycle issue.
   Establish a cuDF-local solution before claiming CPU-free execution works.
5. Remove obsolete GPU-plan state provenance machinery only as part of the
   separate Velox simplification, preserving unrelated Q18 optimizations.

CPU operators may forward typed state and serialize it as ordinary typed data;
they may not execute the typed aggregate. Verify conversion, ordering, local
partitioning, and nested-state support rather than assuming all boundaries work.

## Deployment and compatibility

Enable coordinator `cudf.enabled=true` only on a controlled cluster whose
workers all implement the typed cuDF protocol. Worker `cudf.enabled=true` is a
separate setting. The coordinator flag provides neither capability discovery
nor scheduling enforcement. Java aggregation execution is unsupported for the
metadata-only alternative. Window uses and optimizer-generated aggregate
variants also require coverage before enabling the feature broadly.

A coordinator-only upgrade is not runnable end-to-end yet. Keep the flag false
until the native implementation and GPU-only enforcement are validated.
Rollback changes the coordinator registration after restart and applies to new
plans; never reinterpret an in-flight typed fragment as legacy VARBINARY.

## Validation gates

1. Compile/run `TestFunctionsConfig` and `TestAlternativeDecimalSumAggregation`.
2. Verify distributed partial/intermediate/final plans and exchange schemas,
   with the flag on/off, grouped/global aggregation, modifiers, and windows.
3. Verify native worker plan conversion and Driver replacement before CPU
   aggregate initialization; verify clear failure when GPU execution is disabled
   or unsupported. No Velox core changes or CPU state implementation.
4. Test local exchange, UCX, HTTP, and any supported spill path using actual
   typed vectors. Include empty/all-null input, signs/cancellation, scale and
   precision boundaries, and final overflow errors.
5. Rebuild coordinator and worker, run Q1/Q18 against an independent reference,
   retaining exact decimals and two drivers. Measure memory separately from
   correctness; a compact state does not guarantee device-only Q18 fits.

## Follow-on state schemas

Decimal64 AVG needs count; Decimal128 SUM needs overflow; Decimal128 AVG needs
both. Define exact ROW layouts, null conventions, signed overflow arithmetic,
and interoperability before enabling these alternatives. Raw accumulator sum
bits are not necessarily valid final DECIMAL values: validate numeric bounds
and serializer behavior before representing wider accumulator limbs as ordinary
decimal fields. The Decimal64 SUM case has a stronger bound from short-decimal
input precision and a signed 64-bit count.
