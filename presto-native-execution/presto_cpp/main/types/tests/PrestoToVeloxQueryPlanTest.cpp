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
#include <gtest/gtest.h>
#include <fstream>

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryCtx.h"

#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "velox/functions/prestosql/types/HyperLogLogRegistration.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/IPAddressRegistration.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixRegistration.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"
#include "velox/functions/prestosql/types/JsonRegistration.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneRegistration.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/UuidRegistration.h"
#include "velox/functions/prestosql/types/UuidType.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace {
void validateSqlFunctionHandleParsing(
    const std::shared_ptr<protocol::FunctionHandle>& functionHandle,
    const std::vector<TypePtr>& expectedRawInputTypes) {
  std::vector<TypePtr> actualRawInputTypes;
  TypeParser typeParser;
  const auto sqlFunctionHandle =
      std::static_pointer_cast<protocol::SqlFunctionHandle>(functionHandle);
  parseSqlFunctionHandle(sqlFunctionHandle, actualRawInputTypes, typeParser);
  EXPECT_EQ(expectedRawInputTypes.size(), actualRawInputTypes.size());
  for (int i = 0; i < expectedRawInputTypes.size(); i++) {
    EXPECT_EQ(*expectedRawInputTypes[i], *actualRawInputTypes[i]);
  }
}
} // namespace

class PrestoToVeloxQueryPlanTest : public testing::Test {
 public:
  PrestoToVeloxQueryPlanTest() {
    registerHyperLogLogType();
    registerIPAddressType();
    registerIPPrefixType();
    registerJsonType();
    registerTimestampWithTimeZoneType();
    registerUuidType();
  }
};

TEST_F(PrestoToVeloxQueryPlanTest, exchangeTransportSelection) {
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  auto pool = memory::deprecatedAddDefaultLeafMemoryPool();
  std::ifstream input("data/Output.json");
  ASSERT_TRUE(input.good());
  const auto outputJson = json::parse(input);
  const std::string taskId = "20201107_130540_00011_wrpkw.1.2.3";
  auto queryCtx = core::QueryCtx::create(
      nullptr,
      core::QueryConfig(std::unordered_map<std::string, std::string>{}));
  VeloxInteractiveQueryPlanConverter converter(queryCtx.get(), pool.get());

  for (const auto* transport : {"HTTP", "ANY", ""}) {
    for (bool ordered : {false, true}) {
      SCOPED_TRACE(fmt::format("transport={}, ordered={}", transport, ordered));
      auto fragmentJson = outputJson;
      fragmentJson["root"] = outputJson["root"]["source"];
      auto& remote = fragmentJson["root"];
      if (*transport) {
        fragmentJson["outputTransportType"] = transport;
        remote["transportType"] = transport;
      }
      if (ordered) {
        remote["orderingScheme"] = json::parse(R"({
          "orderBy": [{
            "variable": {"@type":"variable", "name":"regionkey", "type":"bigint"},
            "sortOrder":"ASC_NULLS_FIRST"
          }]
        })");
        remote["ensureSourceOrdering"] = true;
      }
      const protocol::PlanFragment fragment = fragmentJson;
      auto plan = converter.toVeloxQueryPlan(fragment, nullptr, taskId);
      auto output =
          std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
              plan.planNode);
      ASSERT_NE(output, nullptr);
      auto exchange = std::dynamic_pointer_cast<const core::ExchangeNode>(
          output->sources()[0]);
      ASSERT_NE(exchange, nullptr);
      std::string expected = std::string(transport) == "ANY"
          ? std::string{core::TransportKind::kUcx}
          : std::string{core::TransportKind::kInMemory};
      EXPECT_EQ(output->transportKind(), expected);
      EXPECT_EQ(exchange->transportKind(), expected);
      EXPECT_EQ(
          dynamic_cast<const core::MergeExchangeNode*>(exchange.get()) !=
              nullptr,
          ordered);
      EXPECT_EQ(exchange->serdeKind(), "Presto");

      // Coordinator results always use the HTTP page transport.
      fragmentJson["root"] = outputJson["root"];
      const protocol::PlanFragment coordinatorFragment = fragmentJson;
      auto coordinatorPlan =
          converter.toVeloxQueryPlan(coordinatorFragment, nullptr, taskId);
      auto coordinatorOutput =
          std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
              coordinatorPlan.planNode);
      ASSERT_NE(coordinatorOutput, nullptr);
      EXPECT_EQ(
          coordinatorOutput->transportKind(), core::TransportKind::kInMemory);

      // Batch broadcast inputs keep their existing page transport.
      fragmentJson["root"] = outputJson["root"]["source"];
      fragmentJson["root"]["exchangeType"] = "REPLICATE";
      if (*transport) {
        fragmentJson["root"]["transportType"] = transport;
      }
      const protocol::PlanFragment batchFragment = fragmentJson;
      VeloxBatchQueryPlanConverter batch(
          "", nullptr, nullptr, queryCtx.get(), pool.get());
      auto batchPlan = batch.toVeloxQueryPlan(batchFragment, nullptr, taskId);
      auto batchOutput =
          std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
              batchPlan.planNode);
      ASSERT_NE(batchOutput, nullptr);
      auto batchExchange = std::dynamic_pointer_cast<const core::ExchangeNode>(
          batchOutput->sources()[0]);
      ASSERT_NE(batchExchange, nullptr);
      EXPECT_EQ(batchOutput->transportKind(), core::TransportKind::kInMemory);
      EXPECT_EQ(batchExchange->transportKind(), core::TransportKind::kInMemory);
    }
  }
}

TEST_F(PrestoToVeloxQueryPlanTest, parseSqlFunctionHandleWithZeroParam) {
  const std::string str = R"(
      {
        "@type": "sql_function_handle",
        "functionId": "json_file.test.count;",
        "version": "1"
      }
    )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::FunctionHandle> functionHandle = j;
  ASSERT_NE(functionHandle, nullptr);
  validateSqlFunctionHandleParsing(functionHandle, {});
}

TEST_F(PrestoToVeloxQueryPlanTest, parseSqlFunctionHandleWithOneParam) {
  const std::string str = R"(
          {
            "@type": "sql_function_handle",
            "functionId": "json_file.test.sum;tinyint",
            "version": "1"
          }
    )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::FunctionHandle> functionHandle = j;
  ASSERT_NE(functionHandle, nullptr);

  const std::vector<TypePtr> expectedRawInputTypes{TINYINT()};
  validateSqlFunctionHandleParsing(functionHandle, expectedRawInputTypes);
}

TEST_F(PrestoToVeloxQueryPlanTest, parseSqlFunctionHandleWithMultipleParam) {
  const std::string str = R"(
        {
          "@type": "sql_function_handle",
          "functionId": "json_file.test.avg;array(decimal(15, 2));varchar",
          "version": "1"
        }
    )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::FunctionHandle> functionHandle = j;
  ASSERT_NE(functionHandle, nullptr);

  const std::vector<TypePtr> expectedRawInputTypes{
      ARRAY(DECIMAL(15, 2)), VARCHAR()};
  validateSqlFunctionHandleParsing(functionHandle, expectedRawInputTypes);
}

TEST_F(PrestoToVeloxQueryPlanTest, parseSqlFunctionHandleAllComplexTypes) {
  const std::string str = R"(
        {
          "@type": "sql_function_handle",
          "functionId": "json_file.test.all_complex_types;row(map(hugeint, ipaddress), ipprefix);row(array(varbinary), timestamp, date, json, hyperloglog, timestamp with time zone, interval year to month, interval day to second);function(double, boolean);uuid",
          "version": "1"
        }
    )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::FunctionHandle> functionHandle = j;
  ASSERT_NE(functionHandle, nullptr);

  const std::vector<TypePtr> expectedRawInputTypes{
      ROW({MAP(HUGEINT(), IPADDRESS()), IPPREFIX()}),
      ROW(
          {ARRAY(VARBINARY()),
           TIMESTAMP(),
           DATE(),
           JSON(),
           HYPERLOGLOG(),
           TIMESTAMP_WITH_TIME_ZONE(),
           INTERVAL_YEAR_MONTH(),
           INTERVAL_DAY_TIME()}),
      FUNCTION({DOUBLE()}, BOOLEAN()),
      UUID()};
  validateSqlFunctionHandleParsing(functionHandle, expectedRawInputTypes);
}

TEST_F(PrestoToVeloxQueryPlanTest, parseIndexJoinNode) {
  const std::string str = R"(
    {
      "@type": "com.facebook.presto.sql.planner.plan.IndexJoinNode",
      "id": "2",
      "criteria": [
        {
          "left": {
            "@type": "variable",
            "name": "c1",
            "type": "bigint"
          },
          "right": {
            "@type": "variable",
            "name": "k1",
            "type": "bigint"
          }
        },
        {
          "left": {
            "@type": "variable",
            "name": "c2",
            "type": "smallint"
          },
          "right": {
            "@type": "variable",
            "name": "k2",
            "type": "smallint"
          }
        }
      ],
      "filter": {
        "@type": "special",
        "arguments": [
          {
            "@type": "special",
            "arguments": [
              {
                "@type": "variable",
                "name": "k3",
                "type": "smallint"
              },
              {
                "@type": "variable",
                "name": "c3",
                "type": "smallint"
              },
              {
                "@type": "constant",
                "type": "smallint",
                "valueBlock": "CwAAAFNIT1JUX0FSUkFZAQAAAAEABQA="
              }
            ],
            "form": "IN",
            "returnType": "boolean"
          },
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "variable",
                "name": "k4",
                "type": "bigint"
              },
              {
                "@type": "variable",
                "name": "c4",
                "type": "bigint"
              },
              {
                "@type": "variable",
                "name": "c5",
                "type": "bigint"
              }
            ],
            "displayName": "BETWEEN",
            "functionHandle": {
              "@type": "$static",
              "signature": {
                "argumentTypes": [
                  "bigint",
                  "bigint",
                  "bigint"
                ],
                "kind": "SCALAR",
                "longVariableConstraints": [],
                "name": "presto.default.$operator$between",
                "returnType": "boolean",
                "typeVariableConstraints": [],
                "variableArity": false
              },
              "builtInFunctionKind": "ENGINE"
            },
            "returnType": "boolean"
          },
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "constant",
                "type": "bigint",
                "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAAAA"
              },
              {
                "@type": "call",
                "arguments": [
                  {
                    "@type": "variable",
                    "name": "c1",
                    "type": "bigint"
                  },
                  {
                    "@type": "constant",
                    "type": "bigint",
                    "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAGQAAAAAAAAA"
                  }
                ],
                "displayName": "MODULUS",
                "functionHandle": {
                  "@type": "$static",
                  "builtInFunctionKind": "ENGINE",
                  "signature": {
                    "argumentTypes": [
                      "bigint",
                      "bigint"
                    ],
                    "kind": "SCALAR",
                    "longVariableConstraints": [],
                    "name": "presto.default.$operator$modulus",
                    "returnType": "bigint",
                    "typeVariableConstraints": [],
                    "variableArity": false
                  }
                },
                "returnType": "bigint"
              }
            ],
            "displayName": "$operator$equal",
            "functionHandle": {
              "@type": "$static",
              "builtInFunctionKind": "ENGINE",
              "signature": {
                "argumentTypes": [
                  "bigint",
                  "bigint"
                ],
                "kind": "SCALAR",
                "longVariableConstraints": [],
                "name": "presto.default.$operator$equal",
                "returnType": "boolean",
                "typeVariableConstraints": [],
                "variableArity": false
              }
            },
            "returnType": "boolean"
          }
        ],
        "form": "AND",
        "returnType": "boolean"
      },
      "type": "INNER",
      "lookupVariables": []
    }
  )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::IndexJoinNode> indexJoinNode = j;
  ASSERT_NE(indexJoinNode, nullptr);
  ASSERT_EQ(indexJoinNode->type, protocol::JoinType::INNER);
  ASSERT_EQ(indexJoinNode->criteria.size(), 2);
  ASSERT_NE(indexJoinNode->filter, nullptr);
  ASSERT_EQ(indexJoinNode->probeSource, nullptr);
  ASSERT_EQ(indexJoinNode->indexSource, nullptr);
}
