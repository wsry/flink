#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

END_TO_END_DIR="`dirname \"$0\"`" # relative
END_TO_END_DIR="`( cd \"$END_TO_END_DIR\" && pwd )`" # absolutized and normalized
if [ -z "$END_TO_END_DIR" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
fi

export END_TO_END_DIR

if [ -z "$FLINK_DIR" ] ; then
    echo "You have to export the Flink distribution directory as FLINK_DIR"
    exit 1
fi

source "${END_TO_END_DIR}/test-scripts/test-runner-common.sh"

FLINK_DIR="`( cd \"$FLINK_DIR\" && pwd )`" # absolutized and normalized

echo "flink-end-to-end-test directory: $END_TO_END_DIR"
echo "Flink distribution directory: $FLINK_DIR"

# Template for adding a test:

# run_test "<description>" "$END_TO_END_DIR/test-scripts/<script_name>"

run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 1"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 2"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 3"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 4"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 5"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 6"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 7"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 8"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 9"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 10"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 11"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 12"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 13"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 14"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 15"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 16"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 17"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 18"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 19"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 20"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 21"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 22"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 23"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 24"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 25"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 26"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 27"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 28"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 29"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 30"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 31"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 32"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 33"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 34"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 35"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 36"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 37"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 38"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 39"
run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh 40"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"
#run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"

#run_test "ConnectedComponents iterations with high parallelism end-to-end test" "$END_TO_END_DIR/test-scripts/test_high_parallelism_iterations.sh 25"

printf "\n[PASS] All tests passed\n"
exit 0
