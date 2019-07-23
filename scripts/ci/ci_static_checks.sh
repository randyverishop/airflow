#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -uo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


RES=0

# shellcheck source=./ci_mypy.sh
if ! "${MY_DIR}/ci_mypy.sh"; then
    RES=$((RES+1))
fi

# shellcheck source=./ci_flake8.sh
if ! "${MY_DIR}/ci_flake8.sh"; then
    RES=$((RES+2))
fi

# shellcheck source=./ci_pylint.sh
if ! "${MY_DIR}/ci_pylint.sh"; then
    RES=$((RES+4))
fi

# shellcheck source=./ci_docs.sh
if ! "${MY_DIR}/ci_docs.sh"; then
    RES=$((RES+8))
fi

exit "${RES}"
