// Copyright 2021 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.microsoft.durabletask;

import java.time.Duration;

final class Helpers {
    final static Duration maxDuration = Duration.ofSeconds(Long.MAX_VALUE, 999999999L);

    static void throwIfArgumentNull(Object argValue, String argName) {
        if (argValue == null) {
            throw new IllegalArgumentException("Argument '" + argName + "' was null.");
        }
    }

    static boolean isInfiniteTimeout(Duration timeout) {
        return timeout == null || timeout.isNegative() || timeout.equals(maxDuration);
    }

    // Cannot be instantiated
    private Helpers() {
    }
}
