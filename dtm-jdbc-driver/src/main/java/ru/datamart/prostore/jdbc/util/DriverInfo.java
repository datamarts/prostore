/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.prostore.jdbc.util;

public class DriverInfo {
    public static final String DATABASE_PRODUCT_NAME = "DTM";
    public static final String DRIVER_NAME = "DTM JDBC Driver";

    public static String DRIVER_VERSION;
    public static int MAJOR_VERSION;
    public static int MINOR_VERSION;

    public static final int JDBC_MAJOR_VERSION = "4.2".charAt(0) - 48;
    public static final int JDBC_MINOR_VERSION = "4.2".charAt(2) - 48;

    private DriverInfo() {
    }

    static {
        DRIVER_VERSION = DriverInfo.class.getPackage().getImplementationVersion();

        String[] versions = DRIVER_VERSION.split("\\.");
        MAJOR_VERSION = Integer.parseInt(versions[0]);
        MINOR_VERSION = Integer.parseInt(versions[1]);
    }

}
