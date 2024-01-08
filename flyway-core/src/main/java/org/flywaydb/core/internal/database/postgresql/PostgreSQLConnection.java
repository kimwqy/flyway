/*
 * Copyright © Red Gate Software Ltd 2010-2020
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.database.postgresql;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * PostgreSQL connection.
 */
public class PostgreSQLConnection extends Connection<PostgreSQLDatabase> {
    private final String originalRole;

    PostgreSQLConnection(PostgreSQLDatabase database, java.sql.Connection connection) {
        super(database, connection);

        try {
            originalRole = jdbcTemplate.queryForString("SELECT CURRENT_USER");
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to determine current user", e);
        }
    }


    /**
     * 方法作用：将数据库连接的角色（role）重置为其原始值，确保在迁移或回调过程中更改的角色被还原回初始状态，
     * 在Flyway的设计中，可能会在迁移或回调期间更改数据库连接的角色，以满足特定需求。为了保证不同迁移之间的一致性，
     * 当完成迁移或回调时，需要将角色重置为初始状态，以免对后续迁移或操作产生影响
     *
     * 把这个方法体注释掉，经测试高斯和postgresql均无影响。由于 postgresql 和 高斯 之间对设置 role 语法之间的差异，
     * 高斯数据库 set 角色时还需要带上密码，而postgresql则不用，如：
     *
     * postgresql: SET ROLE xxx
     * gaussDB: SET ROLE xxx PASSWORD xxx
     *
     * @return
     * @throws @throws SQLException SQLException
     * @author yzh
     * @see
     * @since 2024/01/08
     */
    @Override
    protected void doRestoreOriginalState() throws SQLException {
        // Reset the role to its original value in case a migration or callback changed it
        // 注释以下代码
        //jdbcTemplate.execute("SET ROLE '" + originalRole + "'");
    }

    @Override
    public Schema doGetCurrentSchema() throws SQLException {
        String currentSchema = jdbcTemplate.queryForString("SELECT current_schema");
        String searchPath = getCurrentSchemaNameOrSearchPath();

        if (!StringUtils.hasText(currentSchema) && !StringUtils.hasText(searchPath)) {
            throw new FlywayException("Unable to determine current schema as search_path is empty. " +
                    "Set the current schema in currentSchema parameter of the JDBC URL or in Flyway's schemas property.");
        }

        String schema = StringUtils.hasText(currentSchema) ? currentSchema : searchPath;

        return getSchema(schema);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("SHOW search_path");
    }

    @Override
    public void changeCurrentSchemaTo(Schema schema) {
        try {
            if (schema.getName().equals(originalSchemaNameOrSearchPath) || originalSchemaNameOrSearchPath.startsWith(schema.getName() + ",") || !schema.exists()) {
                return;
            }

            if (StringUtils.hasText(originalSchemaNameOrSearchPath)) {
                doChangeCurrentSchemaOrSearchPathTo(schema.toString() + "," + originalSchemaNameOrSearchPath);
            } else {
                doChangeCurrentSchemaOrSearchPathTo(schema.toString());
            }
        } catch (SQLException e) {
            throw new FlywaySqlException("Error setting current schema to " + schema, e);
        }
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        jdbcTemplate.execute("SELECT set_config('search_path', ?, false)", schema);
    }

    @Override
    public Schema getSchema(String name) {
        return new PostgreSQLSchema(jdbcTemplate, database, name);
    }

    @Override
    public <T> T lock(Table table, Callable<T> callable) {
        return new PostgreSQLAdvisoryLockTemplate(jdbcTemplate, table.toString().hashCode()).execute(callable);
    }
}