#nullable enable
using MySqlConnector;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace Libplanet.MySQLStore
{
    internal static class MySQLUtils
    {
        internal static QueryFactory OpenMySQLDB(string connectionString, MySqlCompiler compiler) =>
            new QueryFactory(new MySqlConnection(connectionString), compiler);
    }
}
