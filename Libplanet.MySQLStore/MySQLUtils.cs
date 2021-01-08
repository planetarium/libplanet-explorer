#nullable enable
using MySqlConnector;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace Libplanet.MySqlStore
{
    internal static class MySqlUtils
    {
        internal static QueryFactory OpenMySqlDB(string connectionString, MySqlCompiler compiler) =>
            new QueryFactory(new MySqlConnection(connectionString), compiler);
    }
}
