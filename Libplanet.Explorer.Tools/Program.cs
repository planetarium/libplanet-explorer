﻿using Cocona;
using Libplanet.Explorer.Tools.SubCommand;

namespace Libplanet.Explorer.Tools
{
    [HasSubCommands(typeof(MySqlStore), Description = "Manage MySqlStore.")]
    class Program
    {
        static void Main(string[] args) => CoconaLiteApp.Run<Program>(args);

        public void Help()
        {
            Main(new[] { "--help" });
        }
    }
}
