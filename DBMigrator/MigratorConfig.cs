using System;
using System.Collections.Generic;
using System.Text;

namespace DBMigrator
{
    public class MigratorConfig
    {
        public string SourceConnection { get; set; }
        public string DestinationConnection { get; set; }
        public int BatchNumber { get; set; }
        public IEnumerable<string> Tables { get; set; }
    }
}
