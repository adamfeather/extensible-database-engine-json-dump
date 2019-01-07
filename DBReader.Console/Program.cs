using ESEDump.Core;
using System.IO;

namespace DBReader.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var dbLocation = new FileInfo(@"database.dat");
            var outputFile = new FileInfo(@"output.json");

            using (var eseDatabase = new ESEDatabase(dbLocation))
            {
                eseDatabase.JsonDump(outputFile);
            }
        }
    }
}