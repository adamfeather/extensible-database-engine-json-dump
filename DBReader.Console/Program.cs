using ESEDump.Core;
using System.IO;

namespace DBReader.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var dbLocation = new FileInfo(@"..\..\..\webCacheV01.dat");
            var outputFile = new FileInfo(@"..\..\..\webCacheV01.json");

            using (var eseDatabase = new ESEDatabase(dbLocation))
            {
                eseDatabase.JsonDump(outputFile);
            }
        }
    }
}