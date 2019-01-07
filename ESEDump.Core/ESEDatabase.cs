using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json;

namespace ESEDump.Core
{
    /// <summary>
    /// Essent.Interop: Instance > Session > Database > Table
    /// </summary>
    public class ESEDatabase : IDisposable
    {
        private const string ContainersTableName = "Containers";
        private const string ContainerIdColumn = "ContainerId";

        private readonly FileInfo _dbLocation;
        private readonly string _instanceName;
        private Instance _instance;
        private Session _session;
        private JET_DBID _jetDatabaseId;

        public Table ContainersTable { get; private set; }

        public IDictionary<string, JET_COLUMNID> ContainersTableColumns { get; private set; }

        public ESEDatabase(FileInfo dbLocation)
        {
            _instanceName = Guid.NewGuid().ToString();

            _dbLocation = new FileInfo(dbLocation.FullName);
        }

        private void InitializeConnection()
        {
            Api.JetGetDatabaseFileInfo(_dbLocation.FullName, out int pageSize, JET_DbInfo.PageSize);

            SystemParameters.DatabasePageSize = pageSize;

            _instance = new Instance(_instanceName);
            _instance.Init();

            _session = new Session(_instance);

            Api.JetAttachDatabase(_session, _dbLocation.FullName, AttachDatabaseGrbit.ReadOnly);
            Api.JetOpenDatabase(_session, _dbLocation.FullName, null, out JET_DBID jetDatabaseId, OpenDatabaseGrbit.ReadOnly);

            _jetDatabaseId = jetDatabaseId;
        }

        public void JsonDump(FileInfo outputFile)
        {
            InitializeConnection();

            OutputDatabaseAsJson(outputFile.FullName);
        }

        private void OutputDatabaseAsJson(string outputFile)
        {
            ContainersTable = new Table(_session, _jetDatabaseId, ContainersTableName, OpenTableGrbit.ReadOnly);
            ContainersTableColumns = Api.GetColumnDictionary(_session, ContainersTable);

            using (var fileStream = new FileStream(outputFile, FileMode.Create))
            using (var streamWriter = new StreamWriter(fileStream, Encoding.UTF8))
            using (var writer = new JsonTextWriter(streamWriter))
            {
                if (!Api.TryMoveFirst(_session, ContainersTable))
                {
                    return;
                }

                writer.WriteStartObject();
                writer.WritePropertyName("containers");
                writer.WriteStartArray();
                do
                {
                    var containerId = Api.RetrieveColumnAsInt32(_session, ContainersTable, ContainersTableColumns[ContainerIdColumn]);

                    if (containerId == null)
                    {
                        continue;
                    }

                    WriteTableAsJson(writer, $"Container_{containerId}");

                } while (Api.TryMoveNext(_session, ContainersTable));

                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }

        private void WriteTableAsJson(JsonTextWriter writer, string containerId)
        {
            writer.WriteStartObject();
            writer.WritePropertyName($"Container_{containerId}");

            using (var table = new Table(_session, _jetDatabaseId, containerId, OpenTableGrbit.ReadOnly))
            {
                if (!Api.TryMoveFirst(_session, table))
                {
                    return;
                }

                writer.WriteStartObject();

                writer.WritePropertyName("data");
                WriteRowsAsJson(writer, table);

                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        private void WriteRowsAsJson(JsonTextWriter writer, Table table)
        {
            var tableColumns = Api.GetColumnDictionary(_session, table);

            writer.WriteStartArray();

            do
            {
                writer.WriteStartObject();
                foreach (KeyValuePair<string, JET_COLUMNID> column in tableColumns)
                {
                    writer.WritePropertyName(column.Key);
                    WriteValue(writer, table, column);
                }
                writer.WriteEndObject();

            } while (Api.TryMoveNext(_session, table.JetTableid));

            writer.WriteEndArray();
        }

        private void WriteValue(JsonTextWriter writer, Table table, KeyValuePair<string, JET_COLUMNID> column)
        {
            Api.JetGetColumnInfo(_session, _jetDatabaseId, table.Name, column.Key, out JET_COLUMNDEF jetColumnDef);

            var columnType = (int)jetColumnDef.coltyp;

            switch (columnType)
            {
                case (int)JET_coltyp.Nil:
                    writer.WriteValue(string.Empty);
                    break;
                case (int)JET_coltyp.Bit:
                    writer.WriteValue(Api.RetrieveColumnAsBoolean(_session, table, column.Value));
                    break;
                case (int)JET_coltyp.UnsignedByte:
                    writer.WriteValue(Api.RetrieveColumnAsByte(_session, table, column.Value));
                    break;
                case (int)JET_coltyp.Short:
                    writer.WriteValue(Api.RetrieveColumnAsInt16(_session, table, column.Value));
                    break;
                case (int)JET_coltyp.Long:
                    writer.WriteValue(Api.RetrieveColumnAsInt32(_session, table, column.Value));
                    break;
                case (int)JET_coltyp.Currency:
                    writer.WriteValue(Api.RetrieveColumnAsInt64(_session, table, column.Value));
                    break;
                case (int)JET_coltyp.IEEESingle:
                case (int)JET_coltyp.IEEEDouble:
                    writer.WriteValue(Api.RetrieveColumnAsDouble(_session, table, column.Value));
                    break;
                case (int)JET_coltyp.DateTime:
                    writer.WriteValue(Api.RetrieveColumnAsFloat(_session, table, column.Value));
                    break;
                case (int)JET_coltyp.Binary:
                case (int)JET_coltyp.LongBinary:
                    byte[] bytes = Api.RetrieveColumn(_session, table, column.Value) ?? new byte[0];
                    writer.WriteValue(BitConverter.ToString(bytes));
                    break;
                case (int)JET_coltyp.Text:
                case (int)JET_coltyp.LongText:
                    writer.WriteValue(Api.RetrieveColumnAsString(_session, table, column.Value, Encoding.Unicode));
                    break;
                case 14: //JET_coltypUnsignedLong
                    writer.WriteValue(Api.RetrieveColumnAsUInt32(_session, table, column.Value));
                    break;
                case 15: //JET_coltypLongLong
                    writer.WriteValue(Api.RetrieveColumnAsInt64(_session, table, column.Value));
                    break;
                case 16: // JET_coltypGUID
                    writer.WriteValue(Api.RetrieveColumnAsGuid(_session, table, column.Value));
                    break;
                case 17: // JET_coltypUnsignedShort
                    writer.WriteValue(Api.RetrieveColumnAsUInt16(_session, table, column.Value));
                    break;
                default:
                    writer.WriteValue("type not recognised");
                    break;
            }
        }

        public void Dispose()
        {
            this.ContainersTable.Dispose();
            this._session.Dispose();
            this._instance.Dispose();
        }
    }
}