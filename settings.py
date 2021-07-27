# settings = [{
#     'application': 'Ticketing',
#     'dataset_name': 'ticket_as',
#     'table_name': 'ticket_airline',
#     'bq_retention_days': 1110,               # select .... from .... # to working table
#     'gsc_retention_days': 690,
#     'schedule': None,
#     'partition_column': 'eda_load_datetime',
#     'gsc_archival_path': 'gs://archiveddata_ticketing/core/ticketing/ticket_as/processeddata/', # defalte,
#     'standard': 0,     # 7
#     'archival': 690,
#     'near_line': 0,
#     'to_be_archived': 'Y'
# }, {
#     'application': 'Ticketing',
#     'dataset_name': 'ticket_as',
#     'table_name': 'ticket_airline_error',
#     'bq_retention_days': 7,
#     'gsc_retention_days': None,
#     'schedule': None,
#     'partition_column': 'eda_load_datetime',
#     'gsc_archival_path': None,
#     'standard': None,
#     'archival': None,
#     'near_line': None,
#     'to_be_archived': 'N'
# }]
