import tableauserverclient as TSC
import pandas
import os
import snowflake.connector

# # Snowflake Connection
# con = snowflake.connector.connect(
#     user='marcel.kintscher@biztory.be',
#     account='pz96004.europe-west4.gcp',
#     authenticator='externalbrowser',
#     role='BIZTORYTEAM',
#     warehouse='ANALYTICS_WH',
#     database='SANDBOX_MARCELKINTSCHER',
#     schema='HEH_MIGRATION'
# )

# query_calculations = """
# query calculations {
#   calculatedFields {
#     id
#     ,name
#     ,formula
#   }
# }

# """

# query_dashboards = """
# query dashboards {
#   dashboards {
#     id
#     ,name
#     ,workbook {
#       id
#       ,name
#       ,projectName
#     }
#     ,sheets {
#       id
#       ,name
#     }
#   }
# }
# """

# query_datasources_workbooks = """
# query workbooksDatasources {
#   datasourcesConnection {
#     nodes {
#       id
#       ,name
#       ,hasExtracts
#       ,__typename
#       ,downstreamWorkbooks {
#         id
#         ,name
#         ,projectName
#         ,upstreamDatasources {
#           id
#           ,name
#           ,hasExtracts
#         }
#       }
#     }
#   }
# }
# """

query_datasources_workbooks2 = """
query workbooksDatasources {
  workbooksConnection {
    nodes {
      id
      ,luid
      ,name
      ,projectName
      ,owner {
        id
        ,username
        ,name
        ,email
      }
      ,site {
        id
        ,name
      }
      embeddedDatasources {
        id
        ,name
        ,__typename
        ,hasExtracts
        ,extractLastUpdateTime
        ,upstreamDatasources {
          id
          ,name
          ,hasExtracts
        }
      }
    }
  }
}
"""

query_custom_sql_tables = """
query workbooksSQLTables {
  customSQLTablesConnection {
    nodes {
      id
      ,name
      ,isEmbedded
      ,description
      ,query
      ,connectionType
      ,tables {
        id
        ,name
        ,isEmbedded
        ,schema
        ,fullName
      }
      ,downstreamWorkbooks {
        id
        ,luid
        ,name
        ,projectName
        ,owner {
          id
          ,username
          ,name
          ,email
        }
        ,site {
          id
          ,name
        }
      }
    }
  }
}
"""

query_datasources_sheets = """
query sheets {
  datasources {
    id
    ,name
    ,downstreamSheets {
      id
      ,workbook {
        id
        ,name
        ,projectName
      }
    }
  }
}
"""

# query_published_datasources_with_fields = """
# query publishedDatasources {
#   publishedDatasourcesConnection {
#     nodes {
#       id
#       name
#       hasExtracts
#       projectName
#       fields {
#         id
#         name
#         __typename
#         description
#       }
#     }
#   }
# }
# """

# query_dashboards = """
# query dashboards {
#   dashboards {
#     id
#     ,name
#     ,workbook {
#       id
#       ,name
#       ,projectName
#     }
#     ,sheets {
#       id
#       ,name
#     }
#   }
# }
# """

# query_referenced_fields = """
# query referencedFields {
#   workbooks {
#     id
#     ,sheets {
#       id
#       ,name
#       ,sheetFieldInstances {
#           id
#           ,name
#           ,__typename
#       }
#     }
#   }
# }
# """

# #////////////////////////////////////////////////////////////////////
# query_calculations_references = """ 
# query calculationsReferences {
#   fieldsConnection (first: 500) {
#     nodes {
#       id
#       ,name
#       ,referencedByCalculations {
#         id
#         ,name
#       }
#     }
#   }
# }
# """

# query_datasources = """
# query datasources {
# datasourcesConnection {
#   nodes {
#     id
#     name
#     hasExtracts
#     __typename
#     downstreamWorkbooks {
#       id
#       name
#       projectName
#       upstreamDatasources {
#         id
#         name
#         hasExtracts
#       }
#     }
#   }
# }
# }
# """

# Sites
sites = ['','chefsplate','HelloFresh-HR','HF-ANZ','HF-BNLF','HF-CA','HF-DACH','HF-GO','HF-Nordics','HF-UK','HF-US','Sandbox']

for s in sites:
      
  # Tableau Connection
  tableau_auth = TSC.PersonalAccessTokenAuth('heh-migration', 'fwmfckOfSL+uIVQTWuH1iQ==:vwYfG9ZackmbFANj7Njoe22dEs5PPj1i',s)
  server = TSC.Server('https://service-tableau.staging.bi.hellofresh.io', use_server_version=True)

  with server.auth.sign_in(tableau_auth):
  
    # if s != '':
    #   site = server.sites.get_by_name(s)
    #   server.auth.switch_site(site)

    s_alnum = ''.join(ch for ch in s if ch.isalnum())

    ## TABLEAU

    # ## CALCULATIONS
    # calculations = server.metadata.query(query_calculations)
    # data_calculations = calculations['data']
    # df_calculations = pandas.DataFrame(data_calculations)
    # print(df_calculations)
    # filename_calculations="/Users/MK/CLIENTS/HELLOFRESH/tableau-metadata-api/temp/Calculations.json"
    # df_calculations.to_json(filename_calculations)    
    
    # ## DASHBOARDS
    # dashboards = server.metadata.query(query_dashboards)
    # data_dashboards = dashboards['data']
    # df_dashboards = pandas.DataFrame(data_dashboards)
    # print(df_dashboards)
    # filename_dashboards="/Users/MK/CLIENTS/HELLOFRESH/tableau-metadata-api/temp/Dashboards.json"
    # df_dashboards.to_json(filename_dashboards)

    ## DATASOURCES_WORKBOOKS
    print(f'Querying Datasources Workbooks for {s}.')
    datasources_workbooks = server.metadata.query(query_datasources_workbooks2)
    data_datasources_workbooks = datasources_workbooks['data']
    df_datasources_workbooks = pandas.DataFrame(data_datasources_workbooks)
    print(df_datasources_workbooks)
    filename_datasources_workbooks="/Users/MK/CLIENTS/HELLOFRESH/tableau-metadata-api/temp/Datasources_Workbooks_"+s_alnum+".json"
    df_datasources_workbooks.to_json(filename_datasources_workbooks)

    ## SQLQUERIES_WORKBOOKS
    print(f'Querying SQL Queries Workbooks for {s}.')
    sqlqueries_workbooks = server.metadata.query(query_custom_sql_tables)
    data_sqlqueries_workbooks = sqlqueries_workbooks['data']
    df_sqlqueries_workbooks = pandas.DataFrame(data_sqlqueries_workbooks)
    print(df_sqlqueries_workbooks)
    filename_sqlqueries_workbooks="/Users/MK/CLIENTS/HELLOFRESH/tableau-metadata-api/temp/Sqlqueries_Workbooks_"+s_alnum+".json"
    df_sqlqueries_workbooks.to_json(filename_sqlqueries_workbooks)

    # ## DATASOURCES_SHEETS
    # print(f'Querying Datasources Sheets for {s}.')
    # datasources_sheets = server.metadata.query(query_datasources_sheets)
    # data_datasources_sheets = datasources_sheets['data']
    # df_datasources_sheets = pandas.DataFrame(data_datasources_sheets)
    # print(df_datasources_sheets)
    # filename_datasources_sheets="/Users/MK/CLIENTS/HELLOFRESH/tableau-metadata-api/temp/Datasources_Sheets_"+s_alnum+".json"
    # df_datasources_sheets.to_json(filename_datasources_sheets)

    # ## PUBLISHED_DATASOURCES_WITH_FIELDS
    # published_datasources_with_fields = server.metadata.query(query_published_datasources_with_fields)
    # data_published_datasources_with_fields = published_datasources_with_fields['data']
    # df_published_datasources_with_fields = pandas.DataFrame(data_published_datasources_with_fields)
    # filename_published_datasources_with_fields="/Users/MK/CLIENTS/HELLOFRESH/tableau-metadata-api/temp/Published_Datasources_With_Fields.json"
    # df_published_datasources_with_fields.to_json(filename_published_datasources_with_fields)

    # ## REFERENCED_FIELDS
    # referenced_fields = server.metadata.query(query_referenced_fields)
    # data_referenced_fields = referenced_fields['data']
    # df_referenced_fields = pandas.DataFrame(data_referenced_fields)
    # filename_referenced_fields="/Users/MK/CLIENTS/HELLOFRESH/tableau-metadata-api/temp/Referenced_Fields.json"
    # df_referenced_fields.to_json(filename_referenced_fields)

  ## SNOWFLAKE

  # ## CALCULATIONS
  # con.cursor().execute("CREATE OR REPLACE TABLE CLD_CALCULATIONS (ID VARIANT)")
  # put_statement_calculations = "PUT file://" + filename_calculations + " @~/metadata"
  # con.cursor().execute(put_statement_calculations)
  # copy_statement = """COPY INTO CLD_CALCULATIONS FROM @~/metadata/Calculations.json.gz 
  #                     FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
  # con.cursor().execute(copy_statement)

  # ## DASHBOARDS
  # con.cursor().execute("CREATE OR REPLACE TABLE CLD_DASHBOARDS (ID VARIANT)")
  # put_statement_dashboards = "PUT file://" + filename_dashboards + " @~/metadata"
  # con.cursor().execute(put_statement_dashboards)
  # copy_statement = """COPY INTO CLD_DASHBOARDS FROM @~/metadata/Dashboards.json.gz 
  #                     FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
  # con.cursor().execute(copy_statement)

  # ## DATASOURCES_WORKBOOKS
  # con.cursor().execute("CREATE OR REPLACE TABLE HEH_DATASOURCES_WORKBOOKS_"+ s_alnum +" (ID VARIANT)")
  # put_statement_datasources_workbooks = "PUT file://" + filename_datasources_workbooks + " @~/metadata"
  # con.cursor().execute(put_statement_datasources_workbooks)
  # copy_statement = "COPY INTO HEH_DATASOURCES_WORKBOOKS_"+ s_alnum +""" FROM @~/metadata/Datasources_Workbooks.json.gz 
  #                     FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
  # con.cursor().execute(copy_statement)

  # ## SQLQUERIES_WORKBOOKS
  # con.cursor().execute("CREATE OR REPLACE TABLE HEH_SQLQUERIES_WORKBOOKS_"+ s_alnum +" (ID VARIANT)")
  # put_statement_sqlqueries_workbooks = "PUT file://" + filename_sqlqueries_workbooks + " @~/metadata"
  # con.cursor().execute(put_statement_sqlqueries_workbooks)
  # copy_statement = "COPY INTO HEH_SQLQUERIES_WORKBOOKS_"+ s_alnum +""" FROM @~/metadata/Sqlqueries_Workbooks.json.gz 
  #                     FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
  # con.cursor().execute(copy_statement)

  # ## DATASOURCES_SHEETS
  # con.cursor().execute("CREATE OR REPLACE TABLE HEH_DATASOURCES_AND_SHEETS_"+ s_alnum +" (ID VARIANT)")
  # put_statement_datasources_sheets = "PUT file://" + filename_datasources_sheets + " @~/metadata"
  # con.cursor().execute(put_statement_datasources_sheets)
  # copy_statement = "COPY INTO HEH_DATASOURCES_AND_SHEETS_"+ s_alnum +""" FROM @~/metadata/Datasources_Sheets.json.gz 
  #                     FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
  # con.cursor().execute(copy_statement)

  # ## PUBLISHED_DATASOURCES_WITH_FIELDS
  # con.cursor().execute("CREATE OR REPLACE TABLE CLD_PUBLISHED_DATASOURCES_WITH_FIELDS (ID VARIANT)")
  # put_statement_published_datasources_with_fields = "PUT file://" + filename_published_datasources_with_fields + " @~/metadata"
  # con.cursor().execute(put_statement_published_datasources_with_fields)
  # copy_statement = """COPY INTO CLD_PUBLISHED_DATASOURCES_WITH_FIELDS FROM @~/metadata/Published_Datasources_With_Fields.json.gz 
  #                     FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
  # con.cursor().execute(copy_statement)

  # ## REFERENCED_FIELDS
  # con.cursor().execute("CREATE OR REPLACE TABLE CLD_REFERENCED_FIELDS (ID VARIANT)")
  # put_statement_referenced_fields = "PUT file://" + filename_referenced_fields + " @~/metadata"
  # con.cursor().execute(put_statement_referenced_fields)
  # copy_statement = """COPY INTO CLD_REFERENCED_FIELDS FROM @~/metadata/Referenced_Fields.json.gz 
  #                     FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
  # con.cursor().execute(copy_statement)

  # os.remove(filename_calculations)
  # os.remove(filename_dashboards)
  # os.remove(filename_datasources_workbooks)
  # os.remove(filename_sqlqueries_workbooks)
  # os.remove(filename_datasources_sheets)
  # os.remove(filename_published_datasources_with_fields)
  # os.remove(filename_referenced_fields)

server.auth.sign_out()
# con.close()
