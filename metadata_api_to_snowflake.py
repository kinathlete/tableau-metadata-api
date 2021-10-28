import tableauserverclient as TSC
import pandas
import os
import snowflake.connector
​
tableau_auth = TSC.PersonalAccessTokenAuth('metadata', 'put_key_here')
server = TSC.Server("tableau server url")
server.version = '3.9'
​
con = snowflake.connector.connect(
    user='employee_name@biztory.be',
    account='pz96004.europe-west4.gcp',
    authenticator='externalbrowser',
    warehouse='ANALYTICS_WH',
    database='SANDBOX',
    schema='YOURNAME'
)
​
query_calculations = """
query calculations {
  calculatedFields {
    id
    ,name
    ,formula
  }
}
​
"""
​
query_dashboards = """
query dashboards {
  dashboards {
    id
    ,name
    ,workbook {
      id
      ,name
      ,projectName
    }
    ,sheets {
      id
      ,name
    }
  }
}
"""
​
query_datasources_workbooks = """
query workbooksDatasources {
  datasourcesConnection {
    nodes {
      id
      ,name
      ,hasExtracts
      ,__typename
      ,downstreamWorkbooks {
        id
        ,name
        ,projectName
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
​
query_datasources_workbooks2 = """
query workbooksDatasources {
  workbooksConnection {
    nodes {
      id
      ,name
      ,projectName
      ,site {
        id
        ,name
      }
      embeddedDatasources {
        id
        ,name
        ,__typename
        ,hasExtracts
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
​
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
​
query_published_datasources_with_fields = """
query publishedDatasources {
  publishedDatasourcesConnection {
    nodes {
      id
      name
      hasExtracts
      projectName
      fields {
        id
        name
        __typename
        description
      }
    }
  }
}
"""
​
query_dashboards = """
query dashboards {
  dashboards {
    id
    ,name
    ,workbook {
      id
      ,name
      ,projectName
    }
    ,sheets {
      id
      ,name
    }
  }
}
"""
​
query_referenced_fields = """
query referencedFields {
  workbooks {
    id
    ,sheets {
      id
      ,name
      ,sheetFieldInstances {
          id
          ,name
          ,__typename
      }
    }
  }
}
"""
​
​
#////////////////////////////////////////////////////////////////////
query_calculations_references = """ 
query calculationsReferences {
	fieldsConnection (first: 500) {
    nodes {
      id
      ,name
      ,referencedByCalculations {
        id
        ,name
      }
    }
  }
}
"""
​
​
​
​
​
#query_datasources = """
#query datasources {
#  datasourcesConnection {
#    nodes {
#      id
#      name
#      hasExtracts
#      __typename
#      downstreamWorkbooks {
#        id
#        name
#        projectName
#        upstreamDatasources {
#          id
#          name
#          hasExtracts
#        }
#      }
#    }
#  }
#}
#"""
​
​
​
with server.auth.sign_in(tableau_auth):
    #Query the Metadata API and store the response in resp
​
    ## CALCULATIONS
    calculations = server.metadata.query(query_calculations)
    data_calculations = calculations['data']
    df_calculations = pandas.DataFrame(data_calculations)
    filename_calculations="C:\Temp\Calculations.json"
    df_calculations.to_json(filename_calculations)    
    
    ## DASHBOARDS
    dashboards = server.metadata.query(query_dashboards)
    data_dashboards = dashboards['data']
    df_dashboards = pandas.DataFrame(data_dashboards)
    filename_dashboards="C:\Temp\Dashboards.json"
    df_dashboards.to_json(filename_dashboards)
​
    ## DATASOURCES_WORKBOOKS
    datasources_workbooks = server.metadata.query(query_datasources_workbooks2)
    data_datasources_workbooks = datasources_workbooks['data']
    df_datasources_workbooks = pandas.DataFrame(data_datasources_workbooks)
    filename_datasources_workbooks="C:\Temp\Datasources_Workbooks.json"
    df_datasources_workbooks.to_json(filename_datasources_workbooks)
​
    ## DATASOURCES_SHEETS
    datasources_sheets = server.metadata.query(query_datasources_sheets)
    data_datasources_sheets = datasources_sheets['data']
    df_datasources_sheets = pandas.DataFrame(data_datasources_sheets)
    filename_datasources_sheets="C:\Temp\Datasources_Sheets.json"
    df_datasources_sheets.to_json(filename_datasources_sheets)
​
    ## PUBLISHED_DATASOURCES_WITH_FIELDS
    published_datasources_with_fields = server.metadata.query(query_published_datasources_with_fields)
    data_published_datasources_with_fields = published_datasources_with_fields['data']
    df_published_datasources_with_fields = pandas.DataFrame(data_published_datasources_with_fields)
    filename_published_datasources_with_fields="C:\Temp\Published_Datasources_With_Fields.json"
    df_published_datasources_with_fields.to_json(filename_published_datasources_with_fields)
​
    ## REFERENCED_FIELDS
    referenced_fields = server.metadata.query(query_referenced_fields)
    data_referenced_fields = referenced_fields['data']
    df_referenced_fields = pandas.DataFrame(data_referenced_fields)
    filename_referenced_fields="C:\Temp\Referenced_Fields.json"
    df_referenced_fields.to_json(filename_referenced_fields)
    
    
​
​
## CALCULATIONS
con.cursor().execute("CREATE OR REPLACE TABLE CLD_CALCULATIONS (ID VARIANT)")
put_statement_calculations = "PUT file://" + filename_calculations + " @~/metadata"
con.cursor().execute(put_statement_calculations)
copy_statement = """COPY INTO CLD_CALCULATIONS FROM @~/metadata/Calculations.json.gz 
                    FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
con.cursor().execute(copy_statement)
​
## DASHBOARDS
con.cursor().execute("CREATE OR REPLACE TABLE CLD_DASHBOARDS (ID VARIANT)")
put_statement_dashboards = "PUT file://" + filename_dashboards + " @~/metadata"
con.cursor().execute(put_statement_dashboards)
copy_statement = """COPY INTO CLD_DASHBOARDS FROM @~/metadata/Dashboards.json.gz 
                    FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
con.cursor().execute(copy_statement)
​
## DATASOURCES_WORKBOOKS
con.cursor().execute("CREATE OR REPLACE TABLE CLD_DATASOURCES_WORKBOOKS (ID VARIANT)")
put_statement_datasources_workbooks = "PUT file://" + filename_datasources_workbooks + " @~/metadata"
con.cursor().execute(put_statement_datasources_workbooks)
copy_statement = """COPY INTO CLD_DATASOURCES_WORKBOOKS FROM @~/metadata/Datasources_Workbooks.json.gz 
                    FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
con.cursor().execute(copy_statement)
​
## DATASOURCES_SHEETS
con.cursor().execute("CREATE OR REPLACE TABLE CLD_DATASOURCES_AND_SHEETS (ID VARIANT)")
put_statement_datasources_sheets = "PUT file://" + filename_datasources_sheets + " @~/metadata"
con.cursor().execute(put_statement_datasources_sheets)
copy_statement = """COPY INTO CLD_DATASOURCES_AND_SHEETS FROM @~/metadata/Datasources_Sheets.json.gz 
                    FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
con.cursor().execute(copy_statement)
​
## PUBLISHED_DATASOURCES_WITH_FIELDS
con.cursor().execute("CREATE OR REPLACE TABLE CLD_PUBLISHED_DATASOURCES_WITH_FIELDS (ID VARIANT)")
put_statement_published_datasources_with_fields = "PUT file://" + filename_published_datasources_with_fields + " @~/metadata"
con.cursor().execute(put_statement_published_datasources_with_fields)
copy_statement = """COPY INTO CLD_PUBLISHED_DATASOURCES_WITH_FIELDS FROM @~/metadata/Published_Datasources_With_Fields.json.gz 
                    FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
con.cursor().execute(copy_statement)
​
## REFERENCED_FIELDS
con.cursor().execute("CREATE OR REPLACE TABLE CLD_REFERENCED_FIELDS (ID VARIANT)")
put_statement_referenced_fields = "PUT file://" + filename_referenced_fields + " @~/metadata"
con.cursor().execute(put_statement_referenced_fields)
copy_statement = """COPY INTO CLD_REFERENCED_FIELDS FROM @~/metadata/Referenced_Fields.json.gz 
                    FILE_FORMAT = (FORMAT_NAME = json_file_format)"""
con.cursor().execute(copy_statement)
​
os.remove(filename_calculations)
os.remove(filename_dashboards)
os.remove(filename_datasources_workbooks)
os.remove(filename_datasources_sheets)
os.remove(filename_published_datasources_with_fields)
os.remove(filename_referenced_fields)
