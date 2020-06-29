import pandas as pd
import pull_tableau_server_metadata as ptsm

content_type_payload = { "query" : """
query workbooks {
  workbooks {
    id,
    name,
    owner {
      id
    },
    uri
  }
}
		""" }

def get_output_schema():
	return pd.DataFrame({
		'id':prep_string(),
		'name':prep_string(),
		'owner.id':prep_string(),
		'uri':prep_string()
	})

def get_things(df):
	
	server_base_url = df.iloc[0]["server_url"]
	username = df.iloc[0]["username"]
	password = df.iloc[0]["password"]

	data = ptsm.get_stuff(server_base_url, username, password, "workbooks", content_type_payload, None, None, None)
	return data