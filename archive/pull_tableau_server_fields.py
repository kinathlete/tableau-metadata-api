import pandas as pd
import pull_tableau_server_metadata as ptsm

content_type_payload = { "query" : """
query fields {
  fields {
    id,
    name,
    isHidden
  }
}
		""" }

def get_output_schema():
	return pd.DataFrame({
		'id':prep_string(),
		'name':prep_string(),
		'isHidden':prep_bool()
	})

def get_things(df):
	
	server_base_url = df.iloc[0]["server_url"]
	username = df.iloc[0]["username"]
	password = df.iloc[0]["password"]

	data = ptsm.get_stuff(server_base_url, username, password, "fields", content_type_payload, None, None, None)
	return data